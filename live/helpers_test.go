package live

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	celeris "github.com/useceleris/sdk-go-client"
)

// The SDK gives every publish its own id, which the server delivers as is
// (RESEND-01): 16 random bytes, hex-encoded.
var generatedMessageID = regexp.MustCompile("^[0-9a-f]{32}$")

var channelCounter atomic.Int64

// TestMain fails the run at once when the target cannot be used, instead of
// letting every test wait out its connect deadline. Missing configuration
// fails; it never skips.
func TestMain(m *testing.M) {
	loadEnvironment("../.env")

	for _, name := range []string{"CELERIS_WS_URL", "CELERIS_CLIENT_ID", "CELERIS_SIGNING_SECRET"} {
		if os.Getenv(name) == "" {
			fmt.Fprintln(os.Stderr, name+" not set. Put CELERIS_WS_URL, CELERIS_CLIENT_ID and CELERIS_SIGNING_SECRET in the repository's .env (gitignored) or the environment.")
			os.Exit(1)
		}
	}

	target, err := url.Parse(os.Getenv("CELERIS_WS_URL"))

	if err != nil {
		fmt.Fprintln(os.Stderr, "CELERIS_WS_URL is not a URL.")
		os.Exit(1)
	}

	port := target.Port()

	if port == "" {
		port = map[string]string{"wss": "443", "ws": "80"}[target.Scheme]
	}

	probe, err := net.DialTimeout("tcp", net.JoinHostPort(target.Hostname(), port), 5*time.Second)

	if err != nil {
		fmt.Fprintln(os.Stderr, "The realtime service at "+target.Host+" is not reachable. Start the stack, or point CELERIS_WS_URL elsewhere.")
		os.Exit(1)
	}

	_ = probe.Close()
	os.Exit(m.Run())
}

// loadEnvironment reads KEY=VALUE lines without replacing variables already
// set.
func loadEnvironment(path string) {
	file, err := os.Open(path)

	if err != nil {
		return
	}

	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		key, value, found := strings.Cut(line, "=")

		if !found || strings.HasPrefix(line, "#") {
			continue
		}

		key = strings.TrimSpace(key)

		if _, set := os.LookupEnv(key); !set {
			_ = os.Setenv(key, strings.Trim(strings.TrimSpace(value), `'"`))
		}
	}
}

// claim is one wire claim; claims are written in the order given, which the
// tests keep to the wire's own: timestamp, reference, channel_references,
// token_permission, replay, allow_echo.
type claim struct {
	key   string
	value any
}

// signCredentials is hand-written from the protocol document, independent of
// the server module: client tests never import server code.
func signCredentials(clientID, signingSecret string, claims ...claim) celeris.Credentials {
	timestamp := time.Now().UnixMilli()
	var others []claim

	for _, entry := range claims {
		if entry.key == "timestamp" {
			timestamp = entry.value.(int64)
		} else {
			others = append(others, entry)
		}
	}

	payloadJSON := `{"timestamp":` + strconv.FormatInt(timestamp, 10)

	for _, entry := range others {
		encoded, err := json.Marshal(entry.value)

		if err != nil {
			panic(err)
		}

		payloadJSON += `,"` + entry.key + `":` + string(encoded)
	}

	payload := base64.StdEncoding.EncodeToString([]byte(payloadJSON + "}"))
	digest := hmac.New(sha512.New, []byte(signingSecret))
	digest.Write([]byte(payload))

	return celeris.Credentials{
		Payload:   payload,
		Signature: base64.StdEncoding.EncodeToString([]byte(clientID + ":" + hex.EncodeToString(digest.Sum(nil)))),
	}
}

func clientID() string      { return os.Getenv("CELERIS_CLIENT_ID") }
func signingSecret() string { return os.Getenv("CELERIS_SIGNING_SECRET") }
func websocketURL() string  { return os.Getenv("CELERIS_WS_URL") }

func uniqueChannelReference(label string) string {
	return "goqual-" + label + "-" + strconv.FormatInt(time.Now().UnixMilli(), 10) + "-" + strconv.FormatInt(channelCounter.Add(1), 10)
}

func clientWith(t *testing.T, baseURL string, provide celeris.CredentialProvider) *celeris.Client {
	t.Helper()

	client, err := celeris.NewClient(celeris.ClientOptions{
		CredentialProvider:    provide,
		BaseURL:               baseURL,
		AllowInsecureLoopback: true,
	})

	if err != nil {
		t.Fatal(err)
	}

	return client
}

func qualificationClient(t *testing.T, claims ...claim) *celeris.Client {
	t.Helper()

	return clientWith(t, websocketURL(), func(context.Context, celeris.CredentialRequest) (celeris.Credentials, error) {
		return signCredentials(clientID(), signingSecret(), claims...), nil
	})
}

func newChannel(t *testing.T, client *celeris.Client, reference string) *celeris.Channel {
	t.Helper()

	channel, err := client.Channel(reference)

	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(channel.Close)

	return channel
}

func connectedChannel(t *testing.T, reference string, claims ...claim) *celeris.Channel {
	t.Helper()

	channel := newChannel(t, qualificationClient(t, claims...), reference)

	if err := channel.Connect(t.Context()); err != nil {
		t.Fatalf("connect: %v", err)
	}

	return channel
}

func segment(t *testing.T, channel *celeris.Channel, segmentID string) *celeris.Segment {
	t.Helper()

	handle, err := channel.Segment(segmentID)

	if err != nil {
		t.Fatal(err)
	}

	return handle
}

func subscribe(t *testing.T, handle *celeris.Segment) *celeris.Subscription {
	t.Helper()

	subscription, err := handle.Subscribe()

	if err != nil {
		t.Fatal(err)
	}

	return subscription
}

type delivery struct {
	payload  []byte
	metadata celeris.MessageMetadata
}

// collector records every delivery on a segment.
type collector struct {
	mutex      sync.Mutex
	deliveries []delivery
}

func collect(handle *celeris.Segment) *collector {
	collected := &collector{}
	handle.OnMessage(func(payload []byte, metadata celeris.MessageMetadata) {
		collected.mutex.Lock()
		collected.deliveries = append(collected.deliveries, delivery{payload, metadata})
		collected.mutex.Unlock()
	})

	return collected
}

func (collected *collector) all() []delivery {
	collected.mutex.Lock()
	defer collected.mutex.Unlock()

	return append([]delivery(nil), collected.deliveries...)
}

// waitFor registers a listener and waits until one value satisfies predicate.
func waitFor[Value any](t *testing.T, register func(func(Value)) func(), predicate func(Value) bool, description string, timeout time.Duration) Value {
	t.Helper()

	arrived := make(chan Value, 1)
	var once sync.Once
	remove := register(func(value Value) {
		if predicate(value) {
			once.Do(func() { arrived <- value })
		}
	})

	defer remove()

	select {
	case value := <-arrived:
		return value
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s", description)

		var zero Value

		return zero
	}
}

func nextMessage(t *testing.T, handle *celeris.Segment, predicate func(delivery) bool, description string, timeout time.Duration) delivery {
	t.Helper()

	return waitFor(t, func(deliver func(delivery)) func() {
		return handle.OnMessage(func(payload []byte, metadata celeris.MessageMetadata) { deliver(delivery{payload, metadata}) })
	}, predicate, description, timeout)
}

func nextError(t *testing.T, channel *celeris.Channel, predicate func(error) bool, description string, timeout time.Duration) error {
	t.Helper()

	return waitFor(t, channel.Events().OnError, predicate, description, timeout)
}

func serverErrorOfType(errorType celeris.ServerErrorType) func(error) bool {
	return func(err error) bool {
		serverError, ok := errors.AsType[*celeris.ServerError](err)

		return ok && serverError.Type == errorType
	}
}

func patterned(length int) []byte {
	data := make([]byte, length)

	for index := range data {
		data[index] = byte(index % 251)
	}

	return data
}
