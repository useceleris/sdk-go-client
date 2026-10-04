package live

import (
	"context"
	"io"
	"net"
	"net/url"
	"slices"
	"sync"
	"testing"
	"time"

	celeris "github.com/useceleris/sdk-go-client"
)

// droppingProxy forwards TCP connections to the realtime service. It can cut
// every connection at once and refuse new ones, as a network outage would.
type droppingProxy struct {
	listener net.Listener
	target   string

	mutex    sync.Mutex
	refusing bool
	links    []net.Conn
}

func newDroppingProxy(t *testing.T) *droppingProxy {
	t.Helper()

	target, err := url.Parse(websocketURL())

	if err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")

	if err != nil {
		t.Fatal(err)
	}

	proxy := &droppingProxy{listener: listener, target: target.Host}
	t.Cleanup(func() {
		_ = listener.Close()
		proxy.dropAll()
	})

	go proxy.accept()

	return proxy
}

func (proxy *droppingProxy) url() string {
	return "ws://" + proxy.listener.Addr().String()
}

func (proxy *droppingProxy) accept() {
	for {
		client, err := proxy.listener.Accept()

		if err != nil {
			return
		}

		go proxy.link(client)
	}
}

func (proxy *droppingProxy) link(client net.Conn) {
	proxy.mutex.Lock()
	refusing := proxy.refusing
	proxy.mutex.Unlock()

	if refusing {
		_ = client.Close()

		return
	}

	upstream, err := net.Dial("tcp", proxy.target)

	if err != nil {
		_ = client.Close()

		return
	}

	proxy.mutex.Lock()
	proxy.links = append(proxy.links, client, upstream)
	proxy.mutex.Unlock()

	go func() {
		_, _ = io.Copy(upstream, client)
		_ = upstream.Close()
	}()

	_, _ = io.Copy(client, upstream)
	_ = client.Close()
}

func (proxy *droppingProxy) setRefusing(refusing bool) {
	proxy.mutex.Lock()
	proxy.refusing = refusing
	proxy.mutex.Unlock()
}

func (proxy *droppingProxy) dropAll() {
	proxy.mutex.Lock()
	links := proxy.links
	proxy.links = nil
	proxy.mutex.Unlock()

	for _, link := range links {
		_ = link.Close()
	}
}

// SUB-01, REC-02: an outage recovers with fresh credentials, a replay of what
// it missed, and the subscription restored; replayed duplicates are absorbed.
func TestRecoversAfterAnOutageWithReplayAndRestoredSubscriptions(t *testing.T) {
	proxy := newDroppingProxy(t)
	reference := uniqueChannelReference("reconnect")
	var mutex sync.Mutex
	var requests []celeris.CredentialRequest

	// The canonical mapping: a reconnect replays what the outage missed.
	client := clientWith(t, proxy.url(), func(_ context.Context, request celeris.CredentialRequest) (celeris.Credentials, error) {
		mutex.Lock()
		requests = append(requests, request)
		mutex.Unlock()

		if !request.Reconnect {
			return signCredentials(clientID(), signingSecret()), nil
		}

		return signCredentials(clientID(), signingSecret(), claim{"replay", request.ReplayLookback.Milliseconds()}), nil
	})

	receiver := newChannel(t, client, reference)
	recoveries := make(chan celeris.RecoveryEvent, 4)
	receiver.Events().OnRecovery(func(event celeris.RecoveryEvent) { recoveries <- event })
	chat := segment(t, receiver, "chat")
	delivered := collect(chat)
	subscribe(t, chat)

	if err := receiver.Connect(t.Context()); err != nil {
		t.Fatal(err)
	}

	publisher := connectedChannel(t, reference)
	time.Sleep(1500 * time.Millisecond)
	_ = segment(t, publisher, "chat").Publish(t.Context(), []byte("before"))
	nextMessage(t, chat, func(message delivery) bool { return string(message.payload) == "before" }, "the delivery before the outage", 15*time.Second)

	// The outage: every connection is cut and new ones are refused for a
	// while, so the next publish can only arrive by replay.
	proxy.setRefusing(true)
	proxy.dropAll()
	time.Sleep(500 * time.Millisecond)

	if receiver.State() != celeris.StateReconnecting {
		t.Fatalf("state %s during the outage", receiver.State())
	}

	_ = segment(t, publisher, "chat").Publish(t.Context(), []byte("during"))
	time.Sleep(2 * time.Second)
	proxy.setRefusing(false)

	nextMessage(t, chat, func(message delivery) bool { return string(message.payload) == "during" }, "the replayed delivery from the outage", 30*time.Second)

	select {
	case event := <-recoveries:
		if !event.PossibleGaps || !event.PossibleDuplicates {
			t.Fatalf("recovery %+v", event)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no recovery event")
	}

	mutex.Lock()

	for _, request := range requests[1:] {
		if !request.Reconnect || request.DisconnectedAt.IsZero() || request.ReplayLookback < 5*time.Second {
			t.Fatalf("reconnect request %+v", request)
		}
	}

	mutex.Unlock()

	// The subscription was restored on the new connection.
	_ = segment(t, publisher, "chat").Publish(t.Context(), []byte("after"))
	nextMessage(t, chat, func(message delivery) bool { return string(message.payload) == "after" }, "a live delivery after recovery", 15*time.Second)

	// Replay redelivered "before" too; the deduplication window absorbed it.
	var payloads []string
	identifiers := map[string]bool{}

	for _, message := range delivered.all() {
		payloads = append(payloads, string(message.payload))

		if identifiers[message.metadata.MessageID] {
			t.Fatalf("duplicate delivery %s", message.metadata.MessageID)
		}

		identifiers[message.metadata.MessageID] = true
	}

	if !slices.Equal(payloads, []string{"before", "during", "after"}) {
		t.Fatalf("payloads %v", payloads)
	}
}
