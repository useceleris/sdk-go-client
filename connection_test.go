package celeris

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// These tests drive the real WebSocket library against an in-process server,
// in real time.

type realServer struct {
	*httptest.Server
	accepted chan *websocket.Conn
	requests chan *http.Request
	commands chan string

	// A silent server never reads, so it never answers a closing handshake.
	silent bool
}

func newRealServer(t *testing.T, secure, silent bool) *realServer {
	t.Helper()

	server := &realServer{
		accepted: make(chan *websocket.Conn, 4),
		requests: make(chan *http.Request, 4),
		commands: make(chan string, 16),
		silent:   silent,
	}
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		server.requests <- request

		if request.URL.Path != "/channel/room-1" {
			http.Error(writer, "unknown channel", http.StatusNotFound)

			return
		}

		conn, err := websocket.Accept(writer, request, nil)

		if err != nil {
			return
		}

		conn.SetReadLimit(-1)
		server.accepted <- conn

		if server.silent {
			<-request.Context().Done()

			return
		}

		for {
			_, data, err := conn.Read(context.Background())

			if err != nil {
				return
			}

			server.commands <- string(data)
		}
	})

	if secure {
		server.Server = httptest.NewTLSServer(handler)
	} else {
		server.Server = httptest.NewServer(handler)
	}

	t.Cleanup(server.Close)

	return server
}

func (server *realServer) baseURL() string {
	return "ws" + strings.TrimPrefix(server.URL, "http")
}

func newRealChannel(t *testing.T, baseURL string) *Channel {
	t.Helper()

	client, err := NewClient(ClientOptions{
		CredentialProvider:    func(context.Context, CredentialRequest) (Credentials, error) { return testCredentials, nil },
		BaseURL:               baseURL,
		AllowInsecureLoopback: true,
		ConnectTimeout:        5 * time.Second,
	})

	if err != nil {
		t.Fatal(err)
	}

	channel, err := client.Channel("room-1")

	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(channel.Close)

	return channel
}

func (server *realServer) readCommand(t *testing.T) string {
	t.Helper()

	select {
	case command := <-server.commands:
		return command
	case <-time.After(5 * time.Second):
		t.Fatal("the server read nothing")

		return ""
	}
}

func TestRealSocketRoundTrip(t *testing.T) {
	server := newRealServer(t, false, false)
	channel := newRealChannel(t, server.baseURL())
	delivered := make(chan []byte, 1)
	segment(t, channel, "chat").OnMessage(func(payload []byte, _ MessageMetadata) { delivered <- payload })

	if err := channel.Connect(t.Context()); err != nil {
		t.Fatal(err)
	}

	request := <-server.requests

	if query := request.URL.Query(); query.Get("payload") != "payload-1" || query.Get("signature") != "signature-1" {
		t.Fatalf("handshake query %q", request.URL.RawQuery)
	}

	conn := <-server.accepted

	// The attempt's context was released when Connect returned; the
	// connection must outlive it.
	time.Sleep(50 * time.Millisecond)

	if err := segment(t, channel, "chat").PublishWithMessageID(t.Context(), []byte("hi"), "m-1"); err != nil {
		t.Fatal(err)
	}

	if command := server.readCommand(t); command != publishFrame("chat", "m-1", "hi") {
		t.Fatalf("server read %q", command)
	}

	// LIMIT-01: a received message over 1 MiB is delivered, not refused.
	large := strings.Repeat("x", 1536*1024)

	if err := conn.Write(t.Context(), websocket.MessageBinary, []byte(messageFrame("chat", "id-1", large))); err != nil {
		t.Fatal(err)
	}

	select {
	case payload := <-delivered:
		if len(payload) != len(large) {
			t.Fatalf("delivered %d bytes", len(payload))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("nothing delivered")
	}

	// A text message is dropped and reported; the connection stays.
	reported := make(chan error, 1)
	channel.Events().OnError(func(err error) { reported <- err })

	if err := conn.Write(t.Context(), websocket.MessageText, []byte("hello")); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-reported:
		assertCode(t, err, ErrProtocol)
	case <-time.After(5 * time.Second):
		t.Fatal("text message not reported")
	}

	if channel.State() != StateConnected {
		t.Fatalf("state %s", channel.State())
	}
}

// SEC-02: certificates are verified against the system roots, and an
// application loosening http.DefaultTransport does not loosen the SDK.
func TestRealSocketVerifiesCertificates(t *testing.T) {
	server := newRealServer(t, true, false)
	defaultTransport := http.DefaultTransport.(*http.Transport)
	original := defaultTransport.TLSClientConfig
	defaultTransport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	t.Cleanup(func() { defaultTransport.TLSClientConfig = original })

	channel := newRealChannel(t, "wss"+strings.TrimPrefix(server.URL, "https"))
	err := channel.Connect(t.Context())
	assertCode(t, err, ErrTransport)

	if strings.Contains(err.Error(), "payload-1") || strings.Contains(err.Error(), "signature-1") || errors.Unwrap(err) != nil {
		t.Fatalf("error leaks the credential URL: %v", err)
	}
}

func TestRealSocketRefusedHandshakeIsTransport(t *testing.T) {
	server := newRealServer(t, false, false)
	client, err := NewClient(ClientOptions{
		CredentialProvider:    func(context.Context, CredentialRequest) (Credentials, error) { return testCredentials, nil },
		BaseURL:               server.baseURL() + "/elsewhere",
		AllowInsecureLoopback: true,
	})

	if err != nil {
		t.Fatal(err)
	}

	channel, _ := client.Channel("room-1")
	t.Cleanup(channel.Close)
	err = channel.Connect(t.Context())
	assertCode(t, err, ErrTransport)

	if strings.Contains(err.Error(), "payload-1") || strings.Contains(err.Error(), "404") {
		t.Fatalf("error leaks handshake detail: %v", err)
	}
}

// Close stays bounded against a peer that never answers the closing
// handshake.
func TestRealSocketCloseIsBoundedAgainstASilentPeer(t *testing.T) {
	server := newRealServer(t, false, true)
	channel := newRealChannel(t, server.baseURL())

	if err := channel.Connect(t.Context()); err != nil {
		t.Fatal(err)
	}

	<-server.accepted
	start := time.Now()
	channel.Close()

	if elapsed := time.Since(start); elapsed > 6*time.Second || channel.State() != StateClosed {
		t.Fatalf("closed after %v in state %s", elapsed, channel.State())
	}
}
