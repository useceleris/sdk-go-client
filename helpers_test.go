package celeris

import (
	"context"
	"errors"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"testing"
)

var testCredentials = Credentials{Payload: "payload-1", Signature: "signature-1"}

// fakeSocket is an in-memory peer built on channels, so a synctest bubble sees
// every wait on it as durably blocking.
type fakeSocket struct {
	url      string
	incoming chan fakeFrame
	closed   chan struct{}

	mutex      sync.Mutex
	written    []string
	graceful   bool
	closeOnce  sync.Once
	failWrites bool

	// When set, writes wait until it closes or the socket does.
	writeGate chan struct{}
}

type fakeFrame struct {
	binary bool
	data   []byte
}

func newFakeSocket(url string) *fakeSocket {
	return &fakeSocket{url: url, incoming: make(chan fakeFrame, 64), closed: make(chan struct{})}
}

func (socket *fakeSocket) read(ctx context.Context) (bool, []byte, error) {
	select {
	case frame := <-socket.incoming:
		return frame.binary, frame.data, nil
	case <-socket.closed:
		return false, nil, errors.New("socket closed")
	case <-ctx.Done():
		return false, nil, ctx.Err()
	}
}

func (socket *fakeSocket) write(ctx context.Context, data []byte) error {
	socket.mutex.Lock()
	failWrites, writeGate := socket.failWrites, socket.writeGate
	socket.mutex.Unlock()

	if failWrites {
		return errors.New("synthetic write failure")
	}

	if writeGate != nil {
		select {
		case <-writeGate:
		case <-socket.closed:
			return errors.New("socket closed")
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	select {
	case <-socket.closed:
		return errors.New("socket closed")
	default:
	}

	socket.mutex.Lock()
	socket.written = append(socket.written, string(data))
	socket.mutex.Unlock()

	return nil
}

func (socket *fakeSocket) close() error {
	socket.mutex.Lock()
	socket.graceful = true
	socket.mutex.Unlock()

	return socket.closeNow()
}

func (socket *fakeSocket) closeNow() error {
	socket.closeOnce.Do(func() { close(socket.closed) })

	return nil
}

// receive delivers a binary message from the server.
func (socket *fakeSocket) receive(data string) {
	socket.incoming <- fakeFrame{binary: true, data: []byte(data)}
}

// drop closes the socket from the server's side.
func (socket *fakeSocket) drop() {
	_ = socket.closeNow()
}

func (socket *fakeSocket) isClosed() bool {
	select {
	case <-socket.closed:
		return true
	default:
		return false
	}
}

func (socket *fakeSocket) commands() []string {
	socket.mutex.Lock()
	defer socket.mutex.Unlock()

	return slices.Clone(socket.written)
}

func (socket *fakeSocket) setFailWrites(fail bool) {
	socket.mutex.Lock()
	socket.failWrites = fail
	socket.mutex.Unlock()
}

// holdWrites makes writes wait until release is called.
func (socket *fakeSocket) holdWrites() (release func()) {
	gate := make(chan struct{})

	socket.mutex.Lock()
	socket.writeGate = gate
	socket.mutex.Unlock()

	return func() {
		socket.mutex.Lock()
		socket.writeGate = nil
		socket.mutex.Unlock()
		close(gate)
	}
}

func (socket *fakeSocket) clearCommands() {
	socket.mutex.Lock()
	socket.written = nil
	socket.mutex.Unlock()
}

// fakeServer dials fake sockets and records every credential request.
type fakeServer struct {
	mutex    sync.Mutex
	sockets  []*fakeSocket
	requests []CredentialRequest

	// Dials fail while positive, counting down.
	failDials int

	// When set, dials wait for their context to end.
	blockDials bool

	// When set, the provider waits for its context to end.
	blockProvider bool

	// When set, the provider fails.
	failProvider bool

	credentials Credentials
}

func (server *fakeServer) dial(ctx context.Context, address string) (socket, error) {
	server.mutex.Lock()

	if server.failDials > 0 {
		server.failDials--
		server.mutex.Unlock()

		// Real dial errors quote the URL; the channel must never pass it on.
		return nil, errors.New("synthetic dial failure for " + address)
	}

	block := server.blockDials
	server.mutex.Unlock()

	if block {
		<-ctx.Done()

		return nil, ctx.Err()
	}

	socket := newFakeSocket(address)

	server.mutex.Lock()
	server.sockets = append(server.sockets, socket)
	server.mutex.Unlock()

	return socket, nil
}

func (server *fakeServer) provide(ctx context.Context, request CredentialRequest) (Credentials, error) {
	server.mutex.Lock()
	server.requests = append(server.requests, request)
	block, fail, credentials := server.blockProvider, server.failProvider, server.credentials
	server.mutex.Unlock()

	if block {
		<-ctx.Done()

		return Credentials{}, errors.New("synthetic-secret provider failure")
	}

	if fail {
		return Credentials{}, errors.New("synthetic-secret provider failure")
	}

	return credentials, nil
}

func (server *fakeServer) set(change func(server *fakeServer)) {
	server.mutex.Lock()
	change(server)
	server.mutex.Unlock()
}

func (server *fakeServer) socket(index int) *fakeSocket {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	if index < 0 {
		index += len(server.sockets)
	}

	return server.sockets[index]
}

func (server *fakeServer) socketCount() int {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	return len(server.sockets)
}

func (server *fakeServer) credentialRequests() []CredentialRequest {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	return slices.Clone(server.requests)
}

// newTestChannel returns a channel for "room-1" whose sockets and credentials
// come from a fake server, with jitter fixed at zero.
func newTestChannel(t *testing.T) (*Channel, *fakeServer) {
	t.Helper()

	server := &fakeServer{credentials: testCredentials}
	baseURL, err := url.Parse("wss://example.test/")

	if err != nil {
		t.Fatal(err)
	}

	client := &Client{
		credentialProvider:   server.provide,
		baseURL:              baseURL,
		connectTimeout:       defaultConnectTimeout,
		presenceQueryTimeout: defaultPresenceQueryTimeout,
		dial:                 server.dial,
		random:               func() float64 { return 0 },
	}

	channel, err := client.Channel("room-1")

	if err != nil {
		t.Fatal(err)
	}

	// Closing stops the channel's goroutines, so a failed test cannot leave a
	// bubble deadlocked.
	t.Cleanup(channel.Close)

	return channel, server
}

// connectTestChannel connects a new test channel and returns its socket.
func connectTestChannel(t *testing.T) (*Channel, *fakeServer, *fakeSocket) {
	t.Helper()

	channel, server := newTestChannel(t)

	if err := channel.Connect(t.Context()); err != nil {
		t.Fatalf("connect: %v", err)
	}

	return channel, server, server.socket(-1)
}

// recorder collects what listeners receive. It is safe for the goroutine that
// delivers events and the test goroutine to share.
type recorder[Value any] struct {
	mutex  sync.Mutex
	values []Value
}

func (recorder *recorder[Value]) record(value Value) {
	recorder.mutex.Lock()
	recorder.values = append(recorder.values, value)
	recorder.mutex.Unlock()
}

func (recorder *recorder[Value]) all() []Value {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()

	return slices.Clone(recorder.values)
}

func recordStates(channel *Channel) *recorder[ChannelState] {
	states := &recorder[ChannelState]{}
	channel.Events().OnStateChange(states.record)

	return states
}

func recordErrors(channel *Channel) *recorder[error] {
	errorsSeen := &recorder[error]{}
	channel.Events().OnError(errorsSeen.record)

	return errorsSeen
}

func assertCode(t *testing.T, err error, code ErrorCode) {
	t.Helper()

	if !errors.Is(err, code) {
		t.Fatalf("got %v, want %s", err, code)
	}
}

func assertStates(t *testing.T, states *recorder[ChannelState], want ...ChannelState) {
	t.Helper()

	if got := states.all(); !slices.Equal(got, want) {
		t.Fatalf("states %v, want %v", got, want)
	}
}

func assertCommands(t *testing.T, socket *fakeSocket, want ...string) {
	t.Helper()

	if got := socket.commands(); !slices.Equal(got, want) {
		t.Fatalf("commands %q, want %q", got, want)
	}
}

func segment(t *testing.T, channel *Channel, segmentID string) *Segment {
	t.Helper()

	handle, err := channel.Segment(segmentID)

	if err != nil {
		t.Fatal(err)
	}

	return handle
}

func subscribe(t *testing.T, handle *Segment) *Subscription {
	t.Helper()

	subscription, err := handle.Subscribe()

	if err != nil {
		t.Fatal(err)
	}

	return subscription
}

func subscribePresence(t *testing.T, handle *Segment) *Subscription {
	t.Helper()

	subscription, err := handle.SubscribePresence()

	if err != nil {
		t.Fatal(err)
	}

	return subscription
}

// messageFrame lays out a delivery as the server does.
func messageFrame(segmentID, messageID, body string) string {
	identifier := "$-1\n"

	if messageID != "" {
		identifier = bulk(messageID)
	}

	return "@MSG\n$4\nuser\n" + bulk(segmentID) + identifier + ":1\n" + bulk(body)
}

// errorFrame lays out an error frame as the server does; an empty sub type
// is null.
func errorFrame(errorType, message, subType, resource string) string {
	subTypeField := "$-1\n"

	if subType != "" {
		subTypeField = "+" + subType + "\n"
	}

	if resource == "" {
		resource = "$-1\n"
	}

	return "-Err\n+" + errorType + "\n" + subTypeField + bulk(message) + resource
}

func rateLimitFrame() string {
	return errorFrame("RateLimitError", "Rate limit exceeded", "", "")
}

func bulk(value string) string {
	return "$" + strconv.Itoa(len(value)) + "\n" + value + "\n"
}

func publishFrame(segmentID, messageID, body string) string {
	return "@PUB\n" + bulk(segmentID) + bulk(messageID) + bulk(body)
}

// publishAsync publishes from its own goroutine, since Publish waits for the
// socket.
func publishAsync(t *testing.T, handle *Segment, messageID, body string) <-chan error {
	result := make(chan error, 1)

	go func() { result <- handle.PublishWithMessageID(t.Context(), []byte(body), messageID) }()

	return result
}

func publish(t *testing.T, handle *Segment, messageID, body string) {
	t.Helper()

	if err := handle.PublishWithMessageID(t.Context(), []byte(body), messageID); err != nil {
		t.Fatalf("publish %s: %v", messageID, err)
	}
}
