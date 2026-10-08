package celeris

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"
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

	// When each ping was sent, closed socket or not.
	pings []time.Time

	// When set, pongs wait until it closes, as on a dead path or while a
	// listener holds the receiver; otherwise each arrives at once.
	pongGate     chan struct{}
	pingsWaiting int

	// When set, writes wait until it closes or the socket does.
	writeGate chan struct{}
} // end struct fakeSocket

type fakeFrame struct {
	binary bool
	data   []byte
} // end struct fakeFrame

func newFakeSocket(url string) *fakeSocket {
	return &fakeSocket{url: url, incoming: make(chan fakeFrame, 64), closed: make(chan struct{})}
} // end function newFakeSocket

func (socket *fakeSocket) read(ctx context.Context) (bool, []byte, error) {
	select {
	case frame := <-socket.incoming:
		return frame.binary, frame.data, nil
	case <-socket.closed:
		return false, nil, errors.New("socket closed")
	case <-ctx.Done():
		return false, nil, ctx.Err()
	}
} // end method read

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
} // end method write

// ping records the ping and waits for its pong.
func (socket *fakeSocket) ping(ctx context.Context) error {
	socket.mutex.Lock()
	socket.pings = append(socket.pings, time.Now())
	pongGate := socket.pongGate
	socket.mutex.Unlock()

	if pongGate == nil {
		return nil
	}

	socket.mutex.Lock()
	socket.pingsWaiting++
	socket.mutex.Unlock()

	defer func() {
		socket.mutex.Lock()
		socket.pingsWaiting--
		socket.mutex.Unlock()
	}()

	select {
	case <-pongGate:
		return nil
	case <-socket.closed:
		return errors.New("socket closed")
	case <-ctx.Done():
		return ctx.Err()
	}
} // end method ping

func (socket *fakeSocket) close() error {
	socket.mutex.Lock()
	socket.graceful = true
	socket.mutex.Unlock()

	return socket.closeNow()
} // end method close

func (socket *fakeSocket) closeNow() error {
	socket.closeOnce.Do(func() { close(socket.closed) })

	return nil
} // end method closeNow

// receive delivers a binary message from the server.
func (socket *fakeSocket) receive(data string) {
	socket.incoming <- fakeFrame{binary: true, data: []byte(data)}
} // end method receive

// drop closes the socket from the server's side.
func (socket *fakeSocket) drop() {
	_ = socket.closeNow()
} // end method drop

func (socket *fakeSocket) isClosed() bool {
	select {
	case <-socket.closed:
		return true
	default:
		return false
	}
} // end method isClosed

func (socket *fakeSocket) commands() []string {
	socket.mutex.Lock()
	defer socket.mutex.Unlock()

	return slices.Clone(socket.written)
} // end method commands

func (socket *fakeSocket) setFailWrites(fail bool) {
	socket.mutex.Lock()
	socket.failWrites = fail
	socket.mutex.Unlock()
} // end method setFailWrites

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
} // end method holdWrites

// withholdPongs makes pongs wait until answer is called, which delivers every
// pong still awaited and the later ones at once.
func (socket *fakeSocket) withholdPongs() (answer func()) {
	gate := make(chan struct{})

	socket.mutex.Lock()
	socket.pongGate = gate
	socket.mutex.Unlock()

	return func() {
		socket.mutex.Lock()
		socket.pongGate = nil
		socket.mutex.Unlock()
		close(gate)
	}
} // end method withholdPongs

// waitingPings counts the pings still waiting for a withheld pong.
func (socket *fakeSocket) waitingPings() int {
	socket.mutex.Lock()
	defer socket.mutex.Unlock()

	return socket.pingsWaiting
} // end method waitingPings

func (socket *fakeSocket) pingTimes() []time.Time {
	socket.mutex.Lock()
	defer socket.mutex.Unlock()

	return slices.Clone(socket.pings)
} // end method pingTimes

func (socket *fakeSocket) clearCommands() {
	socket.mutex.Lock()
	socket.written = nil
	socket.mutex.Unlock()
} // end method clearCommands

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
} // end struct fakeServer

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
} // end method dial

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
} // end method provide

func (server *fakeServer) set(change func(server *fakeServer)) {
	server.mutex.Lock()
	change(server)
	server.mutex.Unlock()
} // end method set

func (server *fakeServer) socket(index int) *fakeSocket {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	if index < 0 {
		index += len(server.sockets)
	}

	return server.sockets[index]
} // end method socket

func (server *fakeServer) socketCount() int {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	return len(server.sockets)
} // end method socketCount

func (server *fakeServer) credentialRequests() []CredentialRequest {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	return slices.Clone(server.requests)
} // end method credentialRequests

// newTestChannel returns a channel for "room-1" from a client with default
// options, whose sockets and credentials come from a fake server.
func newTestChannel(t *testing.T) (*Channel, *fakeServer) {
	t.Helper()

	return newClientChannel(t, ClientOptions{})
} // end function newTestChannel

// newClientChannel builds a client through NewClient and returns its channel
// for "room-1". The fake server supplies the provider when options have none,
// and every socket. The base URL defaults to wss://example.test/ and jitter is
// fixed at zero.
func newClientChannel(t *testing.T, options ClientOptions) (*Channel, *fakeServer) {
	t.Helper()

	server := &fakeServer{credentials: testCredentials}

	if options.CredentialProvider == nil {
		options.CredentialProvider = server.provide
	}

	if options.BaseURL == "" {
		options.BaseURL = "wss://example.test/"
	}

	client, err := NewClient(options)

	if err != nil {
		t.Fatal(err)
	}

	client.dial = server.dial
	client.random = func() float64 { return 0 }

	channel, err := client.Channel("room-1")

	if err != nil {
		t.Fatal(err)
	}

	// Closing stops the channel's goroutines, so a failed test cannot leave a
	// bubble deadlocked.
	t.Cleanup(channel.Close)

	return channel, server
} // end function newClientChannel

// connectTestChannel connects a new test channel and returns its socket.
func connectTestChannel(t *testing.T) (*Channel, *fakeServer, *fakeSocket) {
	t.Helper()

	return connectClientChannel(t, ClientOptions{})
} // end function connectTestChannel

// connectClientChannel connects a channel from newClientChannel and returns
// its socket.
func connectClientChannel(t *testing.T, options ClientOptions) (*Channel, *fakeServer, *fakeSocket) {
	t.Helper()

	channel, server := newClientChannel(t, options)

	if err := channel.Connect(t.Context()); err != nil {
		t.Fatalf("connect: %v", err)
	}

	return channel, server, server.socket(-1)
} // end function connectClientChannel

// expectConnectTimeout starts a connect whose credentials never arrive and
// checks that it is still pending just before timeout and fails with
// ErrTimeout at it. It returns that error.
func expectConnectTimeout(t *testing.T, channel *Channel, server *fakeServer, timeout time.Duration) error {
	t.Helper()

	server.set(func(server *fakeServer) { server.blockProvider = true })
	defer server.set(func(server *fakeServer) { server.blockProvider = false })

	result := make(chan error, 1)

	go func() { result <- channel.Connect(t.Context()) }()

	synctest.Sleep(timeout - time.Millisecond)

	select {
	case err := <-result:
		t.Fatalf("connect ended before %v: %v", timeout, err)
	default:
	}

	synctest.Sleep(time.Millisecond)

	var err error

	select {
	case err = <-result:
	default:
		t.Fatalf("connect still pending at %v", timeout)
	}

	assertCode(t, err, ErrTimeout)

	return err
} // end function expectConnectTimeout

// expectReconnectTimeout drops the connected socket while credentials never
// arrive, and checks that the first reconnect attempt is still pending just
// before timeout and gives way to the next attempt at it.
func expectReconnectTimeout(t *testing.T, server *fakeServer, timeout time.Duration) {
	t.Helper()

	server.set(func(server *fakeServer) { server.blockProvider = true })
	before := len(server.credentialRequests())
	server.socket(-1).drop()
	synctest.Wait()

	if len(server.credentialRequests()) != before+1 {
		t.Fatal("no reconnect attempt started")
	}

	synctest.Sleep(timeout - time.Millisecond)

	if len(server.credentialRequests()) != before+1 {
		t.Fatalf("reconnect attempt ended before %v", timeout)
	}

	synctest.Sleep(time.Millisecond)

	if len(server.credentialRequests()) != before+2 {
		t.Fatalf("reconnect attempt still pending at %v", timeout)
	}
} // end function expectReconnectTimeout

// expectPresenceTimeout starts a presence query that is never answered and
// checks that it is still pending just before timeout and fails with
// ErrTimeout at it, leaving the channel connected. It returns that error.
func expectPresenceTimeout(t *testing.T, channel *Channel, timeout time.Duration) error {
	t.Helper()

	pending := queryAsync(t.Context(), segment(t, channel, "chat"), 1, 25)
	synctest.Sleep(timeout - time.Millisecond)

	select {
	case outcome := <-pending:
		t.Fatalf("query ended before %v: %v", timeout, outcome.err)
	default:
	}

	synctest.Sleep(time.Millisecond)

	var outcome queryOutcome

	select {
	case outcome = <-pending:
	default:
		t.Fatalf("query still pending at %v", timeout)
	}

	assertCode(t, outcome.err, ErrTimeout)

	if channel.State() != StateConnected {
		t.Fatalf("state %s after the query timed out", channel.State())
	}

	return outcome.err
} // end function expectPresenceTimeout

// recorder collects what listeners receive. It is safe for the goroutine that
// delivers events and the test goroutine to share.
type recorder[Value any] struct {
	mutex  sync.Mutex
	values []Value
} // end struct recorder

func (recorder *recorder[Value]) record(value Value) {
	recorder.mutex.Lock()
	recorder.values = append(recorder.values, value)
	recorder.mutex.Unlock()
} // end method record

func (recorder *recorder[Value]) all() []Value {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()

	return slices.Clone(recorder.values)
} // end method all

func recordStates(channel *Channel) *recorder[ChannelState] {
	states := &recorder[ChannelState]{}
	channel.Events().OnStateChange(states.record)

	return states
} // end function recordStates

func recordErrors(channel *Channel) *recorder[error] {
	errorsSeen := &recorder[error]{}
	channel.Events().OnError(errorsSeen.record)

	return errorsSeen
} // end function recordErrors

func assertCode(t *testing.T, err error, code ErrorCode) {
	t.Helper()

	if !errors.Is(err, code) {
		t.Fatalf("got %v, want %s", err, code)
	}
} // end function assertCode

func assertStates(t *testing.T, states *recorder[ChannelState], want ...ChannelState) {
	t.Helper()

	if got := states.all(); !slices.Equal(got, want) {
		t.Fatalf("states %v, want %v", got, want)
	}
} // end function assertStates

func assertCommands(t *testing.T, socket *fakeSocket, want ...string) {
	t.Helper()

	if got := socket.commands(); !slices.Equal(got, want) {
		t.Fatalf("commands %q, want %q", got, want)
	}
} // end function assertCommands

func segment(t *testing.T, channel *Channel, segmentID string) *Segment {
	t.Helper()

	handle, err := channel.Segment(segmentID)

	if err != nil {
		t.Fatal(err)
	}

	return handle
} // end function segment

func subscribe(t *testing.T, handle *Segment) *Subscription {
	t.Helper()

	subscription, err := handle.Subscribe()

	if err != nil {
		t.Fatal(err)
	}

	return subscription
} // end function subscribe

func subscribePresence(t *testing.T, handle *Segment) *Subscription {
	t.Helper()

	subscription, err := handle.SubscribePresence()

	if err != nil {
		t.Fatal(err)
	}

	return subscription
} // end function subscribePresence

// messageFrame lays out a delivery as the server does.
func messageFrame(segmentID, messageID, body string) string {
	identifier := "$-1\n"

	if messageID != "" {
		identifier = bulk(messageID)
	}

	return "@MSG\n$4\nuser\n" + bulk(segmentID) + identifier + ":1\n" + bulk(body)
} // end function messageFrame

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
} // end function errorFrame

func rateLimitFrame() string {
	return errorFrame("RateLimitError", "Rate limit exceeded", "", "")
} // end function rateLimitFrame

func bulk(value string) string {
	return "$" + strconv.Itoa(len(value)) + "\n" + value + "\n"
} // end function bulk

func publishFrame(segmentID, messageID, body string) string {
	return "@PUB\n" + bulk(segmentID) + bulk(messageID) + bulk(body)
} // end function publishFrame

// publishAsync publishes from its own goroutine, since Publish waits for the
// socket.
func publishAsync(t *testing.T, handle *Segment, messageID, body string) <-chan error {
	result := make(chan error, 1)

	go func() { result <- handle.PublishWithMessageID(t.Context(), []byte(body), messageID) }()

	return result
} // end function publishAsync

func publish(t *testing.T, handle *Segment, messageID, body string) {
	t.Helper()

	if err := handle.PublishWithMessageID(t.Context(), []byte(body), messageID); err != nil {
		t.Fatalf("publish %s: %v", messageID, err)
	}
} // end function publish
