package celeris

import (
	"context"
	"net"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// socket is the one transport seam: the WebSocket a connection drives, so
// tests can substitute an in-memory peer.
type socket interface {
	// read returns the next message, and whether it was binary.
	read(ctx context.Context) (binary bool, data []byte, err error)

	write(ctx context.Context, data []byte) error

	// ping sends a ping frame, then waits for its pong until ctx ends. A pong
	// that never arrives fails only the call, never the socket.
	ping(ctx context.Context) error

	// close performs the closing handshake. The caller bounds how long it may
	// take.
	close() error

	closeNow() error
} // end interface socket

// dialer opens a socket to url, which carries the credentials.
type dialer func(ctx context.Context, url string) (socket, error)

// dialWebSocket dials through the package's own HTTP transport.
func dialWebSocket() dialer {
	httpClient := &http.Client{Transport: httpTransport()}

	return func(ctx context.Context, url string) (socket, error) {
		conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPClient: httpClient})

		if err != nil {
			// The error quotes the URL and with it the credentials, so the
			// caller replaces it with fixed text and never passes it on.
			return nil, err
		}

		// Received messages are never size-checked (LIMIT-01).
		conn.SetReadLimit(-1)

		return webSocket{conn: conn}, nil
	}
} // end function dialWebSocket

// httpTransport is the package's own, so changes an application makes to
// http.DefaultTransport, such as skipping certificate verification, cannot
// weaken a connection (SEC-02). Setting its dialer also turns off automatic
// HTTP/2, which a WebSocket upgrade never uses.
//
// The dialer also probes an idle socket, a backstop to the heartbeat for a
// silently dead path, such as a dropped NAT mapping: the kernel sends and
// answers the probes whatever the listeners do. Linux probes only while
// nothing written awaits acknowledgement, which the heartbeat's pings rarely
// leave it.
func httpTransport() *http.Transport {
	dialer := &net.Dialer{KeepAliveConfig: net.KeepAliveConfig{
		Enable:   true,
		Idle:     keepAliveIdle,
		Interval: keepAliveInterval,
		Count:    keepAliveCount,
	}}

	return &http.Transport{Proxy: http.ProxyFromEnvironment, DialContext: dialer.DialContext}
} // end function httpTransport

type webSocket struct {
	conn *websocket.Conn
} // end struct webSocket

func (socket webSocket) read(ctx context.Context) (bool, []byte, error) {
	messageType, data, err := socket.conn.Read(ctx)

	return messageType == websocket.MessageBinary, data, err
} // end method read

func (socket webSocket) write(ctx context.Context, data []byte) error {
	return socket.conn.Write(ctx, websocket.MessageBinary, data)
} // end method write

// The library's Ping closes the socket only when writing the ping frame takes
// longer than five seconds; a context ending while it waits for the pong ends
// the wait alone.
func (socket webSocket) ping(ctx context.Context) error {
	return socket.conn.Ping(ctx)
} // end method ping

func (socket webSocket) close() error {
	return socket.conn.Close(websocket.StatusNormalClosure, "")
} // end method close

func (socket webSocket) closeNow() error {
	return socket.conn.CloseNow()
} // end method closeNow

// outboundFrame is one command handed to the writer.
type outboundFrame struct {
	data []byte

	// Set for a publish, which settles once written.
	publish *queuedPublish
} // end struct outboundFrame

// connection is one socket and the goroutines that drive it: a writer that
// writes handed-off frames in order, a receiver that routes what arrives, and
// a heartbeat that pings the server and finds a dead path. Its fields are
// guarded by the channel's mutex.
type connection struct {
	channel *Channel
	socket  socket

	// Scopes reads and writes: the socket closes when a read or write context
	// ends, so it outlives any caller's context.
	context context.Context
	cancel  context.CancelFunc

	// Frames handed to the writer and not yet written, the one being written
	// first. They are bounded by count and bytes, so a maximum-size command
	// always fits an empty writer.
	outbound      []*outboundFrame
	outboundBytes int
	writing       *outboundFrame

	ready *sync.Cond

	// closing asks the writer to flush, then perform the closing handshake;
	// broken asks it to stop at once.
	closing bool
	broken  bool

	writerDone chan struct{}

	// Reading time: how long the receiver has waited in read, the only time a
	// pong or a message can arrive. It is the finished reads' total, plus the
	// current read's time since readingSince.
	readingTime  time.Duration
	reading      bool
	readingSince time.Time

	// Whether a ping awaits its pong, and the reading time when the oldest
	// such ping was sent.
	pingUnanswered  bool
	unansweredSince time.Duration

	// When the server was last heard from: the socket opening, a pong or a
	// received message.
	lastHeard time.Time
} // end struct connection

func newConnection(channel *Channel, socket socket) *connection {
	connectionContext, cancel := context.WithCancel(context.Background())

	return &connection{
		channel:    channel,
		socket:     socket,
		context:    connectionContext,
		cancel:     cancel,
		ready:      sync.NewCond(&channel.mutex),
		writerDone: make(chan struct{}),
		lastHeard:  time.Now(),
	}
} // end function newConnection

func (connection *connection) hasRoom(size int) bool {
	return len(connection.outbound) < maximumPendingCommands && connection.outboundBytes+size <= maximumBufferedBytes
} // end method hasRoom

func (connection *connection) handOff(frame *outboundFrame) {
	connection.outbound = append(connection.outbound, frame)
	connection.outboundBytes += len(frame.data)
	connection.ready.Signal()
} // end method handOff

// remove takes back every frame of publish the writer has not started, and
// reports whether there was one. A rate limit can requeue a publish still in
// the writer, so it may have been handed over twice.
func (connection *connection) remove(publish *queuedPublish) bool {
	before := len(connection.outbound)

	connection.outbound = slices.DeleteFunc(connection.outbound, func(frame *outboundFrame) bool {
		if frame.publish != publish || frame == connection.writing {
			return false
		}

		connection.outboundBytes -= len(frame.data)

		return true
	})

	return len(connection.outbound) < before
} // end method remove

// abandon stops the connection without a closing handshake. The caller holds
// the mutex, has detached the connection, and has taken back or failed the
// publishes the socket never started writing.
func (connection *connection) abandon() {
	if connection.broken {
		return
	}

	connection.broken = true
	connection.ready.Broadcast()

	// Cancelling the read and write context first makes the library drop a
	// closing handshake in progress at once.
	go func() {
		connection.cancel()
		_ = connection.socket.closeNow()
	}()
} // end method abandon

// failUnwritten fails every publish handed to this connection that the socket
// never started writing.
func (connection *connection) failUnwritten(err error) {
	for _, frame := range connection.outbound {
		if frame != connection.writing && frame.publish != nil {
			frame.publish.settle(err)
		}
	}

	connection.dropUnwritten()
} // end method failUnwritten

// takeUnwritten takes back, unsettled and in order, every publish handed to
// this connection that the socket never started writing, so recovery can
// queue it for the next socket (QUEUE-01). A rate limit can requeue a publish
// still in the writer, so it may appear twice; it is taken once. A publish
// already written, or being written, keeps that outcome: only a rate limit
// resends.
func (connection *connection) takeUnwritten() []*queuedPublish {
	var publishes []*queuedPublish

	for _, frame := range connection.outbound {
		publish := frame.publish
		beingWritten := connection.writing != nil && connection.writing.publish == publish

		if publish == nil || publish.settled || beingWritten || slices.Contains(publishes, publish) {
			continue
		}

		publish.connection = nil
		publishes = append(publishes, publish)
	}

	connection.dropUnwritten()

	return publishes
} // end method takeUnwritten

// dropUnwritten empties the writer except for the frame the socket is
// writing.
func (connection *connection) dropUnwritten() {
	if connection.writing != nil {
		connection.outbound = []*outboundFrame{connection.writing}
		connection.outboundBytes = len(connection.writing.data)
	} else {
		connection.outbound = nil
		connection.outboundBytes = 0
	}
} // end method dropUnwritten

// writeLoop writes handed-off frames in order until the connection breaks, or
// until it closes and every frame is flushed.
func (connection *connection) writeLoop() {
	defer close(connection.writerDone)

	channel := connection.channel

	for {
		channel.mutex.Lock()

		for len(connection.outbound) == 0 && !connection.closing && !connection.broken {
			connection.ready.Wait()
		}

		if connection.broken || len(connection.outbound) == 0 {
			closing := connection.closing && !connection.broken
			channel.mutex.Unlock()

			if closing {
				_ = connection.socket.close()
				connection.cancel()
			}

			return
		}

		frame := connection.outbound[0]
		connection.writing = frame
		channel.mutex.Unlock()

		err := connection.socket.write(connection.context, frame.data)

		channel.mutex.Lock()
		connection.writing = nil
		connection.outbound = connection.outbound[1:]
		connection.outboundBytes -= len(frame.data)

		if err != nil {
			if frame.publish != nil {
				frame.publish.settle(newError(ErrDeliveryUnknown, "The socket failed while writing the publish, so it may or may not have been sent."))
			}

			// A write the socket refuses leaves the server's view of this
			// connection unknown, so the socket is replaced and reconnecting
			// restores every subscription (RESEND-01).
			queued := channel.receiveSocketFailure(connection)

			// Recovery took back what this socket never wrote; otherwise Close
			// detached it while flushing, so what it never wrote is cancelled.
			connection.failUnwritten(errClosedBeforeSent())
			connection.abandon()
			channel.mutex.Unlock()

			if queued {
				// The writer never runs listeners: one that publishes would wait
				// on this goroutine.
				go channel.dispatchEvents()
			}

			return
		}

		if frame.publish != nil {
			frame.publish.settle(nil)
		}

		if channel.connection == connection {
			channel.queue.drain()
		}

		channel.mutex.Unlock()
	}
} // end method writeLoop
