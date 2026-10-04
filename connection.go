package celeris

import (
	"context"
	"net"
	"net/http"
	"slices"
	"sync"

	"github.com/coder/websocket"
)

// socket is the one transport seam: the WebSocket a connection drives, so
// tests can substitute an in-memory peer.
type socket interface {
	// read returns the next message, and whether it was binary.
	read(ctx context.Context) (binary bool, data []byte, err error)

	write(ctx context.Context, data []byte) error

	// close performs the closing handshake. The caller bounds how long it may
	// take.
	close() error

	closeNow() error
}

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
}

// httpTransport is the package's own, so changes an application makes to
// http.DefaultTransport, such as skipping certificate verification, cannot
// weaken a connection (SEC-02). Setting its dialer also turns off automatic
// HTTP/2, which a WebSocket upgrade never uses.
//
// The dialer probes an idle socket, so a silently dead path, such as a
// dropped NAT mapping, fails it and starts recovery. The kernel sends and
// answers the probes, so they keep working while a listener holds the receive
// goroutine. WebSocket pings would not: the library reads pongs only while the
// channel reads, so a slow listener would look like a dead connection.
func httpTransport() *http.Transport {
	dialer := &net.Dialer{KeepAliveConfig: net.KeepAliveConfig{
		Enable:   true,
		Idle:     keepAliveIdle,
		Interval: keepAliveInterval,
		Count:    keepAliveCount,
	}}

	return &http.Transport{Proxy: http.ProxyFromEnvironment, DialContext: dialer.DialContext}
}

type webSocket struct {
	conn *websocket.Conn
}

func (socket webSocket) read(ctx context.Context) (bool, []byte, error) {
	messageType, data, err := socket.conn.Read(ctx)

	return messageType == websocket.MessageBinary, data, err
}

func (socket webSocket) write(ctx context.Context, data []byte) error {
	return socket.conn.Write(ctx, websocket.MessageBinary, data)
}

func (socket webSocket) close() error {
	return socket.conn.Close(websocket.StatusNormalClosure, "")
}

func (socket webSocket) closeNow() error {
	return socket.conn.CloseNow()
}

// outboundFrame is one command handed to the writer.
type outboundFrame struct {
	data []byte

	// Set for a publish, which settles once written.
	publish *queuedPublish
}

// connection is one socket and the goroutines that drive it: a writer that
// writes handed-off frames in order, and a receiver that routes what arrives.
// Its fields are guarded by the channel's mutex.
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
}

func newConnection(channel *Channel, socket socket) *connection {
	connectionContext, cancel := context.WithCancel(context.Background())

	return &connection{
		channel:    channel,
		socket:     socket,
		context:    connectionContext,
		cancel:     cancel,
		ready:      sync.NewCond(&channel.mutex),
		writerDone: make(chan struct{}),
	}
}

func (connection *connection) hasRoom(size int) bool {
	return len(connection.outbound) < maximumPendingCommands && connection.outboundBytes+size <= maximumBufferedBytes
}

func (connection *connection) handOff(frame *outboundFrame) {
	connection.outbound = append(connection.outbound, frame)
	connection.outboundBytes += len(frame.data)
	connection.ready.Signal()
}

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
}

// abandon stops the connection without a closing handshake. The caller holds
// the mutex and has detached the connection; publishes not yet written fail
// with err.
func (connection *connection) abandon(err error) {
	if connection.broken {
		return
	}

	connection.broken = true
	connection.failUnwritten(err)
	connection.ready.Broadcast()

	// Cancelling the read and write context first makes the library drop a
	// closing handshake in progress at once.
	go func() {
		connection.cancel()
		_ = connection.socket.closeNow()
	}()
}

// failUnwritten fails every publish handed to this connection that the socket
// never started writing.
func (connection *connection) failUnwritten(err error) {
	for _, frame := range connection.outbound {
		if frame != connection.writing && frame.publish != nil {
			frame.publish.settle(err)
		}
	}

	if connection.writing != nil {
		connection.outbound = []*outboundFrame{connection.writing}
		connection.outboundBytes = len(connection.writing.data)
	} else {
		connection.outbound = nil
		connection.outboundBytes = 0
	}
}

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
			lost := errConnectionLost()

			if connection.closing {
				lost = errClosedBeforeSent()
			}

			connection.abandon(lost)
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
}
