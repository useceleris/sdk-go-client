package celeris

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"
	"testing/synctest"
	"time"
)

// QUEUE-01: publishes the SDK never handed to a socket wait in the queue
// across a reconnect, and go out after the restored subscriptions.

// dropUnreachable drops the connection while every dial waits for its
// deadline, leaving the channel reconnecting.
func dropUnreachable(t *testing.T, channel *Channel, server *fakeServer, socket *fakeSocket) {
	t.Helper()

	server.set(func(server *fakeServer) { server.blockDials = true })
	socket.drop()
	synctest.Wait()

	if channel.State() != StateReconnecting {
		t.Fatalf("state %s", channel.State())
	}
} // end function dropUnreachable

// reconnectNow lets dials through: the waiting attempt times out after 15 s
// and the next one, with jitter fixed at zero, connects at once. It returns
// the new socket.
func reconnectNow(t *testing.T, channel *Channel, server *fakeServer) *fakeSocket {
	t.Helper()

	server.set(func(server *fakeServer) { server.blockDials = false })
	synctest.Sleep(15 * time.Second)

	if channel.State() != StateConnected {
		t.Fatalf("state %s", channel.State())
	}

	return server.socket(-1)
} // end function reconnectNow

func assertPending(t *testing.T, result <-chan error) {
	t.Helper()

	select {
	case err := <-result:
		t.Fatalf("publish ended early: %v", err)
	default:
	}
} // end function assertPending

func assertSent(t *testing.T, results ...<-chan error) {
	t.Helper()

	for _, result := range results {
		if err := <-result; err != nil {
			t.Fatal(err)
		}
	}
} // end function assertSent

func TestPublishWhileReconnectingIsSentAfterTheReconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		dropUnreachable(t, channel, server, socket)
		pending := publishAsync(t, segment(t, channel, "chat"), "m-1", "x")
		synctest.Sleep(14 * time.Second)
		assertPending(t, pending)

		next := reconnectNow(t, channel, server)
		assertSent(t, pending)
		assertCommands(t, socket)
		assertCommands(t, next, publishFrame("chat", "m-1", "x"))
	})
} // end function TestPublishWhileReconnectingIsSentAfterTheReconnect

// The publishes the writer never started go back to the front of the queue,
// so they and those waiting for room behind them are sent after the
// reconnect, in call order. Only the one being written may have been sent: it
// is DeliveryUnknown and never sent again.
func TestUnwrittenPublishesAreSentAfterTheReconnectInOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		lobby := channel.DefaultSegment()
		socket.holdWrites()

		var results []<-chan error

		// 64 fill the writer, and two more wait for room.
		for index := range 66 {
			results = append(results, publishAsync(t, lobby, "m-"+strconv.Itoa(index), "x"))
			synctest.Wait()
		}

		dropUnreachable(t, channel, server, socket)

		// The socket was writing the first one when it dropped.
		assertCode(t, <-results[0], ErrDeliveryUnknown)

		for _, result := range results[1:] {
			assertPending(t, result)
		}

		next := reconnectNow(t, channel, server)
		assertSent(t, results[1:]...)

		var want []string

		for index := 1; index < 66; index++ {
			want = append(want, publishFrame("default", "m-"+strconv.Itoa(index), "x"))
		}

		assertCommands(t, next, want...)
	})
} // end function TestUnwrittenPublishesAreSentAfterTheReconnectInOrder

// Restoration goes first, even ahead of a publish to a restored segment that
// was queued before the drop; the publishes follow in call order.
func TestRestorationPrecedesPublishesQueuedAcrossTheReconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		alpha := segment(t, channel, "alpha")
		subscribe(t, alpha)
		subscribePresence(t, alpha)

		// The rate-limit pause holds the first publish in the queue.
		receiveAll(socket, rateLimitFrame())
		early := publishAsync(t, alpha, "q-1", "x")
		synctest.Wait()
		dropUnreachable(t, channel, server, socket)

		late := publishAsync(t, segment(t, channel, "beta"), "q-2", "x")
		synctest.Wait()

		later := publishAsync(t, alpha, "q-3", "x")
		synctest.Wait()

		next := reconnectNow(t, channel, server)
		assertSent(t, early, late, later)
		assertCommands(t, next,
			"@SUB\n$5\nalpha\n",
			"@PRES_SUB\n$5\nalpha\n",
			publishFrame("alpha", "q-1", "x"),
			publishFrame("beta", "q-2", "x"),
			publishFrame("alpha", "q-3", "x"),
		)
	})
} // end function TestRestorationPrecedesPublishesQueuedAcrossTheReconnect

// A subscription cancelled while reconnecting still follows a publish to its
// segment queued before it, so the segment ends up left as asked. A publish
// never subscribes to presence, so a cancelled presence subscription needs no
// command on the new socket.
func TestCancellingBehindAQueuedPublishLeavesTheSegmentAfterIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		gamma := segment(t, channel, "gamma")
		subscription := subscribe(t, gamma)
		presence := subscribePresence(t, gamma)
		dropUnreachable(t, channel, server, socket)

		queued := publishAsync(t, gamma, "g-1", "x")
		synctest.Wait()
		subscription.Cancel()
		presence.Cancel()

		next := reconnectNow(t, channel, server)
		assertSent(t, queued)
		assertCommands(t, next, publishFrame("gamma", "g-1", "x"), "@UNSUB\n$5\ngamma\n")
	})
} // end function TestCancellingBehindAQueuedPublishLeavesTheSegmentAfterIt

func TestPublishWhileReconnectingIsRefusedWhenTheQueueIsFull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectClientChannel(t, ClientOptions{PublishQueueSize: 1})
		lobby := channel.DefaultSegment()
		dropUnreachable(t, channel, server, socket)

		waiting := publishAsync(t, lobby, "m-1", "x")
		synctest.Wait()

		err := lobby.PublishWithMessageID(t.Context(), []byte("x"), "m-2")
		assertCode(t, err, ErrBackpressure)

		if want := "The publish queue is full (size 1). Retry once some publishes have gone out."; err.Error() != want {
			t.Fatalf("message %q", err.Error())
		}

		next := reconnectNow(t, channel, server)
		assertSent(t, waiting)
		assertCommands(t, next, publishFrame("default", "m-1", "x"))
	})
} // end function TestPublishWhileReconnectingIsRefusedWhenTheQueueIsFull

func TestFailedReconnectAttemptKeepsTheQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		channel.client.random = func() float64 { return 0.5 }
		server.set(func(server *fakeServer) { server.failDials = 1 })
		socket.drop()
		synctest.Wait()

		pending := publishAsync(t, channel.DefaultSegment(), "m-1", "x")
		synctest.Sleep(250 * time.Millisecond)

		// The connect, then the failed attempt.
		if got := len(server.credentialRequests()); got != 2 || channel.State() != StateReconnecting {
			t.Fatalf("%d credential requests, state %s", got, channel.State())
		}

		assertPending(t, pending)
		synctest.Sleep(500 * time.Millisecond)

		if channel.State() != StateConnected {
			t.Fatalf("state %s", channel.State())
		}

		assertSent(t, pending)
		assertCommands(t, server.socket(-1), publishFrame("default", "m-1", "x"))
	})
} // end function TestFailedReconnectAttemptKeepsTheQueue

// Recovery that stops in failed refuses each queued publish with the error
// OnError reports, and an explicit connect starts with nothing queued.
func TestQueuedPublishesFailWithTheTerminalError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectClientChannel(t, ClientOptions{MaximumReconnectAttempts: 1})
		channel.client.random = func() float64 { return 0.5 }
		lobby := channel.DefaultSegment()
		errorsSeen := recordErrors(channel)
		server.set(func(server *fakeServer) { server.failDials = 1000 })
		socket.drop()
		synctest.Wait()

		first := publishAsync(t, lobby, "m-1", "x")
		synctest.Wait()

		second := publishAsync(t, lobby, "m-2", "x")
		synctest.Sleep(250 * time.Millisecond)

		if channel.State() != StateFailed {
			t.Fatalf("state %s", channel.State())
		}

		reported := errorsSeen.all()

		if len(reported) != 1 {
			t.Fatalf("errors %v", reported)
		}

		assertCode(t, reported[0], ErrTransport)

		for _, result := range []<-chan error{first, second} {
			// The very error OnError reported.
			if err := <-result; !errors.Is(err, reported[0]) {
				t.Fatalf("got %v, want the reported %v", err, reported[0])
			}
		}

		server.set(func(server *fakeServer) { server.failDials = 0 })

		if err := channel.Connect(t.Context()); err != nil {
			t.Fatal(err)
		}

		synctest.Wait()
		assertCommands(t, server.socket(-1))
	})
} // end function TestQueuedPublishesFailWithTheTerminalError

func TestCloseWhileReconnectingCancelsQueuedPublishes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		dropUnreachable(t, channel, server, socket)
		pending := publishAsync(t, channel.DefaultSegment(), "m-1", "x")
		synctest.Wait()
		channel.Close()

		err := <-pending
		assertCode(t, err, ErrCancelled)

		if want := "Channel closed before the publish was sent."; err.Error() != want {
			t.Fatalf("message %q", err.Error())
		}
	})
} // end function TestCloseWhileReconnectingCancelsQueuedPublishes

func TestCancellingAPublishQueuedWhileReconnectingWithdrawsIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		dropUnreachable(t, channel, server, socket)

		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)

		go func() { result <- channel.DefaultSegment().PublishWithMessageID(ctx, []byte("x"), "m-1") }()

		synctest.Wait()
		cancel()

		err := <-result
		assertCode(t, err, ErrCancelled)

		if want := "Publish cancelled by its context before it was sent."; err.Error() != want {
			t.Fatalf("message %q", err.Error())
		}

		next := reconnectNow(t, channel, server)
		assertCommands(t, next)
	})
} // end function TestCancellingAPublishQueuedWhileReconnectingWithdrawsIt

// Recovery takes back each unwritten publish once, in order; one the socket
// is writing, or one already written whose resend copy waits, keeps its
// outcome, since only a rate limit resends.
func TestTakingBackUnwrittenPublishesSkipsWrittenAndDuplicateCopies(t *testing.T) {
	inFlight := &queuedPublish{segmentID: "in-flight"}
	written := &queuedPublish{segmentID: "written", settled: true}
	first := &queuedPublish{segmentID: "first"}
	second := &queuedPublish{segmentID: "second"}
	writing := &outboundFrame{data: []byte("in-flight"), publish: inFlight}
	connection := &connection{writing: writing}
	connection.outbound = []*outboundFrame{
		writing,
		{data: []byte("first"), publish: first},
		{data: []byte("@SUB\n")},
		{data: []byte("written"), publish: written},
		{data: []byte("in-flight"), publish: inFlight},
		{data: []byte("second"), publish: second},
		{data: []byte("first"), publish: first},
	}

	if taken := connection.takeUnwritten(); !slices.Equal(taken, []*queuedPublish{first, second}) {
		t.Fatalf("took %d publishes, want first and second", len(taken))
	}

	if len(connection.outbound) != 1 || connection.outbound[0] != writing || connection.outboundBytes != len(writing.data) {
		t.Fatalf("writer kept %d frames, %d bytes", len(connection.outbound), connection.outboundBytes)
	}
} // end function TestTakingBackUnwrittenPublishesSkipsWrittenAndDuplicateCopies
