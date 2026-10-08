package celeris

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestCloseIsIdempotentAndConcurrent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _ := newTestChannel(t)
		states := recordStates(channel)
		var group sync.WaitGroup

		for range 5 {
			group.Go(channel.Close)
		}

		group.Wait()
		channel.Close()

		if channel.State() != StateClosed {
			t.Fatalf("state %s", channel.State())
		}

		assertStates(t, states, StateClosing, StateClosed)
	})
} // end function TestCloseIsIdempotentAndConcurrent

func TestCloseShutsAConnectedChannelGracefully(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		states := recordStates(channel)
		channel.Close()
		assertStates(t, states, StateClosing, StateClosed)

		socket.mutex.Lock()
		graceful := socket.graceful
		socket.mutex.Unlock()

		if !graceful {
			t.Fatal("no closing handshake")
		}
	})
} // end function TestCloseShutsAConnectedChannelGracefully

func TestCloseFlushesHandedOffPublishes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		release := socket.holdWrites()
		written := publishAsync(t, channel.DefaultSegment(), "m-1", "x")
		synctest.Wait()

		go func() {
			synctest.Wait()
			release()
		}()

		channel.Close()

		if err := <-written; err != nil {
			t.Fatalf("handed-off publish failed: %v", err)
		}

		assertCommands(t, socket, publishFrame("default", "m-1", "x"))
	})
} // end function TestCloseFlushesHandedOffPublishes

func TestCloseIsBoundedByItsBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		socket.holdWrites()
		writing := publishAsync(t, channel.DefaultSegment(), "m-1", "x")
		synctest.Wait()
		waiting := publishAsync(t, channel.DefaultSegment(), "m-2", "y")
		synctest.Wait()
		start := time.Now()
		closed := make(chan struct{})

		go func() {
			channel.Close()
			close(closed)
		}()

		synctest.Sleep(5*time.Second - time.Millisecond)

		if channel.State() != StateClosing {
			t.Fatalf("state %s before the budget", channel.State())
		}

		synctest.Sleep(time.Millisecond)
		<-closed

		if elapsed := time.Since(start); elapsed != 5*time.Second || channel.State() != StateClosed {
			t.Fatalf("closed after %v in state %s", elapsed, channel.State())
		}

		assertCode(t, <-writing, ErrDeliveryUnknown)
		assertCode(t, <-waiting, ErrCancelled)
	})
} // end function TestCloseIsBoundedByItsBudget

func TestCloseAbortsAPendingAttemptAndDiscardsLateCredentials(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server := newTestChannel(t)
		release := make(chan struct{})
		channel.client.credentialProvider = func(context.Context, CredentialRequest) (Credentials, error) {
			<-release

			return testCredentials, nil
		}

		result := make(chan error, 1)

		go func() { result <- channel.Connect(t.Context()) }()

		synctest.Wait()
		channel.Close()
		assertCode(t, <-result, ErrCancelled)
		close(release)
		synctest.Wait()

		if channel.State() != StateClosed || server.socketCount() != 0 {
			t.Fatalf("state %s with %d sockets", channel.State(), server.socketCount())
		}
	})
} // end function TestCloseAbortsAPendingAttemptAndDiscardsLateCredentials

func TestStaleSocketEventsAfterCloseAreIgnored(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		channel.Close()
		states := recordStates(channel)
		errorsSeen := recordErrors(channel)

		select {
		case socket.incoming <- fakeFrame{binary: false, data: []byte("text")}:
		default:
		}

		socket.drop()
		synctest.Wait()

		if len(states.all()) != 0 || len(errorsSeen.all()) != 0 || channel.State() != StateClosed {
			t.Fatalf("states %v, errors %v", states.all(), errorsSeen.all())
		}
	})
} // end function TestStaleSocketEventsAfterCloseAreIgnored

func TestCloseWorksFromEveryNonTerminalState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		idle, _ := newTestChannel(t)
		idle.Close()

		failed, server := newTestChannel(t)
		server.set(func(server *fakeServer) { server.failProvider = true })
		assertCode(t, failed.Connect(t.Context()), ErrTransport)
		failed.Close()

		reconnecting, server, socket := connectTestChannel(t)
		server.set(func(server *fakeServer) { server.blockDials = true })
		socket.drop()
		synctest.Wait()
		reconnecting.Close()

		connected, _, _ := connectTestChannel(t)
		connected.Close()

		for _, channel := range []*Channel{idle, failed, reconnecting, connected} {
			if channel.State() != StateClosed {
				t.Fatalf("state %s", channel.State())
			}
		}
	})
} // end function TestCloseWorksFromEveryNonTerminalState

// A write failing while Close flushes the writer fails the publishes behind it
// as cancelled by Close, not as lost to a connection that will be restored.
func TestWriteFailingDuringCloseCancelsTheRest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		_ = socket.holdWrites()
		writing := publishAsync(t, channel.DefaultSegment(), "m-1", "x")
		synctest.Wait()
		waiting := publishAsync(t, channel.DefaultSegment(), "m-2", "y")
		synctest.Wait()

		go channel.Close()

		synctest.Wait()
		socket.drop()
		assertCode(t, <-writing, ErrDeliveryUnknown)
		assertCode(t, <-waiting, ErrCancelled)
	})
} // end function TestWriteFailingDuringCloseCancelsTheRest

// While Close flushes the writer, a new publish is refused and nothing is
// written for it. A publish still queued behind the writer is cancelled.
func TestClosingRefusesPublishesAndCancelsQueuedOnes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		lobby := channel.DefaultSegment()
		release := socket.holdWrites()
		var handedOff []<-chan error
		var want []string

		// 64 fill the writer, so the next one waits in the queue.
		for index := range 64 {
			handedOff = append(handedOff, publishAsync(t, lobby, "m-"+strconv.Itoa(index), "x"))
			want = append(want, publishFrame("default", "m-"+strconv.Itoa(index), "x"))
			synctest.Wait()
		}

		queued := publishAsync(t, lobby, "queued", "x")
		synctest.Wait()
		closed := make(chan struct{})

		go func() {
			channel.Close()
			close(closed)
		}()

		synctest.Wait()

		if channel.State() != StateClosing {
			t.Fatalf("state %s, want closing", channel.State())
		}

		err := <-queued
		assertCode(t, err, ErrCancelled)

		if want := "Channel closed before the publish was sent."; err.Error() != want {
			t.Fatalf("message %q, want %q", err.Error(), want)
		}

		err = lobby.PublishWithMessageID(t.Context(), []byte("x"), "late")
		assertCode(t, err, ErrNotConnected)

		if want := "Channel is not connected; it is closing."; err.Error() != want {
			t.Fatalf("message %q, want %q", err.Error(), want)
		}

		release()
		<-closed

		for _, result := range handedOff {
			if err := <-result; err != nil {
				t.Fatal(err)
			}
		}

		assertCommands(t, socket, want...)
	})
} // end function TestClosingRefusesPublishesAndCancelsQueuedOnes
