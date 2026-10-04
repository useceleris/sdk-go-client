package celeris

import (
	"context"
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
}

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
}

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
}

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
}

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
}

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
}

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
}

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
}
