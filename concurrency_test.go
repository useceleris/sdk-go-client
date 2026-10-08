package celeris

import (
	"runtime"
	"slices"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
)

// Listeners never run under an SDK lock, so they may call back into the
// channel. Calls they make never wait for their own events: those follow once
// the listener returns.
func TestListenersMayCallBackIntoTheChannel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		chat := segment(t, channel, "chat")
		outcomes := &recorder[string]{}
		var removeSelf func()

		removeSelf = chat.OnMessage(func(_ []byte, metadata MessageMetadata) {
			if err := chat.PublishWithMessageID(t.Context(), []byte("echo"), "echo-"+metadata.MessageID); err != nil {
				outcomes.record("publish failed: " + err.Error())
			}

			subscription, err := chat.Subscribe()

			if err != nil {
				outcomes.record("subscribe failed: " + err.Error())
			} else {
				subscription.Cancel()
			}

			channel.Events().OnNotice(func(ServerNotice) {})()
			removeSelf()
			outcomes.record("done " + metadata.MessageID)
		})

		receiveAll(socket, messageFrame("chat", "id-1", "x"), messageFrame("chat", "id-2", "x"))

		if got := outcomes.all(); !slices.Equal(got, []string{"done id-1"}) {
			t.Fatalf("outcomes %v", got)
		}

		assertCommands(t, socket, publishFrame("chat", "echo-id-1", "echo"), "@SUB\n$4\nchat\n", "@UNSUB\n$4\nchat\n")
	})
} // end function TestListenersMayCallBackIntoTheChannel

func TestCloseFromInsideAListener(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		log := &recorder[string]{}
		channel.Events().OnStateChange(func(state ChannelState) { log.record(string(state)) })
		channel.Events().OnNotice(func(ServerNotice) {
			channel.Close()
			log.record("closed in listener")
		})

		segment(t, channel, "chat").OnMessage(func([]byte, MessageMetadata) { log.record("message") })

		socket.receive("*2\n@SERVER_MSG\n:1\n$0\n\n" + messageFrame("chat", "id-1", "x"))
		synctest.Wait()

		// The batch's later entry is never routed once the channel closed.
		if want := []string{"closed in listener", "closing", "closed"}; !slices.Equal(log.all(), want) {
			t.Fatalf("log %v", log.all())
		}
	})
} // end function TestCloseFromInsideAListener

func TestConnectFromInsideAFailedStateListener(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server := newTestChannel(t)
		server.set(func(server *fakeServer) { server.failDials = 1 })
		retried := make(chan error, 1)
		channel.Events().OnStateChange(func(state ChannelState) {
			if state == StateFailed {
				retried <- channel.Connect(t.Context())
			}
		})

		assertCode(t, channel.Connect(t.Context()), ErrTransport)

		if err := <-retried; err != nil {
			t.Fatal(err)
		}

		synctest.Wait()

		if channel.State() != StateConnected {
			t.Fatalf("state %s", channel.State())
		}
	})
} // end function TestConnectFromInsideAFailedStateListener

func TestEventsFollowTheOrderStateChanged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		log := &recorder[string]{}
		channel.Events().OnStateChange(func(state ChannelState) { log.record(string(state)) })
		channel.Events().OnError(func(error) { log.record("error") })
		server.set(func(server *fakeServer) { server.credentials = Credentials{} })
		socket.drop()
		synctest.Wait()

		// A terminal failure reports its error before the failed state.
		if want := []string{"reconnecting", "error", "failed"}; !slices.Equal(log.all(), want) {
			t.Fatalf("log %v", log.all())
		}
	})
} // end function TestEventsFollowTheOrderStateChanged

// A listener that ends its goroutine, as t.FailNow does, must not leave the
// channel marked as delivering.
func TestListenerEndingItsGoroutineLeavesDispatchUsable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _ := newTestChannel(t)
		seen := &recorder[ChannelState]{}
		channel.Events().OnStateChange(func(state ChannelState) {
			seen.record(state)

			if state == StateConnecting {
				runtime.Goexit()
			}
		})

		channel.mutex.Lock()
		channel.queueStateChange(StateConnecting)
		channel.queueStateChange(StateFailed)
		channel.mutex.Unlock()

		go channel.dispatchEvents()

		synctest.Wait()

		channel.mutex.Lock()
		draining := channel.draining
		channel.mutex.Unlock()

		if draining || !slices.Equal(seen.all(), []ChannelState{StateConnecting, StateFailed}) {
			t.Fatalf("draining %v, seen %v", draining, seen.all())
		}
	})
} // end function TestListenerEndingItsGoroutineLeavesDispatchUsable

// LIFE-04 and RES-03: concurrent use from many goroutines is safe, and every
// goroutine the channel started ends once it closes.
func TestConcurrentUseIsSafe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		var group sync.WaitGroup

		for worker := range 8 {
			group.Go(func() {
				chat := segment(t, channel, "chat-"+strconv.Itoa(worker%3))

				for index := range 20 {
					remove := chat.OnMessage(func([]byte, MessageMetadata) { _ = channel.State() })
					subscription, err := chat.Subscribe()

					if err == nil {
						defer subscription.Cancel()
					}

					_ = chat.PublishWithMessageID(t.Context(), []byte("x"), "m-"+strconv.Itoa(worker)+"-"+strconv.Itoa(index))
					remove()
				}
			})
		}

		group.Go(func() {
			for index := range 50 {
				socket.receive(messageFrame("chat-"+strconv.Itoa(index%3), "id-"+strconv.Itoa(index), "x"))
			}
		})

		group.Wait()
		synctest.Wait()

		if channel.State() != StateConnected {
			t.Fatalf("state %s", channel.State())
		}
	})
} // end function TestConcurrentUseIsSafe

func TestRepeatedConnectAndCloseUnderLoad(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for range 50 {
			channel, _, socket := connectTestChannel(t)
			subscribe(t, segment(t, channel, "chat"))
			segment(t, channel, "chat").OnMessage(func([]byte, MessageMetadata) {})

			go func() {
				for index := range 5 {
					select {
					case socket.incoming <- fakeFrame{binary: true, data: []byte(messageFrame("chat", "id-"+strconv.Itoa(index), "x"))}:
					case <-socket.closed:
						return
					}
				}
			}()

			channel.Close()

			if channel.State() != StateClosed || !socket.isClosed() {
				t.Fatalf("state %s, socket closed %v", channel.State(), socket.isClosed())
			}
		}
	})
} // end function TestRepeatedConnectAndCloseUnderLoad
