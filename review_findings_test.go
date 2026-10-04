package celeris

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// Regression tests for findings of the independent client review.

// A listener that calls Close while Close is delivering events returns at
// once instead of waiting on the call delivering to it.
func TestCloseFromAListenerDuringCloseReturns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, _ := connectTestChannel(t)
		channel.Events().OnStateChange(func(state ChannelState) {
			if state == StateClosing {
				channel.Close()
			}
		})

		done := make(chan struct{})

		go func() {
			channel.Close()
			close(done)
		}()

		synctest.Sleep(30 * time.Second)

		select {
		case <-done:
		default:
			t.Fatal("Close deadlocked")
		}
	})
}

// Frames handed to the writer before a limit, and written during the pause,
// are not commands sent since it: a second frame for the same burst is the
// same episode.
func TestBacklogWrittenDuringThePauseIsNotANewEpisode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		chat := segment(t, channel, "chat")

		for burst := range 8 {
			release := socket.holdWrites()
			done := publishAsync(t, chat, "m-"+strconv.Itoa(burst), "x")
			synctest.Wait()
			receiveAll(socket, rateLimitFrame())
			release()
			synctest.Wait()

			if err := <-done; err != nil {
				t.Fatal(err)
			}

			receiveAll(socket, rateLimitFrame())
			socket.clearCommands()
			synctest.Sleep(time.Second)

			if len(socket.commands()) == 0 {
				t.Fatalf("burst %d treated as a used-up quota", burst+1)
			}
		}
	})
}

func TestCallerDeadlineWithItsOwnCauseIsATimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server := newTestChannel(t)
		server.set(func(server *fakeServer) { server.blockProvider = true })
		ctx, cancel := context.WithTimeoutCause(t.Context(), time.Second, errors.New("caller's own cause"))
		defer cancel()

		assertCode(t, channel.Connect(ctx), ErrTimeout)
	})
}

func TestConnectWithADoneContextNeverCallsTheProvider(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server := newTestChannel(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		assertCode(t, channel.Connect(ctx), ErrCancelled)
		synctest.Wait()

		if len(server.credentialRequests()) != 0 {
			t.Fatal("the provider was called")
		}
	})
}

func TestPublishChecksTheConnectionBeforeTheMessageID(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _ := newTestChannel(t)
		assertCode(t, channel.DefaultSegment().PublishWithMessageID(t.Context(), []byte("x"), "bad\nid"), ErrNotConnected)
		assertCode(t, channel.DefaultSegment().PublishWithMessageID(t.Context(), []byte("x"), ""), ErrNotConnected)
		assertCode(t, channel.DefaultSegment().PublishWithMessageID(t.Context(), make([]byte, 3<<20), "m-1"), ErrNotConnected)
	})
}

// A recovery listener registered inside the connected listener receives the
// recovery event, as listeners registered before dispatch do in the reference.
func TestListenersRegisteredBeforeTheirTurnReceiveTheEvent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		recoveries := &recorder[RecoveryEvent]{}
		channel.Events().OnStateChange(func(state ChannelState) {
			if state == StateConnected {
				channel.Events().OnRecovery(recoveries.record)
			}
		})

		socket.drop()
		synctest.Wait()

		if len(recoveries.all()) != 1 {
			t.Fatalf("recoveries %v", recoveries.all())
		}
	})
}

// A listener's panic is reported before the next queued event.
func TestListenerPanicIsReportedBeforeTheNextEvent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		log := &recorder[string]{}
		channel.Events().OnStateChange(func(state ChannelState) {
			if state == StateConnected {
				panic("listener failure")
			}
		})
		channel.Events().OnRecovery(func(RecoveryEvent) { log.record("recovery") })
		channel.Events().OnError(func(error) { log.record("error") })

		socket.drop()
		synctest.Wait()

		if want := []string{"error", "recovery"}; !slices.Equal(log.all(), want) {
			t.Fatalf("log %v", log.all())
		}
	})
}

func TestTimeoutMessagesNameTheirBound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server := newTestChannel(t)
		server.set(func(server *fakeServer) { server.blockProvider = true })

		if err := channel.Connect(t.Context()); err == nil || err.Error() != "Connection attempt timed out after 15s." {
			t.Fatalf("got %v", err)
		}

		connected, _, _ := connectTestChannel(t)
		_, err := segment(t, connected, "chat").PresenceList(t.Context(), 1, 25)

		if err == nil || err.Error() != "Presence query timed out after 10s." {
			t.Fatalf("got %v", err)
		}
	})
}

// A publish taken back from the writer is never resent by a later rate
// limit.
func TestPublishTakenBackFromTheWriterIsNeverResent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		chat := segment(t, channel, "chat")
		release := socket.holdWrites()
		writing := publishAsync(t, chat, "m-1", "x")
		synctest.Wait()

		ctx, cancel := context.WithCancel(t.Context())
		withdrawn := make(chan error, 1)

		go func() { withdrawn <- chat.PublishWithMessageID(ctx, []byte("y"), "m-2") }()

		synctest.Wait()
		cancel()
		assertCode(t, <-withdrawn, ErrCancelled)
		release()

		if err := <-writing; err != nil {
			t.Fatal(err)
		}

		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(time.Second)

		if got := socket.commands(); !reflect.DeepEqual(got, []string{publishFrame("chat", "m-1", "x"), publishFrame("chat", "m-1", "x")}) {
			t.Fatalf("commands %q", got)
		}
	})
}

// A rate limit requeues a publish still waiting in the writer, so it has two
// copies. Cancelling it takes back both: a publish reported cancelled never
// goes out.
func TestCancelTakesBackEveryCopyARateLimitLeft(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		chat := segment(t, channel, "chat")
		release := socket.holdWrites()
		writing := publishAsync(t, chat, "m-0", "x")
		synctest.Wait()

		ctx, cancel := context.WithCancel(t.Context())
		waiting := make(chan error, 1)

		go func() { waiting <- chat.PublishWithMessageID(ctx, []byte("y"), "m-1") }()

		synctest.Wait()
		receiveAll(socket, rateLimitFrame())
		cancel()
		assertCode(t, <-waiting, ErrCancelled)
		release()

		if err := <-writing; err != nil {
			t.Fatal(err)
		}

		synctest.Sleep(time.Second)
		assertCommands(t, socket, publishFrame("chat", "m-0", "x"), publishFrame("chat", "m-0", "x"))
	})
}

// A publish whose context ends mid-write reports DeliveryUnknown, and such a
// publish is never resent, even by a rate limit that follows.
func TestDeliveryUnknownPublishIsNeverResent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		chat := segment(t, channel, "chat")
		release := socket.holdWrites()
		ctx, cancel := context.WithCancel(t.Context())
		writing := make(chan error, 1)

		go func() { writing <- chat.PublishWithMessageID(ctx, []byte("x"), "m-1") }()

		synctest.Wait()
		cancel()
		assertCode(t, <-writing, ErrDeliveryUnknown)
		release()
		synctest.Wait()
		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(time.Second)
		assertCommands(t, socket, publishFrame("chat", "m-1", "x"))
	})
}

// A panic inside routing, where the mutex is held, crashes the program with
// its own message instead of hanging the channel. The scenario runs in a
// child process, since the crash ends the process running it.
func TestRoutingPanicCrashesInsteadOfHanging(t *testing.T) {
	if os.Getenv("CELERIS_ROUTING_PANIC") == "1" {
		channel, _, socket := connectTestChannel(t)
		channel.mutex.Lock()
		channel.client.random = func() float64 { panic("injected routing failure") }
		channel.mutex.Unlock()

		// The rate limit's pause draws its jitter while routing.
		socket.receive(rateLimitFrame())
		time.Sleep(time.Minute)

		return
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRoutingPanicCrashesInsteadOfHanging$")
	command.Env = append(os.Environ(), "CELERIS_ROUTING_PANIC=1")
	output, err := command.CombinedOutput()

	if ctx.Err() != nil || err == nil || !strings.Contains(string(output), "injected routing failure") {
		t.Fatalf("child did not crash with the panic (deadline passed: %v, error: %v):\n%s", ctx.Err() != nil, err, output)
	}
}

// Taking a publish back from the writer frees room, so a publish waiting for
// it goes out instead of waiting for unrelated traffic.
func TestPublishTakenBackFreesRoomForTheNext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		large := make([]byte, 1100*1024)
		first, _ := encodePublish("default", "b", large)
		second, _ := encodePublish("default", "c", large)

		// The writer cannot take the first before it is taken back.
		channel.mutex.Lock()
		taken, _ := channel.queue.publish("default", first)
		waiting, _ := channel.queue.publish("default", second)
		channel.cancelPublish(taken, newError(ErrCancelled, "cancelled"))
		channel.mutex.Unlock()

		if err := <-waiting.result; err != nil {
			t.Fatal(err)
		}

		if got := socket.commands(); len(got) != 1 || got[0] != string(second) {
			t.Fatalf("%d commands", len(got))
		}
	})
}

// A listener ending the receive goroutine, as t.FailNow does, replaces the
// socket rather than leaving it unread.
func TestListenerEndingTheReceiveGoroutineReplacesTheSocket(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		seen := &recorder[string]{}
		channel.DefaultSegment().OnMessage(func(_ []byte, metadata MessageMetadata) {
			seen.record(metadata.MessageID)

			if metadata.MessageID == "id-1" {
				runtime.Goexit()
			}
		})

		receiveAll(socket, messageFrame("default", "id-1", "x"))
		receiveAll(server.socket(-1), messageFrame("default", "id-2", "x"))

		if got := seen.all(); !slices.Equal(got, []string{"id-1", "id-2"}) || server.socketCount() != 2 || channel.State() != StateConnected {
			t.Fatalf("seen %v, %d sockets, state %s", got, server.socketCount(), channel.State())
		}
	})
}

// The listeners after one that ends its goroutine still receive the event.
func TestListenersAfterOneEndingItsGoroutineStillReceive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _ := newTestChannel(t)
		after := &recorder[ChannelState]{}
		channel.Events().OnStateChange(func(state ChannelState) {
			if state == StateConnecting {
				runtime.Goexit()
			}
		})
		channel.Events().OnStateChange(after.record)

		channel.mutex.Lock()
		channel.queueStateChange(StateConnecting)
		channel.mutex.Unlock()

		go channel.dispatchEvents()

		synctest.Wait()

		if got := after.all(); !slices.Equal(got, []ChannelState{StateConnecting}) {
			t.Fatalf("after %v", got)
		}
	})
}

// reentrantContext calls back into the channel from Err, as a context may.
type reentrantContext struct {
	context.Context
	channel *Channel
}

func (ctx reentrantContext) Err() error {
	_ = ctx.channel.State()

	return ctx.Context.Err()
}

// A caller's context is never read while the channel's mutex is held.
func TestCallerContextsRunOutsideTheMutex(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		ctx := reentrantContext{Context: t.Context(), channel: channel}

		if err := channel.DefaultSegment().PublishWithMessageID(ctx, []byte("x"), "m-1"); err != nil {
			t.Fatal(err)
		}

		pending := queryAsync(ctx, segment(t, channel, "chat"), 1, 25)
		socket.receive(presenceResponseFrame(response("1")))

		if outcome := <-pending; outcome.err != nil {
			t.Fatal(outcome.err)
		}

		other, _ := newTestChannel(t)

		if err := other.Connect(ctx); err != nil {
			t.Fatal(err)
		}
	})
}
