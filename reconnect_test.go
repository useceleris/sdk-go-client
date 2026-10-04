package celeris

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"testing/synctest"
	"time"
)

// expectAttemptAfter checks that the next reconnect attempt dials exactly
// delay after the previous one ended.
func expectAttemptAfter(t *testing.T, server *fakeServer, delay time.Duration) *fakeSocket {
	t.Helper()

	count := server.socketCount()

	if delay > 0 {
		synctest.Sleep(delay - time.Millisecond)

		if server.socketCount() != count {
			t.Fatalf("attempt before %v", delay)
		}

		synctest.Sleep(time.Millisecond)
	} else {
		synctest.Wait()
	}

	if server.socketCount() != count+1 {
		t.Fatalf("no attempt after %v", delay)
	}

	return server.socket(-1)
}

func TestReconnectDelaysAreJitteredAndBoundedThenFail(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		channel.client.random = func() float64 { return 0.5 }
		errorsSeen := recordErrors(channel)
		states := recordStates(channel)

		// Every attempt dials, then the handshake socket breaks before use, so
		// each fails as a transport error.
		server.set(func(server *fakeServer) { server.failDials = 10 })
		socket.drop()
		synctest.Wait()

		if channel.State() != StateReconnecting {
			t.Fatalf("state %s", channel.State())
		}

		delays := []time.Duration{250, 500, 1000, 2000, 4000, 8000, 15000, 15000, 15000, 15000}

		for _, delay := range delays {
			before := len(server.credentialRequests())
			synctest.Sleep(delay*time.Millisecond - time.Millisecond)

			if len(server.credentialRequests()) != before {
				t.Fatalf("attempt before %vms", delay)
			}

			synctest.Sleep(time.Millisecond)

			if len(server.credentialRequests()) != before+1 {
				t.Fatalf("no attempt after %vms", delay)
			}
		}

		if channel.State() != StateFailed {
			t.Fatalf("state %s", channel.State())
		}

		reported := errorsSeen.all()

		if len(reported) != 1 || !errors.Is(reported[0], ErrTransport) {
			t.Fatalf("errors %v", reported)
		}

		assertStates(t, states, StateReconnecting, StateFailed)

		reconnects := 0

		for _, request := range server.credentialRequests() {
			if request.Reconnect {
				reconnects++
			}
		}

		if reconnects != 10 {
			t.Fatalf("%d reconnect requests", reconnects)
		}
	})
}

func TestRetryBudgetResetsOnlyAfterSixtySecondsConnected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		channel.client.random = func() float64 { return 0.5 }
		server.set(func(server *fakeServer) { server.failDials = 3 })
		socket.drop()
		synctest.Wait()

		for _, delay := range []time.Duration{250, 500, 1000} {
			synctest.Sleep(delay * time.Millisecond)
		}

		socket = expectAttemptAfter(t, server, 2000*time.Millisecond)

		synctest.Sleep(time.Second)
		socket.drop()
		socket = expectAttemptAfter(t, server, 2000*time.Millisecond)

		synctest.Sleep(time.Minute)
		socket.drop()
		expectAttemptAfter(t, server, 250*time.Millisecond)

		if channel.State() != StateConnected {
			t.Fatalf("state %s", channel.State())
		}
	})
}

func TestRecoveryFollowsTheConnectedStateWithItsRetryIndex(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		log := &recorder[any]{}
		channel.Events().OnStateChange(func(state ChannelState) { log.record(state) })
		channel.Events().OnRecovery(func(event RecoveryEvent) { log.record(event) })
		server.set(func(server *fakeServer) { server.failDials = 2 })
		socket.drop()
		synctest.Wait()

		want := []any{StateReconnecting, StateConnected, RecoveryEvent{RetryIndex: 2, PossibleGaps: true, PossibleDuplicates: true}}

		if got := log.all(); !reflect.DeepEqual(got, want) {
			t.Fatalf("log %v", got)
		}
	})
}

func TestReconnectRequestsFreshCredentialsWithAGrowingLookback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		channel.client.random = func() float64 { return 1 }
		server.set(func(server *fakeServer) { server.failDials = 3 })
		synctest.Sleep(5 * time.Second)
		disconnectedAt := time.Now()
		socket.drop()
		synctest.Wait()
		synctest.Sleep(500 * time.Millisecond)
		synctest.Sleep(time.Second)
		synctest.Sleep(2 * time.Second)

		var reconnects []CredentialRequest

		for _, request := range server.credentialRequests() {
			if request.Reconnect {
				reconnects = append(reconnects, request)
			}
		}

		if len(reconnects) != 3 {
			t.Fatalf("%d reconnect requests", len(reconnects))
		}

		var lookbacks []time.Duration

		for _, request := range reconnects {
			if !request.DisconnectedAt.Equal(disconnectedAt) || request.ChannelReference != "room-1" {
				t.Fatalf("request %+v", request)
			}

			lookbacks = append(lookbacks, request.ReplayLookback)
		}

		// The outage so far, rounded up to whole milliseconds, plus five
		// seconds of overlap.
		want := []time.Duration{5500 * time.Millisecond, 6500 * time.Millisecond, 8500 * time.Millisecond}

		if !slices.Equal(lookbacks, want) {
			t.Fatalf("lookbacks %v, want %v", lookbacks, want)
		}

		if replayLookback(1500*time.Microsecond) != 5002*time.Millisecond || replayLookback(5_000_000_000*time.Millisecond) != replayLookbackCap {
			t.Fatal("lookback is not rounded up or not capped")
		}
	})
}

func TestDeterministicReconnectFailureIsTerminalAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		errorsSeen := recordErrors(channel)
		server.set(func(server *fakeServer) { server.credentials = Credentials{} })
		socket.drop()
		synctest.Wait()

		if reported := errorsSeen.all(); channel.State() != StateFailed || len(reported) != 1 || !errors.Is(reported[0], ErrConfiguration) {
			t.Fatalf("state %s, errors %v", channel.State(), reported)
		}
	})
}

func TestUndecodableMessageNeverStartsAReconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		errorsSeen := recordErrors(channel)
		socket.incoming <- fakeFrame{binary: false, data: []byte("text")}
		synctest.Wait()

		if channel.State() != StateConnected || len(errorsSeen.all()) != 1 || socket.isClosed() || server.socketCount() != 1 {
			t.Fatalf("state %s, errors %v", channel.State(), errorsSeen.all())
		}
	})
}

func TestCloseDuringAReconnectAttemptStopsRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		server.set(func(server *fakeServer) { server.blockProvider = true })
		socket.drop()
		synctest.Wait()
		states := recordStates(channel)

		channel.Close()
		assertStates(t, states, StateClosing, StateClosed)
		synctest.Wait()

		if channel.State() != StateClosed || server.socketCount() != 1 {
			t.Fatalf("state %s with %d sockets", channel.State(), server.socketCount())
		}
	})
}

// A close at the instant a retry timer fires must not let the closed channel
// reconnect.
func TestCloseRacingARetryTimerNeverReconnects(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		channel.client.random = func() float64 { return 1 }
		socket.drop()
		synctest.Wait()
		count := server.socketCount()

		// The retry is due in 500ms; close lands at the same instant.
		time.Sleep(500 * time.Millisecond)
		channel.Close()
		synctest.Wait()

		if channel.State() != StateClosed || server.socketCount() > count+1 {
			t.Fatalf("state %s", channel.State())
		}

		for index := count; index < server.socketCount(); index++ {
			if !server.socket(index).isClosed() {
				t.Fatal("a socket opened after close stayed open")
			}
		}
	})
}

func TestRestorationSendsCurrentIntentMessagesThenPresence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		subscribe(t, segment(t, channel, "beta"))
		subscribe(t, segment(t, channel, "alpha"))
		cancelledMessages := subscribe(t, segment(t, channel, "gone"))
		subscribePresence(t, channel.DefaultSegment())
		subscribePresence(t, segment(t, channel, "alpha"))
		cancelledPresence := subscribePresence(t, segment(t, channel, "brief"))
		publish(t, segment(t, channel, "beta"), "m-1", "x")

		server.set(func(server *fakeServer) { server.blockDials = true })
		socket.drop()
		synctest.Wait()
		cancelledMessages.Cancel()
		cancelledPresence.Cancel()
		server.set(func(server *fakeServer) { server.blockDials = false })
		synctest.Sleep(15 * time.Second)

		if channel.State() != StateConnected {
			t.Fatalf("state %s", channel.State())
		}

		// Interests only, messages before presence, registration order,
		// cancelled intent excluded, no publish resent.
		assertCommands(t, server.socket(-1), "@SUB\n$4\nbeta\n", "@SUB\n$5\nalpha\n", "@PRES_SUB\n$7\ndefault\n", "@PRES_SUB\n$5\nalpha\n")
	})
}

func TestRestorationPrecedesAnyPublishMadeOnConnected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		subscribe(t, segment(t, channel, "chat"))
		log := &recorder[string]{}
		channel.Events().OnStateChange(func(state ChannelState) {
			if state == StateConnected {
				publishAsync(t, segment(t, channel, "chat"), "m-1", "x")
			}

			log.record(string(state))
		})
		channel.Events().OnRecovery(func(RecoveryEvent) { log.record("recovery") })
		socket.drop()
		synctest.Wait()

		assertCommands(t, server.socket(-1), "@SUB\n$4\nchat\n", publishFrame("chat", "m-1", "x"))

		if want := []string{"reconnecting", "connected", "recovery"}; !slices.Equal(log.all(), want) {
			t.Fatalf("log %v", log.all())
		}
	})
}

// Frames that arrived with the handshake are routed only after the connected
// state and the recovery event.
func TestHandshakeFramesFollowConnectedAndRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, first := connectTestChannel(t)
		log := &recorder[string]{}
		channel.Events().OnStateChange(func(state ChannelState) { log.record(string(state)) })
		channel.Events().OnRecovery(func(RecoveryEvent) { log.record("recovery") })
		channel.Events().OnNotice(func(ServerNotice) { log.record("notice") })
		server.set(func(server *fakeServer) { server.blockDials = true })
		first.drop()
		synctest.Wait()

		// The next socket carries a greeting the moment it opens.
		original := channel.client.dial
		channel.client.dial = func(ctx context.Context, address string) (socket, error) {
			opened, err := original(ctx, address)

			if err == nil {
				opened.(*fakeSocket).receive("@SERVER_MSG\n:1\n$5\nhello\n")
			}

			return opened, err
		}

		server.set(func(server *fakeServer) { server.blockDials = false })
		synctest.Sleep(15 * time.Second)

		if want := []string{"reconnecting", "connected", "recovery", "notice"}; !slices.Equal(log.all(), want) {
			t.Fatalf("log %v", log.all())
		}
	})
}

func TestDefaultSegmentKeepsDeliveringWithoutRestoration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		subscribe(t, channel.DefaultSegment())
		subscribe(t, segment(t, channel, "chat"))
		delivered := recordMessageIDs(channel.DefaultSegment())
		socket.drop()
		synctest.Wait()
		restored := server.socket(-1)

		// The server rejoins "default" by itself; a named segment must be
		// rejoined explicitly.
		assertCommands(t, restored, "@SUB\n$4\nchat\n")
		receiveAll(restored, messageFrame("default", "id-1", "a"))

		if !slices.Equal(delivered.all(), []string{"id-1"}) {
			t.Fatalf("delivered %v", delivered.all())
		}
	})
}

func TestReplayedDuplicatesAreAbsorbedAcrossReconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		subscribe(t, segment(t, channel, "chat"))
		delivered := recordMessageIDs(segment(t, channel, "chat"))
		receiveAll(socket, messageFrame("chat", "id-1", "a"), messageFrame("chat", "id-2", "b"))
		socket.drop()
		synctest.Wait()
		receiveAll(server.socket(-1), messageFrame("chat", "id-1", "a"), messageFrame("chat", "id-2", "b"), messageFrame("chat", "id-3", "c"))

		if !slices.Equal(delivered.all(), []string{"id-1", "id-2", "id-3"}) {
			t.Fatalf("delivered %v", delivered.all())
		}
	})
}

func TestRequestIDsAreNeverReusedAcrossReconnects(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		chat := segment(t, channel, "chat")
		pending := queryAsync(t.Context(), chat, 1, 25)
		socket.receive(presenceResponseFrame(response("1")))
		<-pending
		socket.drop()
		synctest.Wait()

		restored := server.socket(-1)
		pending = queryAsync(t.Context(), chat, 1, 25)
		assertCommands(t, restored, "@PRES_LIST\n$4\nchat\n;1\n;25\n$1\n2\n")
		restored.receive(presenceResponseFrame(response("2")))

		if outcome := <-pending; outcome.err != nil {
			t.Fatal(outcome.err)
		}
	})
}

func TestSecondRecoveryRestoresIntentWithoutDuplicates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		subscribe(t, segment(t, channel, "chat"))
		subscribePresence(t, segment(t, channel, "chat"))
		socket.drop()
		synctest.Wait()
		first := server.socket(-1)
		first.drop()
		synctest.Wait()
		second := server.socket(-1)

		for _, restored := range []*fakeSocket{first, second} {
			assertCommands(t, restored, "@SUB\n$4\nchat\n", "@PRES_SUB\n$4\nchat\n")
		}

		if channel.State() != StateConnected {
			t.Fatalf("state %s", channel.State())
		}
	})
}
