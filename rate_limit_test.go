package celeris

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"testing/synctest"
	"time"
)

// Time in these tests is the bubble's fake clock: synctest.Sleep advances it,
// runs every timer that comes due, and waits for what they started.

// exhaustRateLimit reports eight limits in a row, each after the previous
// resend round, so the next limit is treated as a used-up quota.
func exhaustRateLimit(socket *fakeSocket) {
	for range 8 {
		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(time.Second)
	}
}

func TestRateLimitPausesThenResendsSubscriptionsBeforePublishes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		errorsSeen := recordErrors(channel)
		chat := segment(t, channel, "chat")
		lobby := segment(t, channel, "lobby")
		subscribe(t, chat)
		subscribePresence(t, chat)
		publish(t, lobby, "m-1", "a")
		socket.clearCommands()

		receiveAll(socket, rateLimitFrame())
		queued := publishAsync(t, lobby, "m-2", "b")
		synctest.Sleep(999 * time.Millisecond)

		reported := errorsSeen.all()

		if len(reported) != 1 {
			t.Fatalf("errors %v", reported)
		}

		if serverError, ok := errors.AsType[*ServerError](reported[0]); !ok || serverError.Type != RateLimitError {
			t.Fatalf("errors %v", reported)
		}

		assertCommands(t, socket)
		synctest.Sleep(time.Millisecond)

		if err := <-queued; err != nil {
			t.Fatal(err)
		}

		assertCommands(t, socket, "@SUB\n$4\nchat\n", "@PRES_SUB\n$4\nchat\n", publishFrame("lobby", "m-1", "a"), publishFrame("lobby", "m-2", "b"))
	})
}

func TestRateLimitResendsAPublishOnceAndOnlyWithinTheWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		chat := segment(t, channel, "chat")
		publish(t, chat, "old", "old")
		synctest.Sleep(2001 * time.Millisecond)
		publish(t, chat, "new", "new")
		socket.clearCommands()

		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(time.Second)
		assertCommands(t, socket, publishFrame("chat", "new", "new"))

		socket.clearCommands()
		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(30 * time.Second)
		assertCommands(t, socket)
	})
}

func TestSubscriptionToggledWhilePausedSendsOneCommand(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		receiveAll(socket, rateLimitFrame())
		subscribe(t, segment(t, channel, "chat")).Cancel()
		subscribe(t, segment(t, channel, "chat"))
		synctest.Sleep(time.Second)
		assertCommands(t, socket, "@SUB\n$4\nchat\n")
	})
}

func TestRateLimitBacksOffThenStartsOverAfterAQuietWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		channel.client.random = func() float64 { return 1 }
		subscribe(t, segment(t, channel, "chat"))
		synctest.Wait()

		for _, pause := range []time.Duration{1500 * time.Millisecond, 2000 * time.Millisecond} {
			socket.clearCommands()
			receiveAll(socket, rateLimitFrame())
			synctest.Sleep(pause - time.Millisecond)
			assertCommands(t, socket)
			synctest.Sleep(time.Millisecond)
			assertCommands(t, socket, "@SUB\n$4\nchat\n")
		}

		synctest.Sleep(10 * time.Second)
		subscribe(t, segment(t, channel, "lobby"))
		synctest.Wait()
		socket.clearCommands()
		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(1500 * time.Millisecond)
		assertCommands(t, socket, "@SUB\n$5\nlobby\n")
	})
}

func TestCancellingAQueuedPublishWithdrawsIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		receiveAll(socket, rateLimitFrame())
		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)

		go func() { result <- segment(t, channel, "chat").PublishWithMessageID(ctx, []byte("x"), "m-1") }()

		synctest.Wait()
		cancel()
		err := <-result
		assertCode(t, err, ErrCancelled)

		if want := "Publish cancelled by its context before it was sent."; err.Error() != want {
			t.Fatalf("message %q", err.Error())
		}

		synctest.Sleep(time.Second)
		assertCommands(t, socket)
	})
}

func TestQueuedPublishesFailWhenTheConnectionDrops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		receiveAll(socket, rateLimitFrame())
		pending := publishAsync(t, segment(t, channel, "chat"), "m-1", "x")
		synctest.Wait()
		server.set(func(server *fakeServer) { server.blockDials = true })
		socket.drop()

		err := <-pending
		assertCode(t, err, ErrNotConnected)

		if want := "Connection lost before the publish was sent; publish again once the channel reconnects."; err.Error() != want {
			t.Fatalf("message %q", err.Error())
		}

		if channel.State() != StateReconnecting {
			t.Fatalf("state %s", channel.State())
		}
	})
}

func TestSubscriptionChangeNeverOvertakesAnEarlierPublishToItsSegment(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		receiveAll(socket, rateLimitFrame())
		chat := segment(t, channel, "chat")
		subscription := subscribe(t, chat)
		published := publishAsync(t, chat, "m-1", "x")
		synctest.Wait()
		subscription.Cancel()
		synctest.Sleep(time.Second)

		if err := <-published; err != nil {
			t.Fatal(err)
		}

		synctest.Wait()

		// Publishing joins the segment, so the UNSUB has to follow it.
		assertCommands(t, socket, publishFrame("chat", "m-1", "x"), "@UNSUB\n$4\nchat\n")
	})
}

func TestSubscriptionChangeWaitsOnlyForPublishesQueuedBeforeIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		receiveAll(socket, rateLimitFrame())
		chat := segment(t, channel, "chat")
		first := publishAsync(t, chat, "m-1", "1")
		synctest.Wait()
		subscribe(t, chat)
		second := publishAsync(t, chat, "m-2", "2")
		synctest.Wait()
		synctest.Sleep(time.Second)

		for _, result := range []<-chan error{first, second} {
			if err := <-result; err != nil {
				t.Fatal(err)
			}
		}

		assertCommands(t, socket, publishFrame("chat", "m-1", "1"), "@SUB\n$4\nchat\n", publishFrame("chat", "m-2", "2"))
	})
}

func TestQuotaProbeResendsDroppedSubscriptionsOnADoublingSchedule(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		subscribe(t, segment(t, channel, "chat"))
		synctest.Wait()
		exhaustRateLimit(socket)

		for _, delay := range []time.Duration{time.Minute, 2 * time.Minute} {
			socket.clearCommands()
			receiveAll(socket, rateLimitFrame())
			synctest.Sleep(delay - time.Millisecond)
			assertCommands(t, socket)
			synctest.Sleep(time.Millisecond)
			assertCommands(t, socket, "@SUB\n$4\nchat\n")
		}
	})
}

func TestResendingReturnsOnceCommandsGoAWindowWithoutALimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		subscribe(t, segment(t, channel, "chat"))
		synctest.Wait()
		exhaustRateLimit(socket)
		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(time.Minute)

		synctest.Sleep(3 * time.Second)
		subscribe(t, segment(t, channel, "lobby"))
		synctest.Wait()
		socket.clearCommands()
		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(time.Second)
		assertCommands(t, socket, "@SUB\n$5\nlobby\n")
	})
}

func TestQuotaProbingSurvivesAReconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		subscribe(t, segment(t, channel, "chat"))
		synctest.Wait()
		exhaustRateLimit(socket)
		receiveAll(socket, rateLimitFrame())

		socket.drop()
		synctest.Wait()
		restored := server.socket(-1)
		assertCommands(t, restored, "@SUB\n$4\nchat\n")

		restored.clearCommands()
		receiveAll(restored, rateLimitFrame())
		synctest.Sleep(2*time.Minute - time.Millisecond)
		assertCommands(t, restored)
		synctest.Sleep(time.Millisecond)
		assertCommands(t, restored, "@SUB\n$4\nchat\n")
	})
}

func TestSeveralFramesForOneBurstCountOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		subscribe(t, segment(t, channel, "chat"))
		synctest.Wait()

		// Eight episodes, each reported through two limit frames: still eight
		// resend rounds, not a give-up at four.
		for range 8 {
			socket.clearCommands()
			receiveAll(socket, rateLimitFrame(), rateLimitFrame())
			synctest.Sleep(time.Second)
			assertCommands(t, socket, "@SUB\n$4\nchat\n")
		}

		socket.clearCommands()
		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(time.Minute - time.Millisecond)
		assertCommands(t, socket)
		synctest.Sleep(time.Millisecond)
		assertCommands(t, socket, "@SUB\n$4\nchat\n")
	})
}

func TestLateReportWhileProbingKeepsTheQuotaExhausted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		subscribe(t, segment(t, channel, "chat"))
		synctest.Wait()
		exhaustRateLimit(socket)
		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(time.Minute)

		// The dropped probe's report lands past the suspect window but far
		// inside the confirmation span: probing continues, doubled.
		synctest.Sleep(2500 * time.Millisecond)
		socket.clearCommands()
		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(2*time.Minute - time.Millisecond)
		assertCommands(t, socket)
		synctest.Sleep(time.Millisecond)
		assertCommands(t, socket, "@SUB\n$4\nchat\n")
	})
}

func TestLimitLongAfterAcceptedTrafficEndsProbing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		subscribe(t, segment(t, channel, "chat"))
		synctest.Wait()
		exhaustRateLimit(socket)
		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(time.Minute)

		// The probe's frames were accepted; a limit far later is a new burst,
		// handled with normal resend rounds again.
		synctest.Sleep(40 * time.Second)
		receiveAll(socket, rateLimitFrame())
		subscribe(t, segment(t, channel, "lobby"))
		socket.clearCommands()
		synctest.Sleep(time.Second)
		assertCommands(t, socket, "@SUB\n$5\nlobby\n")

		receiveAll(socket, rateLimitFrame())
		socket.clearCommands()
		synctest.Sleep(time.Second)
		assertCommands(t, socket, "@SUB\n$5\nlobby\n")
	})
}

func TestSentPresenceQueryProvesTheQuotaReturned(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		chat := segment(t, channel, "chat")
		subscribe(t, chat)
		synctest.Wait()
		exhaustRateLimit(socket)
		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(time.Second)

		go func() { _, _ = chat.PresenceList(t.Context(), 1, 25) }()

		synctest.Wait()
		synctest.Sleep(2500 * time.Millisecond)
		socket.clearCommands()
		subscribe(t, segment(t, channel, "lobby"))
		synctest.Wait()
		assertCommands(t, socket, "@SUB\n$5\nlobby\n", "@SUB\n$4\nchat\n")
	})
}

func TestRateLimitResendsAtMostTheLast64Publishes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		lobby := segment(t, channel, "lobby")

		for index := range 70 {
			publish(t, lobby, "m-"+strconv.Itoa(index), "x")
		}

		socket.clearCommands()
		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(time.Second)

		if commands := socket.commands(); len(commands) != 64 || commands[0] != publishFrame("lobby", "m-6", "x") {
			t.Fatalf("%d commands, first %q", len(commands), commands[0])
		}
	})
}

func TestPresenceQueryWhilePausedIsBackpressure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		receiveAll(socket, rateLimitFrame())

		_, err := segment(t, channel, "chat").PresenceList(t.Context(), 1, 25)
		assertCode(t, err, ErrBackpressure)

		if want := "Sending is paused after a rate limit; try again in a moment."; err.Error() != want {
			t.Fatalf("message %q", err.Error())
		}
	})
}

func TestRestoredSubscriptionsAreResentAfterARateLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		subscribe(t, segment(t, channel, "chat"))
		socket.drop()
		synctest.Wait()
		restored := server.socket(-1)
		restored.clearCommands()

		receiveAll(restored, rateLimitFrame())
		synctest.Sleep(time.Second)
		assertCommands(t, restored, "@SUB\n$4\nchat\n")

		if channel.State() != StateConnected {
			t.Fatalf("state %s", channel.State())
		}
	})
}

func TestRefusedSubscriptionWriteReplacesTheSocket(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		errorsSeen := recordErrors(channel)
		states := recordStates(channel)
		socket.setFailWrites(true)

		subscribe(t, segment(t, channel, "chat"))
		synctest.Wait()

		if !socket.isClosed() || channel.State() != StateConnected || len(errorsSeen.all()) != 0 {
			t.Fatalf("closed %v, state %s, errors %v", socket.isClosed(), channel.State(), errorsSeen.all())
		}

		assertStates(t, states, StateReconnecting, StateConnected)
		assertCommands(t, server.socket(-1), "@SUB\n$4\nchat\n")
	})
}
