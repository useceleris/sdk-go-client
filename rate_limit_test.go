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
} // end function exhaustRateLimit

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
} // end function TestRateLimitPausesThenResendsSubscriptionsBeforePublishes

func TestRateLimitResendsACommandSentExactly2000msBeforeNotOneSent2001msBefore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		chat := segment(t, channel, "chat")
		publish(t, chat, "old", "old")
		synctest.Sleep(time.Millisecond)
		publish(t, chat, "edge", "edge")
		synctest.Sleep(2000 * time.Millisecond)
		socket.clearCommands()

		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(time.Second)
		assertCommands(t, socket, publishFrame("chat", "edge", "edge"))
	})
} // end function TestRateLimitResendsACommandSentExactly2000msBeforeNotOneSent2001msBefore

func TestRateLimitResendsAPublishByteForByteWithItsOriginalIDOnlyOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)

		if err := segment(t, channel, "chat").Publish(t.Context(), []byte("x")); err != nil {
			t.Fatal(err)
		}

		original := socket.commands()
		socket.clearCommands()

		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(time.Second)
		assertCommands(t, socket, original...)

		socket.clearCommands()
		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(31 * time.Second)
		assertCommands(t, socket)
	})
} // end function TestRateLimitResendsAPublishByteForByteWithItsOriginalIDOnlyOnce

func TestSubscriptionToggledWhilePausedSendsOneCommand(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		receiveAll(socket, rateLimitFrame())
		subscribe(t, segment(t, channel, "chat")).Cancel()
		subscribe(t, segment(t, channel, "chat"))
		synctest.Sleep(time.Second)
		assertCommands(t, socket, "@SUB\n$4\nchat\n")
	})
} // end function TestSubscriptionToggledWhilePausedSendsOneCommand

func TestRateLimitBacksOffThenStartsOverAfterAQuietWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		channel.client.random = func() float64 { return 1 }
		subscribe(t, segment(t, channel, "chat"))
		synctest.Wait()

		// 1 s plus the reconnect delay for the streak index, which is capped at
		// 30 s. The seventh limit is the first to reach the cap.
		pauses := []time.Duration{1500, 2000, 3000, 5000, 9000, 17000, 31000}

		for _, pause := range pauses {
			pause *= time.Millisecond
			socket.clearCommands()
			receiveAll(socket, rateLimitFrame())
			synctest.Sleep(pause - time.Millisecond)
			assertCommands(t, socket)
			synctest.Sleep(time.Millisecond)
			assertCommands(t, socket, "@SUB\n$4\nchat\n")
		}

		// Past the last pause and its suspect window, the streak starts over.
		synctest.Sleep(40 * time.Second)
		subscribe(t, segment(t, channel, "lobby"))
		synctest.Wait()
		socket.clearCommands()
		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(1499 * time.Millisecond)
		assertCommands(t, socket)
		synctest.Sleep(time.Millisecond)
		assertCommands(t, socket, "@SUB\n$5\nlobby\n")
	})
} // end function TestRateLimitBacksOffThenStartsOverAfterAQuietWindow

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
} // end function TestCancellingAQueuedPublishWithdrawsIt

// QUEUE-01: a publish waiting out a rate-limit pause was never handed to the
// socket, so it waits across the reconnect, which ends the pause.
func TestPublishWaitingOutAPauseIsSentAfterTheReconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		receiveAll(socket, rateLimitFrame())
		pending := publishAsync(t, segment(t, channel, "chat"), "m-1", "x")
		synctest.Wait()
		socket.drop()
		synctest.Wait()

		if err := <-pending; err != nil {
			t.Fatal(err)
		}

		assertCommands(t, socket)
		assertCommands(t, server.socket(-1), publishFrame("chat", "m-1", "x"))
	})
} // end function TestPublishWaitingOutAPauseIsSentAfterTheReconnect

// QUEUE-01: a publish handed to a socket is never sent again after a
// reconnect, not even the copy a rate limit queued for resending.
func TestRateLimitResendCopyIsNotSentAfterTheReconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		publish(t, segment(t, channel, "chat"), "m-1", "x")
		receiveAll(socket, rateLimitFrame())
		socket.drop()
		synctest.Wait()

		if channel.State() != StateConnected {
			t.Fatalf("state %s", channel.State())
		}

		synctest.Sleep(time.Minute)
		assertCommands(t, socket, publishFrame("chat", "m-1", "x"))
		assertCommands(t, server.socket(-1))
	})
} // end function TestRateLimitResendCopyIsNotSentAfterTheReconnect

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
} // end function TestSubscriptionChangeNeverOvertakesAnEarlierPublishToItsSegment

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
} // end function TestSubscriptionChangeWaitsOnlyForPublishesQueuedBeforeIt

// The first eight limits in a row each resend; from the ninth, recent
// publishes are dropped and recent subscriptions wait for the probe.
func TestDropsRecentPublishesAndKeepsSubscriptionsForTheProbeAfterEightLimitsInARow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		lobby := segment(t, channel, "lobby")
		subscribe(t, segment(t, channel, "chat"))
		synctest.Wait()

		for range 7 {
			socket.clearCommands()
			receiveAll(socket, rateLimitFrame())
			synctest.Sleep(time.Second)
			assertCommands(t, socket, "@SUB\n$4\nchat\n")
		}

		// The eighth still resends.
		publish(t, lobby, "m-8", "x")
		socket.clearCommands()
		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(time.Second)
		assertCommands(t, socket, "@SUB\n$4\nchat\n", publishFrame("lobby", "m-8", "x"))

		// The ninth does not.
		publish(t, lobby, "m-9", "x")
		socket.clearCommands()
		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(time.Minute - time.Millisecond)
		assertCommands(t, socket)
		synctest.Sleep(time.Millisecond)
		assertCommands(t, socket, "@SUB\n$4\nchat\n")
	})
} // end function TestDropsRecentPublishesAndKeepsSubscriptionsForTheProbeAfterEightLimitsInARow

func TestQuotaProbeResendsDroppedSubscriptionsOnADoublingScheduleUpToOneHour(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		subscribe(t, segment(t, channel, "chat"))
		synctest.Wait()
		exhaustRateLimit(socket)

		delays := []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 16 * time.Minute, 32 * time.Minute, time.Hour, time.Hour}

		for _, delay := range delays {
			socket.clearCommands()
			receiveAll(socket, rateLimitFrame())
			synctest.Sleep(delay - time.Millisecond)
			assertCommands(t, socket)
			synctest.Sleep(time.Millisecond)
			assertCommands(t, socket, "@SUB\n$4\nchat\n")
		}
	})
} // end function TestQuotaProbeResendsDroppedSubscriptionsOnADoublingScheduleUpToOneHour

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
} // end function TestResendingReturnsOnceCommandsGoAWindowWithoutALimit

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
} // end function TestQuotaProbingSurvivesAReconnect

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
} // end function TestSeveralFramesForOneBurstCountOnce

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
} // end function TestLateReportWhileProbingKeepsTheQuotaExhausted

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
} // end function TestLimitLongAfterAcceptedTrafficEndsProbing

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
} // end function TestSentPresenceQueryProvesTheQuotaReturned

func TestRateLimitResendsAtMostTheLast64Publishes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		lobby := segment(t, channel, "lobby")

		for index := 1; index <= 65; index++ {
			publish(t, lobby, "m-"+strconv.Itoa(index), strconv.Itoa(index))
		}

		socket.clearCommands()
		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(time.Second)

		var want []string

		for index := 2; index <= 65; index++ {
			want = append(want, publishFrame("lobby", "m-"+strconv.Itoa(index), strconv.Itoa(index)))
		}

		assertCommands(t, socket, want...)
	})
} // end function TestRateLimitResendsAtMostTheLast64Publishes

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
} // end function TestPresenceQueryWhilePausedIsBackpressure

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
} // end function TestRestoredSubscriptionsAreResentAfterARateLimit

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
} // end function TestRefusedSubscriptionWriteReplacesTheSocket
