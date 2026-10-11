package celeris

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// Most tests hold the writer on a first publish, so the commands queued
// behind it wait and go out packed once it is released (BATCH-01).

func assertFrames(t *testing.T, socket *fakeSocket, want ...string) {
	t.Helper()

	if got := socket.frames(); !slices.Equal(got, want) {
		t.Fatalf("frames %q, want %q", got, want)
	}
} // end function assertFrames

func awaitAll(t *testing.T, results ...<-chan error) {
	t.Helper()

	for _, result := range results {
		if err := <-result; err != nil {
			t.Fatal(err)
		}
	}
} // end function awaitAll

func TestCommandsWaitingForTheWriterGoOutAsOneArrayInOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		chat := segment(t, channel, "chat")
		release := socket.holdWrites()
		first := publishAsync(t, chat, "first", "x")
		synctest.Wait()

		subscribe(t, chat)
		second := publishAsync(t, chat, "id-1", "m1")
		synctest.Wait()
		third := publishAsync(t, chat, "id-2", "m2")
		synctest.Wait()

		release()
		awaitAll(t, first, second, third)
		assertFrames(t, socket,
			publishFrame("chat", "first", "x"),
			"*3\n@SUB\n$4\nchat\n"+publishFrame("chat", "id-1", "m1")+publishFrame("chat", "id-2", "m2"),
		)
	})
} // end function TestCommandsWaitingForTheWriterGoOutAsOneArrayInOrder

func TestTheWriterPacksAtMost16Commands(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		chat := segment(t, channel, "chat")
		release := socket.holdWrites()
		results := []<-chan error{publishAsync(t, chat, "first", "x")}
		synctest.Wait()

		want := []string{publishFrame("chat", "first", "x")}

		for index := range 17 {
			messageID := "m-" + string(rune('a'+index))
			results = append(results, publishAsync(t, chat, messageID, "x"))
			synctest.Wait()
			want = append(want, publishFrame("chat", messageID, "x"))
		}

		release()
		awaitAll(t, results...)

		frames := socket.frames()

		if len(frames) != 3 || !strings.HasPrefix(frames[1], "*16\n@PUB") || frames[2] != want[17] {
			t.Fatalf("frames %q", frames)
		}

		assertCommands(t, socket, want...)
	})
} // end function TestTheWriterPacksAtMost16Commands

func TestTheWriterPacksUpTo64KiBWithTheArrayHeader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		chat := segment(t, channel, "chat")

		// A publish here is 26 bytes of framing plus its payload, so "*2\n"
		// and payloads of 32 740 and 32 741 bytes make exactly 65 536.
		packs := func(firstPayload, secondPayload int) []string {
			socket.clearCommands()
			release := socket.holdWrites()
			held := publishAsync(t, chat, "h", "x")
			synctest.Wait()
			first := publishAsync(t, chat, "a", strings.Repeat("x", firstPayload))
			synctest.Wait()
			second := publishAsync(t, chat, "b", strings.Repeat("x", secondPayload))
			synctest.Wait()

			release()
			awaitAll(t, held, first, second)

			return socket.frames()[1:]
		}

		if frames := packs(32_740, 32_741); len(frames) != 1 || len(frames[0]) != 65_536 || !strings.HasPrefix(frames[0], "*2\n") {
			t.Fatalf("%d frames", len(frames))
		}

		if frames := packs(32_740, 32_742); len(frames) != 2 || !strings.HasPrefix(frames[0], "@PUB") || !strings.HasPrefix(frames[1], "@PUB") {
			t.Fatalf("%d frames", len(frames))
		}
	})
} // end function TestTheWriterPacksUpTo64KiBWithTheArrayHeader

func TestACommandLargerThan64KiBGoesAlone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		chat := segment(t, channel, "chat")
		large := strings.Repeat("x", 70*1024)
		release := socket.holdWrites()
		held := publishAsync(t, chat, "h", "x")
		synctest.Wait()

		var results []<-chan error

		for _, publish := range [][2]string{{"a", "x"}, {"b", large}, {"c", "x"}} {
			results = append(results, publishAsync(t, chat, publish[0], publish[1]))
			synctest.Wait()
		}

		release()
		awaitAll(t, append(results, held)...)
		assertFrames(t, socket,
			publishFrame("chat", "h", "x"),
			publishFrame("chat", "a", "x"),
			publishFrame("chat", "b", large),
			publishFrame("chat", "c", "x"),
		)
	})
} // end function TestACommandLargerThan64KiBGoesAlone

func TestAFailedWriteFailsEveryPublishInItsBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		chat := segment(t, channel, "chat")
		release := socket.holdWrites()
		held := publishAsync(t, chat, "h", "x")
		synctest.Wait()

		var results []<-chan error

		for _, messageID := range []string{"a", "b", "c"} {
			results = append(results, publishAsync(t, chat, messageID, "x"))
			synctest.Wait()
		}

		socket.refuseWrites()
		release()
		awaitAll(t, held)

		for _, result := range results {
			assertCode(t, <-result, ErrDeliveryUnknown)
		}

		assertFrames(t, socket, publishFrame("chat", "h", "x"))
	})
} // end function TestAFailedWriteFailsEveryPublishInItsBatch

func TestCancellingAWaitingPublishTakesItOutOfTheNextBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		chat := segment(t, channel, "chat")
		release := socket.holdWrites()
		held := publishAsync(t, chat, "h", "x")
		synctest.Wait()

		first := publishAsync(t, chat, "a", "x")
		synctest.Wait()

		ctx, cancel := context.WithCancel(t.Context())
		cancelled := make(chan error, 1)

		go func() { cancelled <- chat.PublishWithMessageID(ctx, []byte("x"), "b") }()

		synctest.Wait()
		last := publishAsync(t, chat, "c", "x")
		synctest.Wait()

		cancel()
		assertCode(t, <-cancelled, ErrCancelled)

		release()
		awaitAll(t, held, first, last)
		assertFrames(t, socket,
			publishFrame("chat", "h", "x"),
			"*2\n"+publishFrame("chat", "a", "x")+publishFrame("chat", "c", "x"),
		)
	})
} // end function TestCancellingAWaitingPublishTakesItOutOfTheNextBatch

// The writer takes all three at once, since they are handed over under one
// lock; cancelling one while the batch is written leaves its outcome unknown.
func TestCancellingAPublishInTheBatchBeingWrittenIsDeliveryUnknown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		release := socket.holdWrites()
		var publishes []*queuedPublish

		channel.mutex.Lock()

		for _, messageID := range []string{"a", "b", "c"} {
			data, _ := encodePublish("chat", messageID, []byte("x"))
			publish, _ := channel.queue.publish("chat", messageID, data)
			publishes = append(publishes, publish)
		}

		channel.mutex.Unlock()
		synctest.Wait()

		channel.mutex.Lock()
		channel.cancelPublish(publishes[1], newError(ErrCancelled, "cancelled"))
		channel.mutex.Unlock()

		assertCode(t, <-publishes[1].result, ErrDeliveryUnknown)

		release()

		for _, publish := range []*queuedPublish{publishes[0], publishes[2]} {
			if err := <-publish.result; err != nil {
				t.Fatal(err)
			}
		}

		assertFrames(t, socket, "*3\n"+publishFrame("chat", "a", "x")+publishFrame("chat", "b", "x")+publishFrame("chat", "c", "x"))
	})
} // end function TestCancellingAPublishInTheBatchBeingWrittenIsDeliveryUnknown

func TestAPublishRefusedForItsSizeIsNotResent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		errorsSeen := recordErrors(channel)
		chat := segment(t, channel, "chat")
		publish(t, chat, "too-big", "big")
		publish(t, chat, "fits", "ok")
		socket.clearCommands()

		receiveAll(socket,
			errorFrame("MessageSizeLimitError", "Message size limit exceeded; payload size = 3 bytes; size limit = 1 KB", "PUB", bulk("too-big")),
			rateLimitFrame(),
		)
		synctest.Sleep(time.Second)
		assertCommands(t, socket, publishFrame("chat", "fits", "ok"))

		reported := errorsSeen.all()

		if len(reported) != 2 {
			t.Fatalf("errors %v", reported)
		}

		refused, ok := errors.AsType[*ServerError](reported[0])

		if !ok || refused.Type != MessageSizeLimitError || refused.SubType != "PUB" || refused.Resource != "too-big" {
			t.Fatalf("errors %v", reported)
		}
	})
} // end function TestAPublishRefusedForItsSizeIsNotResent
