package celeris

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// Boundary cases: each pins one comparison to the side the reference takes.

func TestChannelReferenceCharacterClassEdges(t *testing.T) {
	if rule := channelReferenceRule("azAZ09-_"); rule != "" {
		t.Fatalf("edge characters refused: %s", rule)
	}

	for _, character := range []string{"`", "{", "@", "[", "/", ":", " ", "."} {
		if channelReferenceRule("a"+character+"b") == "" {
			t.Errorf("%q accepted", character)
		}
	}
} // end function TestChannelReferenceCharacterClassEdges

func TestErrorNameCharacterClassEdges(t *testing.T) {
	for _, name := range []string{"A", "Z", "a", "z", "Az09_"} {
		if !validErrorName([]byte(name)) {
			t.Errorf("%q refused", name)
		}
	}

	for _, name := range []string{"@", "[", "`", "{", "0A", "_A", "A/", "A:", "A-"} {
		if validErrorName([]byte(name)) {
			t.Errorf("%q accepted", name)
		}
	}
} // end function TestErrorNameCharacterClassEdges

func TestBulkLengthExactlyTheRemainingBytesNeedsItsTerminator(t *testing.T) {
	_, err := decodeServerMessage([]byte("@SERVER_MSG\n:1\n$1\nx"))
	assertProtocolError(t, err, "Missing bulk byte terminator.", "Payload", 15)
} // end function TestBulkLengthExactlyTheRemainingBytesNeedsItsTerminator

func TestPresenceConnectionsCountTowardTheDepthLimit(t *testing.T) {
	response := "@PRES_LIST_RESPONSE\n+s\n$1\n1\n;1\n;1\n;1\n;1\n;1\n*1\n*3\n+u\n+c\n:1\n"

	if _, err := decodeServerMessage([]byte(strings.Repeat("*1\n", 30) + response)); err != nil {
		t.Fatalf("connection at depth 31 refused: %v", err)
	}

	if _, err := decodeServerMessage([]byte(strings.Repeat("*1\n", 31) + response)); !errors.Is(err, ErrProtocol) {
		t.Fatalf("connection at depth 32 accepted: %v", err)
	}
} // end function TestPresenceConnectionsCountTowardTheDepthLimit

func TestErrorResourcesCountTowardTheDepthLimit(t *testing.T) {
	frame := func(levels int) string {
		return "-Err\n+ParserError\n$-1\n$1\nm\n" + strings.Repeat("*1\n", levels) + "$-1\n"
	}

	if _, err := decodeServerMessage([]byte(frame(31))); err != nil {
		t.Fatalf("31 resource levels refused: %v", err)
	}

	if _, err := decodeServerMessage([]byte(frame(32))); !errors.Is(err, ErrProtocol) {
		t.Fatalf("32 resource levels accepted: %v", err)
	}
} // end function TestErrorResourcesCountTowardTheDepthLimit

func TestEveryCommandIsMeasuredExactlyAgainstTheLimit(t *testing.T) {
	// "@PUB\n$1\ns\n$1\nm\n$2097127\n" and the closing LF are 25 bytes.
	if encoded, err := encodePublish("s", "m", make([]byte, maximumCommandBytes-25)); err != nil || len(encoded) != maximumCommandBytes {
		t.Fatalf("publish with an id at the limit: %d bytes, %v", len(encoded), err)
	}

	if _, err := encodePublish("s", "m", make([]byte, maximumCommandBytes-24)); !errors.Is(err, ErrConfiguration) {
		t.Fatalf("publish with an id over the limit: %v", err)
	}

	// "@SUB\n$2097143\n", the id and LF: 15 bytes of framing.
	segmentID := strings.Repeat("s", maximumCommandBytes-15)

	if encoded, err := encodeSegmentCommand("SUB", segmentID); err != nil || len(encoded) != maximumCommandBytes {
		t.Fatalf("subscribe at the limit: %d bytes, %v", len(encoded), err)
	}

	if _, err := encodeSegmentCommand("SUB", segmentID+"s"); !errors.Is(err, ErrConfiguration) {
		t.Fatalf("subscribe over the limit: %v", err)
	}

	// "@PRES_LIST\n$1\ns\n;1\n;25\n$2097130\n", the id and LF: 33 bytes.
	requestID := strings.Repeat("1", maximumCommandBytes-33)

	if encoded, err := encodePresenceList("s", 1, 25, requestID); err != nil || len(encoded) != maximumCommandBytes {
		t.Fatalf("presence query at the limit: %d bytes, %v", len(encoded), err)
	}

	if _, err := encodePresenceList("s", 1, 25, requestID+"1"); !errors.Is(err, ErrConfiguration) {
		t.Fatalf("presence query over the limit: %v", err)
	}
} // end function TestEveryCommandIsMeasuredExactlyAgainstTheLimit

func TestDeduplicationWindowHoldsExactly1024IDs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		delivered := recordMessageIDs(segment(t, channel, "chat"))

		for index := range 1024 {
			receiveAll(socket, messageFrame("chat", "id-"+strconv.Itoa(index), "x"))
		}

		// All 1024 are still remembered.
		receiveAll(socket, messageFrame("chat", "id-0", "x"))

		if got := len(delivered.all()); got != 1024 {
			t.Fatalf("%d delivered, want the repeat dropped", got)
		}

		// One more evicts the oldest.
		receiveAll(socket, messageFrame("chat", "id-1024", "x"), messageFrame("chat", "id-0", "x"))

		if got := len(delivered.all()); got != 1026 {
			t.Fatalf("%d delivered, want the evicted id delivered again", got)
		}
	})
} // end function TestDeduplicationWindowHoldsExactly1024IDs

// A command sent exactly two seconds before a limit is still a suspect, and
// recording a newer command does not expire it early.
func TestRateLimitSuspectWindowIncludesItsEdge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		subscribe(t, segment(t, channel, "alpha"))
		publish(t, segment(t, channel, "lobby"), "early", "x")
		synctest.Wait()
		synctest.Sleep(rateLimitSuspectWindow)
		subscribe(t, segment(t, channel, "beta"))
		publish(t, segment(t, channel, "lobby"), "late", "y")
		synctest.Wait()
		socket.clearCommands()

		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(time.Second)

		assertCommands(t, socket, "@SUB\n$5\nalpha\n", "@SUB\n$4\nbeta\n", publishFrame("lobby", "early", "x"), publishFrame("lobby", "late", "y"))
	})
} // end function TestRateLimitSuspectWindowIncludesItsEdge

// With nothing to abandon, a quota limit schedules no probe, so the next
// probe still starts at a minute.
func TestQuotaLimitWithNothingToAbandonSchedulesNoProbe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)

		// Eight limits, each with a publish sent since, so none is a repeat.
		for index := range 8 {
			receiveAll(socket, rateLimitFrame())
			synctest.Sleep(time.Second)
			publish(t, channel.DefaultSegment(), "m-"+strconv.Itoa(index), "x")
		}

		// The ninth is a quota limit, with no subscription sent to abandon.
		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(time.Second)

		subscribe(t, segment(t, channel, "chat"))
		synctest.Wait()
		socket.clearCommands()
		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(time.Minute - time.Millisecond)
		assertCommands(t, socket)
		synctest.Sleep(time.Millisecond)
		assertCommands(t, socket, "@SUB\n$4\nchat\n")
	})
} // end function TestQuotaLimitWithNothingToAbandonSchedulesNoProbe

// Probing ends only once commands went strictly longer than the quiet span
// without a limit.
func TestProbingEndsOnlyAfterTheQuietSpanHasPassed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		subscribe(t, segment(t, channel, "chat"))
		synctest.Wait()
		exhaustRateLimit(socket)
		receiveAll(socket, rateLimitFrame())
		synctest.Sleep(time.Minute)

		// The probe went out just now. Exactly two seconds later the quota is
		// not yet proven back, so a new subscription does not restore anything.
		synctest.Sleep(rateLimitSuspectWindow)
		channel.mutex.Lock()
		probing := channel.queue.probeCount
		channel.mutex.Unlock()
		subscribe(t, segment(t, channel, "lobby"))
		synctest.Wait()

		channel.mutex.Lock()
		stillProbing := channel.queue.probeCount
		channel.mutex.Unlock()

		if probing == 0 || stillProbing != probing {
			t.Fatalf("probe count %d then %d, want probing to continue", probing, stillProbing)
		}

		synctest.Sleep(time.Millisecond)
		subscribe(t, segment(t, channel, "other"))
		synctest.Wait()

		channel.mutex.Lock()
		ended := channel.queue.probeCount
		channel.mutex.Unlock()

		if ended != 0 {
			t.Fatalf("probe count %d, want probing ended", ended)
		}
	})
} // end function TestProbingEndsOnlyAfterTheQuietSpanHasPassed

func TestPublishPastItsDeadlineIsATimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, _ := connectTestChannel(t)
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()

		synctest.Sleep(2 * time.Second)

		err := channel.DefaultSegment().Publish(ctx, []byte("x"))
		assertCode(t, err, ErrTimeout)

		if err.Error() != "Publish not sent: its context's deadline had already passed." {
			t.Fatalf("message %q", err.Error())
		}
	})
} // end function TestPublishPastItsDeadlineIsATimeout
