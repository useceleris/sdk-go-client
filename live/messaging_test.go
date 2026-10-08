package live

import (
	"bytes"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	celeris "github.com/useceleris/sdk-go-client"
)

func TestDeliversBinaryPayloadsWithTheirIDsAndPerConnectionEcho(t *testing.T) {
	reference := uniqueChannelReference("msg")
	publisher := connectedChannel(t, reference)
	receiver := connectedChannel(t, reference)
	publisherSaw := collect(segment(t, publisher, "chat"))
	subscribe(t, segment(t, publisher, "chat"))
	subscribe(t, segment(t, receiver, "chat"))
	time.Sleep(1500 * time.Millisecond)

	if err := segment(t, publisher, "chat").Publish(t.Context(), []byte("hello-바이너리")); err != nil {
		t.Fatal(err)
	}

	message := nextMessage(t, segment(t, receiver, "chat"), func(delivery) bool { return true }, "cross-connection delivery", 15*time.Second)

	// REV-01: the id the SDK generated arrives unchanged.
	if !generatedMessageID.MatchString(message.metadata.MessageID) || string(message.payload) != "hello-바이너리" || message.metadata.Timestamp <= 0 {
		t.Fatalf("delivery %+v", message)
	}

	// The publishing connection itself is echo-suppressed.
	time.Sleep(1500 * time.Millisecond)

	if len(publisherSaw.all()) != 0 {
		t.Fatal("the publisher received its own publish without echo")
	}
} // end function TestDeliversBinaryPayloadsWithTheirIDsAndPerConnectionEcho

func TestEchoesToThePublisherWhenTheTokenAllowsEcho(t *testing.T) {
	channel := connectedChannel(t, uniqueChannelReference("echo"), claim{"allow_echo", true})
	chat := segment(t, channel, "chat")
	subscribe(t, chat)
	time.Sleep(1500 * time.Millisecond)

	if err := chat.Publish(t.Context(), []byte("self")); err != nil {
		t.Fatal(err)
	}

	nextMessage(t, chat, func(message delivery) bool { return string(message.payload) == "self" }, "an echoed publish", 15*time.Second)
} // end function TestEchoesToThePublisherWhenTheTokenAllowsEcho

func TestDeliversOnTheDefaultSegmentWithoutSubscribing(t *testing.T) {
	reference := uniqueChannelReference("default")
	publisher := connectedChannel(t, reference)
	receiver := connectedChannel(t, reference)
	time.Sleep(1500 * time.Millisecond)

	if err := publisher.DefaultSegment().Publish(t.Context(), []byte("lobby")); err != nil {
		t.Fatal(err)
	}

	message := nextMessage(t, receiver.DefaultSegment(), func(message delivery) bool { return string(message.payload) == "lobby" }, "default-segment delivery", 15*time.Second)

	if message.metadata.SegmentID != "default" {
		t.Fatalf("segment %q", message.metadata.SegmentID)
	}
} // end function TestDeliversOnTheDefaultSegmentWithoutSubscribing

// Needs the qualification app on a plan with message_size_limit_in_kb of at
// least 1024. Framed, the full payload is larger than 1 MiB (LIMIT-01).
func TestRoundTripsLargeBinaryPayloads(t *testing.T) {
	for _, size := range []int{100 * 1024, 1024 * 1024} {
		reference := uniqueChannelReference("large")
		publisher := connectedChannel(t, reference)
		receiver := connectedChannel(t, reference)
		subscribe(t, segment(t, receiver, "bulk"))
		time.Sleep(1500 * time.Millisecond)
		payload := patterned(size)

		if err := segment(t, publisher, "bulk").Publish(t.Context(), payload); err != nil {
			t.Fatal(err)
		}

		message := nextMessage(t, segment(t, receiver, "bulk"), func(message delivery) bool { return len(message.payload) == size }, "the large delivery", 30*time.Second)

		if !bytes.Equal(message.payload, payload) {
			t.Fatalf("%d-byte payload changed in transit", size)
		}
	}
} // end function TestRoundTripsLargeBinaryPayloads

func TestReportsAPublishOverThePlanCapAsMessageSizeLimit(t *testing.T) {
	channel := connectedChannel(t, uniqueChannelReference("oversize"))

	// Over every plan's cap but under the 2 MiB ceiling, so it completes
	// locally before the server rejects it.
	if err := segment(t, channel, "bulk").Publish(t.Context(), make([]byte, 1536*1024)); err != nil {
		t.Fatal(err)
	}

	rejection := nextError(t, channel, serverErrorOfType(celeris.MessageSizeLimitError), "the MessageSizeLimitError frame", 20*time.Second)

	if !strings.Contains(rejection.Error(), "Message size limit exceeded") || channel.State() != celeris.StateConnected {
		t.Fatalf("got %v, state %s", rejection, channel.State())
	}
} // end function TestReportsAPublishOverThePlanCapAsMessageSizeLimit

func TestRefusesACommandOverTwoMebibytesLocallyAndStaysConnected(t *testing.T) {
	reference := uniqueChannelReference("ceiling")
	publisher := connectedChannel(t, reference)
	receiver := connectedChannel(t, reference)
	bulk := segment(t, receiver, "bulk")
	received := collect(bulk)
	subscribe(t, bulk)
	settle()

	if err := segment(t, publisher, "bulk").Publish(t.Context(), make([]byte, 2*1024*1024)); !errors.Is(err, celeris.ErrConfiguration) {
		t.Fatalf("got %v", err)
	}

	publishAndAwait(t, publisher, receiver, "bulk", "after")

	assertPayloads(t, received, "after")

	if state := publisher.State(); state != celeris.StateConnected {
		t.Fatalf("state %s", state)
	}
} // end function TestRefusesACommandOverTwoMebibytesLocallyAndStaysConnected

func TestReportsAReadOnlyTokensPublishAsPermissionDenied(t *testing.T) {
	readOnly := connectedChannel(t, uniqueChannelReference("perm"), claim{"token_permission", map[string]bool{"read": true, "write": false}})

	// Publish completes locally; the denial arrives later through OnError.
	if err := segment(t, readOnly, "chat").Publish(t.Context(), []byte("denied")); err != nil {
		t.Fatal(err)
	}

	denial, ok := errors.AsType[*celeris.ServerError](nextError(t, readOnly, serverErrorOfType(celeris.PermissionDeniedError), "the PermissionDeniedError frame", 15*time.Second))

	if !ok {
		t.Fatal("not a server error")
	}

	if denial.SubType != "PUB" || denial.Resource != "chat" || denial.Message == "" || readOnly.State() != celeris.StateConnected {
		t.Fatalf("denial %+v", denial)
	}
} // end function TestReportsAReadOnlyTokensPublishAsPermissionDenied

func TestKeepsAWriteOnlyTokenPublishingWhileReceivingNothing(t *testing.T) {
	reference := uniqueChannelReference("writeonly")
	writeOnly := connectedChannel(t, reference, claim{"token_permission", map[string]bool{"read": false, "write": true}})
	reader := connectedChannel(t, reference)
	writerSaw := collect(segment(t, writeOnly, "chat"))
	subscribe(t, segment(t, writeOnly, "chat"))
	subscribe(t, segment(t, reader, "chat"))
	time.Sleep(1500 * time.Millisecond)

	if err := segment(t, writeOnly, "chat").Publish(t.Context(), []byte("one-way")); err != nil {
		t.Fatal(err)
	}

	nextMessage(t, segment(t, reader, "chat"), func(message delivery) bool { return string(message.payload) == "one-way" }, "delivery to the reader", 15*time.Second)
	time.Sleep(1500 * time.Millisecond)

	if len(writerSaw.all()) != 0 {
		t.Fatal("a write-only token received")
	}
} // end function TestKeepsAWriteOnlyTokenPublishingWhileReceivingNothing

// RES-05: subscriptions the server drops under its rate limit recover.
func TestRecoversSubscriptionsTheServerDropsUnderItsRateLimit(t *testing.T) {
	reference := uniqueChannelReference("limit")

	// Separate token references keep separate per-connection limits.
	subscriber := connectedChannel(t, reference, claim{"reference", "limit-subscriber"})
	publisher := connectedChannel(t, reference, claim{"reference", "limit-publisher"})
	var mutex sync.Mutex
	rateLimited := false
	delivered := map[string]bool{}
	subscriber.Events().OnError(func(err error) {
		if serverErrorOfType(celeris.RateLimitError)(err) {
			mutex.Lock()
			rateLimited = true
			mutex.Unlock()
		}
	})

	// The limiter tolerates bursts, so subscriptions go out in growing batches
	// until one trips it; some are then dropped, and only recovery can restore
	// them.
	var segmentIDs []string

	limited := func() bool {
		mutex.Lock()
		defer mutex.Unlock()

		return rateLimited
	}

	for !limited() && len(segmentIDs) < 2000 {
		for range 250 {
			segmentID := "limit-" + strconv.Itoa(len(segmentIDs))
			segmentIDs = append(segmentIDs, segmentID)
			handle := segment(t, subscriber, segmentID)
			handle.OnMessage(func([]byte, celeris.MessageMetadata) {
				mutex.Lock()
				delivered[segmentID] = true
				mutex.Unlock()
			})

			subscribe(t, handle)
		}

		time.Sleep(500 * time.Millisecond)
	}

	if !limited() {
		t.Fatal("the subscription burst must trip the limit")
	}

	// Publish to every segment not yet delivered, round after round, until each
	// subscription has recovered. A publish the publisher's own limit drops is
	// published again next round.
	deadline := time.Now().Add(150 * time.Second)

	pending := func() []string {
		mutex.Lock()
		defer mutex.Unlock()

		var missing []string

		for _, segmentID := range segmentIDs {
			if !delivered[segmentID] {
				missing = append(missing, segmentID)
			}
		}

		return missing
	}

	for len(pending()) > 0 && time.Now().Before(deadline) {
		time.Sleep(3 * time.Second)

		for _, segmentID := range pending() {
			err := segment(t, publisher, segmentID).Publish(t.Context(), []byte(segmentID))

			switch {
			case errors.Is(err, celeris.ErrBackpressure):
				// The publisher trips its own limit: let it drain.
				time.Sleep(2 * time.Second)
			case errors.Is(err, celeris.ErrNotConnected):
				// Under this load the server sheds connections ("channel
				// broadcast processing lag exceeded threshold"); the channel
				// recovers by itself, restoring its subscriptions.
				time.Sleep(time.Second)
			case err != nil:
				t.Fatal(err)
			}

			time.Sleep(10 * time.Millisecond)
		}
	}

	if missing := pending(); len(missing) != 0 {
		t.Fatalf("%d of %d subscriptions never recovered", len(missing), len(segmentIDs))
	}
} // end function TestRecoversSubscriptionsTheServerDropsUnderItsRateLimit

// RESEND-01: publishes a rate limit may have dropped are resent with their
// original ids, so the receiver drops every copy that got through.
func TestDeliversNoDuplicateUnderAPublishRateLimit(t *testing.T) {
	reference := uniqueChannelReference("limit-publish")

	// Separate token references keep separate per-connection limits.
	receiver := connectedChannel(t, reference, claim{"reference", "limit-receiver"})
	publisher := connectedChannel(t, reference, claim{"reference", "limit-publisher"})
	received := collect(segment(t, receiver, "burst"))
	subscribe(t, segment(t, receiver, "burst"))
	settle()

	var mutex sync.Mutex
	rateLimited := false
	publisher.Events().OnError(func(err error) {
		if serverErrorOfType(celeris.RateLimitError)(err) {
			mutex.Lock()
			rateLimited = true
			mutex.Unlock()
		}
	})

	limited := func() bool {
		mutex.Lock()
		defer mutex.Unlock()

		return rateLimited
	}

	// The limiter tolerates bursts, so publishes go out in growing bursts
	// until one trips it, with a ceiling on the total so the test ends.
	burst := segment(t, publisher, "burst")
	published := map[string]bool{}

	for size := 100; !limited() && len(published) < 5000; size *= 2 {
		for range size {
			body := "b-" + strconv.Itoa(len(published))
			published[body] = true
			err := burst.Publish(t.Context(), []byte(body))

			if errors.Is(err, celeris.ErrBackpressure) {
				// Sending is paused after a limit: this burst is over.
				break
			}

			if err != nil {
				t.Fatal(err)
			}
		}

		time.Sleep(500 * time.Millisecond)
	}

	if !limited() {
		t.Fatal("the publish bursts must trip the limit")
	}

	time.Sleep(3 * time.Second)
	marker := awaitMessage(t, segment(t, receiver, "burst"), withBody("marker"), "the marker", 30*time.Second)

	if err := burst.Publish(t.Context(), []byte("marker")); err != nil {
		t.Fatal(err)
	}

	marker()

	seen := map[string]bool{}

	for _, message := range received.all() {
		body := string(message.payload)

		if seen[message.metadata.MessageID] {
			t.Fatalf("message id %s delivered twice", message.metadata.MessageID)
		}

		seen[message.metadata.MessageID] = true

		if !published[body] && body != "marker" {
			t.Fatalf("delivered %q, which was never published", body)
		}
	}

	for _, channel := range []*celeris.Channel{publisher, receiver} {
		if state := channel.State(); state != celeris.StateConnected {
			t.Fatalf("state %s after the bursts", state)
		}
	}
} // end function TestDeliversNoDuplicateUnderAPublishRateLimit

func TestDeliversACustomMessageIDUnchangedAndDropsARepeatOfIt(t *testing.T) {
	reference := uniqueChannelReference("custom-id")
	publisher := connectedChannel(t, reference)
	receiver := connectedChannel(t, reference)
	chat := segment(t, receiver, "chat")
	received := collect(chat)
	subscribe(t, chat)
	settle()
	messageID := "order-" + strconv.FormatInt(time.Now().UnixMilli(), 10)

	for _, body := range []string{"first", "repeat"} {
		if err := segment(t, publisher, "chat").PublishWithMessageID(t.Context(), []byte(body), messageID); err != nil {
			t.Fatal(err)
		}
	}

	publishAndAwait(t, publisher, receiver, "chat", "marker")

	assertPayloads(t, received, "first", "marker")

	if got := received.all()[0].metadata.MessageID; got != messageID {
		t.Fatalf("message id %q, want %q", got, messageID)
	}
} // end function TestDeliversACustomMessageIDUnchangedAndDropsARepeatOfIt

func TestRoundTripsAnEmptyPayload(t *testing.T) {
	reference := uniqueChannelReference("empty")
	publisher := connectedChannel(t, reference)
	receiver := connectedChannel(t, reference)
	chat := segment(t, receiver, "chat")
	subscribe(t, chat)
	settle()

	arrived := awaitMessage(t, chat, func(message delivery) bool { return len(message.payload) == 0 }, "the empty payload", 15*time.Second)

	if err := segment(t, publisher, "chat").Publish(t.Context(), []byte{}); err != nil {
		t.Fatal(err)
	}

	if message := arrived(); !generatedMessageID.MatchString(message.metadata.MessageID) {
		t.Fatalf("message id %q", message.metadata.MessageID)
	}
} // end function TestRoundTripsAnEmptyPayload

func TestRefusesAPayloadOneByteOverThe1024KiBPlanCap(t *testing.T) {
	channel := connectedChannel(t, uniqueChannelReference("cap-plus-one"))
	rejected := awaitEvent(t, channel.Events().OnError, serverErrorOfType(celeris.MessageSizeLimitError), "the MessageSizeLimitError frame", 20*time.Second)

	if err := segment(t, channel, "bulk").Publish(t.Context(), make([]byte, 1024*1024+1)); err != nil {
		t.Fatal(err)
	}

	if rejection := rejected(); !strings.Contains(rejection.Error(), "size limit = 1024 KB") || channel.State() != celeris.StateConnected {
		t.Fatalf("got %v, state %s", rejection, channel.State())
	}
} // end function TestRefusesAPayloadOneByteOverThe1024KiBPlanCap

func TestDeliversAPacedBurstOf50MessagesInPublishOrderOneTimeEach(t *testing.T) {
	reference := uniqueChannelReference("burst")
	publisher := connectedChannel(t, reference)
	receiver := connectedChannel(t, reference)
	chat := segment(t, receiver, "chat")
	received := collect(chat)
	subscribe(t, chat)
	settle()
	bodies := make([]string, 50)

	for index := range bodies {
		bodies[index] = "b" + strconv.Itoa(index)
	}

	last := awaitMessage(t, chat, withBody("b49"), "the last message of the burst", 30*time.Second)

	// Paced below the per-second publish limit.
	for batch := range slices.Chunk(bodies, 10) {
		publishAll(t, publisher, "chat", batch...)
		time.Sleep(1100 * time.Millisecond)
	}

	last()
	settle()

	assertPayloads(t, received, bodies...)
	messageIDs := map[string]bool{}

	for _, message := range received.all() {
		messageIDs[message.metadata.MessageID] = true
	}

	if len(messageIDs) != 50 {
		t.Fatalf("%d unique message ids, want 50", len(messageIDs))
	}
} // end function TestDeliversAPacedBurstOf50MessagesInPublishOrderOneTimeEach
