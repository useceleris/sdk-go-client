package live

import (
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	celeris "github.com/useceleris/sdk-go-client"
)

// REV-01: a token's replay claim makes each segment join first deliver that
// segment's recent history, ids preserved.

// The server keeps at most this many messages per segment for replay.
const backlogCapacity = 100

func settle() {
	time.Sleep(1500 * time.Millisecond)
} // end function settle

func publishAll(t *testing.T, publisher *celeris.Channel, segmentID string, bodies ...string) {
	t.Helper()

	for _, body := range bodies {
		publishText(t, publisher, segmentID, body)
	}
} // end function publishAll

// awaitBody registers its listener at once, before the action that causes the
// delivery, and returns the wait for the first delivery of body. register is
// a segment's or the channel's OnMessage.
func awaitBody(t *testing.T, register func(func([]byte, celeris.MessageMetadata)) func(), body string) func() {
	t.Helper()

	arrived := make(chan struct{})
	var once sync.Once
	remove := register(func(payload []byte, _ celeris.MessageMetadata) {
		if string(payload) == body {
			once.Do(func() { close(arrived) })
		}
	})

	return func() {
		t.Helper()

		defer remove()

		select {
		case <-arrived:
		case <-time.After(25 * time.Second):
			t.Fatalf("timed out waiting for %q", body)
		}
	}
} // end function awaitBody

type identifiedDelivery struct {
	body      string
	messageID string
} // end struct identifiedDelivery

func identified(collected *collector) []identifiedDelivery {
	var deliveries []identifiedDelivery

	for _, message := range collected.all() {
		deliveries = append(deliveries, identifiedDelivery{string(message.payload), message.metadata.MessageID})
	}

	return deliveries
} // end function identified

func numbered(count int) []string {
	bodies := make([]string, count)

	for index := range bodies {
		bodies[index] = "m" + strconv.Itoa(index)
	}

	return bodies
} // end function numbered

func TestReplaysRecentMessagesWithIdenticalIDs(t *testing.T) {
	reference := uniqueChannelReference("replay")
	publisher := connectedChannel(t, reference)
	liveReceiver := connectedChannel(t, reference)
	liveHistory := segment(t, liveReceiver, "history")
	live := collect(liveHistory)
	subscribe(t, liveHistory)
	settle()

	lastLive := awaitBody(t, liveHistory.OnMessage, "three")
	publishAll(t, publisher, "history", "one", "two", "three")
	lastLive()

	replayReceiver := connectedChannel(t, reference, claim{"replay", 60_000})
	replayHistory := segment(t, replayReceiver, "history")
	replayed := collect(replayHistory)
	lastReplayed := awaitBody(t, replayHistory.OnMessage, "three")
	subscribe(t, replayHistory)
	lastReplayed()

	if got, want := identified(replayed), identified(live); !slices.Equal(got, want) {
		t.Fatalf("replayed %q, live %q", got, want)
	}

	for _, message := range identified(replayed) {
		if !generatedMessageID.MatchString(message.messageID) {
			t.Fatalf("message id %q", message.messageID)
		}
	}
} // end function TestReplaysRecentMessagesWithIdenticalIDs

func TestReplaysNothingToATokenWithoutAReplayClaim(t *testing.T) {
	reference := uniqueChannelReference("no-replay")
	publisher := connectedChannel(t, reference)
	publishAll(t, publisher, "history", "old-1", "old-2")
	settle()

	receiver := connectedChannel(t, reference)
	history := segment(t, receiver, "history")
	delivered := collect(history)
	subscribe(t, history)
	settle()

	// The live message proves the join, so a replay would have arrived first.
	live := awaitBody(t, history.OnMessage, "live")
	publishAll(t, publisher, "history", "live")
	live()
	settle()

	assertPayloads(t, delivered, "live")
} // end function TestReplaysNothingToATokenWithoutAReplayClaim

func TestReplaysInPublishOrder(t *testing.T) {
	reference := uniqueChannelReference("replay-order")
	publisher := connectedChannel(t, reference)
	published := numbered(10)
	publishAll(t, publisher, "history", published...)
	settle()

	receiver := connectedChannel(t, reference, claim{"replay", true})
	history := segment(t, receiver, "history")
	delivered := collect(history)
	last := awaitBody(t, history.OnMessage, "m9")
	subscribe(t, history)
	last()
	settle()

	assertPayloads(t, delivered, published...)
} // end function TestReplaysInPublishOrder

func TestReplaysAtMostTheLastHundredMessagesOfASegment(t *testing.T) {
	reference := uniqueChannelReference("replay-capacity")
	publisher := connectedChannel(t, reference)
	published := numbered(backlogCapacity + 5)

	// Paced below the per-second publish limit, so no publish is resent.
	for batch := range slices.Chunk(published, 10) {
		publishAll(t, publisher, "history", batch...)
		time.Sleep(1100 * time.Millisecond)
	}

	receiver := connectedChannel(t, reference, claim{"replay", true})
	history := segment(t, receiver, "history")
	delivered := collect(history)
	last := awaitBody(t, history.OnMessage, published[len(published)-1])
	subscribe(t, history)
	last()
	settle()

	assertPayloads(t, delivered, published[len(published)-backlogCapacity:]...)
} // end function TestReplaysAtMostTheLastHundredMessagesOfASegment

func TestReplaysOnlyTheWindowOfANumericReplayClaim(t *testing.T) {
	reference := uniqueChannelReference("replay-window")
	publisher := connectedChannel(t, reference)
	publishAll(t, publisher, "history", "old")
	time.Sleep(5 * time.Second)
	publishAll(t, publisher, "history", "recent")
	settle()

	receiver := connectedChannel(t, reference, claim{"replay", 3_000})
	history := segment(t, receiver, "history")
	delivered := collect(history)
	recent := awaitBody(t, history.OnMessage, "recent")
	subscribe(t, history)
	recent()
	time.Sleep(2500 * time.Millisecond)

	assertPayloads(t, delivered, "recent")
} // end function TestReplaysOnlyTheWindowOfANumericReplayClaim

func TestReplaysTheDefaultSegmentOnConnect(t *testing.T) {
	reference := uniqueChannelReference("replay-default")
	publisher := connectedChannel(t, reference)
	publishAll(t, publisher, "default", "d1", "d2")
	settle()

	// The listener is in place before connect, when the server joins default.
	receiver := newChannel(t, qualificationClient(t, websocketURL(), claim{"replay", true}), reference)
	lobby := collect(receiver.DefaultSegment())
	last := awaitBody(t, receiver.DefaultSegment().OnMessage, "d2")

	if err := receiver.Connect(t.Context()); err != nil {
		t.Fatalf("connect: %v", err)
	}

	last()

	assertPayloads(t, lobby, "d1", "d2")
} // end function TestReplaysTheDefaultSegmentOnConnect

func TestReplaysEachSegmentsBacklogWhenThatSegmentIsJoined(t *testing.T) {
	reference := uniqueChannelReference("replay-per-join")
	publisher := connectedChannel(t, reference)
	publishAll(t, publisher, "alpha", "a1")
	publishAll(t, publisher, "beta", "b1")
	settle()

	receiver := connectedChannel(t, reference, claim{"replay", true})
	alphaSegment := segment(t, receiver, "alpha")
	betaSegment := segment(t, receiver, "beta")
	alpha := collect(alphaSegment)
	beta := collect(betaSegment)
	alphaReplay := awaitBody(t, alphaSegment.OnMessage, "a1")
	subscribe(t, alphaSegment)
	alphaReplay()
	settle()
	assertPayloads(t, beta)

	betaReplay := awaitBody(t, betaSegment.OnMessage, "b1")
	subscribe(t, betaSegment)
	betaReplay()

	assertPayloads(t, alpha, "a1")
	assertPayloads(t, beta, "b1")
} // end function TestReplaysEachSegmentsBacklogWhenThatSegmentIsJoined

func TestReplaysToASegmentJoinedByPublishing(t *testing.T) {
	reference := uniqueChannelReference("replay-publish-join")
	publisher := connectedChannel(t, reference)
	publishAll(t, publisher, "history", "h1")
	settle()

	receiver := connectedChannel(t, reference, claim{"replay", true})
	history := segment(t, receiver, "history")
	delivered := collect(history)
	replay := awaitBody(t, history.OnMessage, "h1")
	publishAll(t, receiver, "history", "joining")
	replay()
	settle()

	assertPayloads(t, delivered, "h1")
} // end function TestReplaysToASegmentJoinedByPublishing

func TestRecoversWhatARejoinMissedAndDropsWhatItAlreadyDelivered(t *testing.T) {
	reference := uniqueChannelReference("replay-rejoin")
	publisher := connectedChannel(t, reference)
	receiver := connectedChannel(t, reference, claim{"replay", true})
	history := segment(t, receiver, "history")
	delivered := collect(history)
	first := subscribe(t, history)
	settle()

	one := awaitBody(t, history.OnMessage, "one")
	publishAll(t, publisher, "history", "one")
	one()

	first.Cancel()
	settle()
	publishAll(t, publisher, "history", "two")
	settle()

	// The re-join replays "one" and "two"; the deduplication window drops
	// "one".
	two := awaitBody(t, history.OnMessage, "two")
	subscribe(t, history)
	two()

	three := awaitBody(t, history.OnMessage, "three")
	publishAll(t, publisher, "history", "three")
	three()
	settle()

	assertPayloads(t, delivered, "one", "two", "three")
	identifiers := map[string]bool{}

	for _, message := range identified(delivered) {
		identifiers[message.messageID] = true
	}

	if len(identifiers) != 3 {
		t.Fatalf("%d unique message ids, want 3", len(identifiers))
	}
} // end function TestRecoversWhatARejoinMissedAndDropsWhatItAlreadyDelivered

func TestReplaysToTheChannelListenerOneTimeForEachMessage(t *testing.T) {
	reference := uniqueChannelReference("replay-channel-listener")
	publisher := connectedChannel(t, reference)
	publishAll(t, publisher, "history", "h1", "h2")
	settle()

	receiver := connectedChannel(t, reference, claim{"replay", true})
	seen := &collector{}
	receiver.Events().OnMessage(seen.record)
	both := awaitBody(t, receiver.Events().OnMessage, "h2")
	membership := subscribe(t, segment(t, receiver, "history"))
	both()

	// A re-join replays both again; the channel listener sees neither twice.
	membership.Cancel()
	settle()
	subscribe(t, segment(t, receiver, "history"))
	time.Sleep(4 * time.Second)

	var tagged []string

	for _, message := range seen.all() {
		tagged = append(tagged, message.metadata.SegmentID+":"+string(message.payload))
	}

	if want := []string{"history:h1", "history:h2"}; !slices.Equal(tagged, want) {
		t.Fatalf("seen %q, want %q", tagged, want)
	}
} // end function TestReplaysToTheChannelListenerOneTimeForEachMessage
