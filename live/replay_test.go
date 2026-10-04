package live

import (
	"sync"
	"testing"
	"time"

	celeris "github.com/useceleris/sdk-go-client"
)

// REV-01: a fresh connection with a replay lookback receives the same
// messages again, ids preserved.
func TestReplaysRecentMessagesWithIdenticalIDs(t *testing.T) {
	reference := uniqueChannelReference("replay")
	publisher := connectedChannel(t, reference)
	liveReceiver := connectedChannel(t, reference)
	live := collect(segment(t, liveReceiver, "history"))
	subscribe(t, segment(t, liveReceiver, "history"))
	time.Sleep(1500 * time.Millisecond)

	for _, body := range []string{"one", "two", "three"} {
		if err := segment(t, publisher, "history").Publish(t.Context(), []byte(body)); err != nil {
			t.Fatal(err)
		}
	}

	nextMessage(t, segment(t, liveReceiver, "history"), func(delivery) bool { return len(live.all()) >= 3 }, "the three live deliveries", 20*time.Second)
	liveReceiver.Close()

	replayReceiver := connectedChannel(t, reference, claim{"replay", 60_000})
	var mutex sync.Mutex
	replayed := map[string]string{}
	history := segment(t, replayReceiver, "history")
	history.OnMessage(func(payload []byte, metadata celeris.MessageMetadata) {
		mutex.Lock()
		replayed[string(payload)] = metadata.MessageID
		mutex.Unlock()
	})
	subscribe(t, history)
	nextMessage(t, history, func(delivery) bool {
		mutex.Lock()
		defer mutex.Unlock()

		return len(replayed) >= 3
	}, "the replayed history", 25*time.Second)

	mutex.Lock()
	defer mutex.Unlock()

	for index, message := range live.all()[:3] {
		if replayed[string(message.payload)] != message.metadata.MessageID || !generatedMessageID.MatchString(message.metadata.MessageID) {
			t.Fatalf("message %d replayed as %q, live id %q", index, replayed[string(message.payload)], message.metadata.MessageID)
		}
	}
}

// Re-joining a segment replays history; the deduplication window absorbs it.
func TestDeduplicationHoldsAgainstOverlappingReplay(t *testing.T) {
	reference := uniqueChannelReference("dedup")
	publisher := connectedChannel(t, reference)
	receiver := connectedChannel(t, reference, claim{"replay", 60_000})
	history := segment(t, receiver, "history")
	delivered := collect(history)
	membership := subscribe(t, history)
	time.Sleep(1500 * time.Millisecond)

	_ = segment(t, publisher, "history").Publish(t.Context(), []byte("first"))
	nextMessage(t, history, func(delivery) bool { return len(delivered.all()) >= 1 }, "the live delivery", 20*time.Second)

	membership.Cancel()
	time.Sleep(1500 * time.Millisecond)
	subscribe(t, history)
	time.Sleep(4 * time.Second)

	identifiers := map[string]bool{}

	for _, message := range delivered.all() {
		if identifiers[message.metadata.MessageID] {
			t.Fatalf("duplicate delivery %s", message.metadata.MessageID)
		}

		identifiers[message.metadata.MessageID] = true
	}
}
