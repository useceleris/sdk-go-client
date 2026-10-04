package live

import (
	"testing"
	"time"
)

// Connections opened through the entrypoint are spread across the stack's
// nodes; these assert fan-out and presence consistency between independent
// connections (REC-03).

func TestFansOutPublishesAcrossNodes(t *testing.T) {
	reference := uniqueChannelReference("xnode")
	primary := connectedChannel(t, reference)
	secondary := connectedChannel(t, reference)
	subscribe(t, segment(t, secondary, "chat"))
	time.Sleep(2 * time.Second)

	if err := segment(t, primary, "chat").Publish(t.Context(), []byte("across")); err != nil {
		t.Fatal(err)
	}

	message := nextMessage(t, segment(t, secondary, "chat"), func(message delivery) bool { return string(message.payload) == "across" }, "cross-node delivery", 25*time.Second)

	if !generatedMessageID.MatchString(message.metadata.MessageID) {
		t.Fatalf("message id %q", message.metadata.MessageID)
	}
}

func TestReportsConsistentPresenceAcrossNodes(t *testing.T) {
	reference := uniqueChannelReference("xpres")
	primary := connectedChannel(t, reference)
	secondary := connectedChannel(t, reference)
	subscribe(t, segment(t, primary, "room"))
	subscribe(t, segment(t, secondary, "room"))
	time.Sleep(3 * time.Second)

	fromPrimary, err := segment(t, primary, "room").PresenceList(t.Context(), 1, 25)

	if err != nil {
		t.Fatal(err)
	}

	fromSecondary, err := segment(t, secondary, "room").PresenceList(t.Context(), 1, 25)

	if err != nil || fromPrimary.Total != fromSecondary.Total || fromPrimary.Total < 2 {
		t.Fatalf("primary %d, secondary %d, %v", fromPrimary.Total, fromSecondary.Total, err)
	}
}
