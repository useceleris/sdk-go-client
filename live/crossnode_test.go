package live

import (
	"slices"
	"strconv"
	"testing"
	"time"

	celeris "github.com/useceleris/sdk-go-client"
)

// The second connection opens through CELERIS_WS_URL_PEER, a gateway that
// routes to a different server node; without it these tests skip. They assert
// fan-out, presence consistency, presence events, replay and ordering between
// independent connections (REC-03, PRESENCE-04).

func TestFansOutPublishesAcrossNodes(t *testing.T) {
	peer := peerWebsocketURL(t)
	reference := uniqueChannelReference("xnode")
	primary := connectedChannel(t, reference)
	secondary := connectedChannelAt(t, peer, reference)
	subscribe(t, segment(t, secondary, "chat"))
	time.Sleep(2 * time.Second)

	if err := segment(t, primary, "chat").Publish(t.Context(), []byte("across")); err != nil {
		t.Fatal(err)
	}

	message := nextMessage(t, segment(t, secondary, "chat"), func(message delivery) bool { return string(message.payload) == "across" }, "cross-node delivery", 25*time.Second)

	if !generatedMessageID.MatchString(message.metadata.MessageID) {
		t.Fatalf("message id %q", message.metadata.MessageID)
	}
} // end function TestFansOutPublishesAcrossNodes

func TestReportsConsistentPresenceAcrossNodes(t *testing.T) {
	peer := peerWebsocketURL(t)
	reference := uniqueChannelReference("xpres")
	primary := connectedChannel(t, reference)
	secondary := connectedChannelAt(t, peer, reference)
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
} // end function TestReportsConsistentPresenceAcrossNodes

func TestDeliversAPresenceJoinFromAnotherNode(t *testing.T) {
	peer := peerWebsocketURL(t)
	reference := uniqueChannelReference("xpres-event")
	watcher := connectedChannel(t, reference)
	room := segment(t, watcher, "room")

	if _, err := room.SubscribePresence(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)

	joiner := connectedChannelAt(t, peer, reference)
	joins := make(chan celeris.PresenceEvent, 8)
	room.OnPresence(func(event celeris.PresenceEvent) {
		if event.Joined {
			select {
			case joins <- event:
			default:
			}
		}
	})

	subscribe(t, segment(t, joiner, "room"))

	select {
	case join := <-joins:
		if join.SegmentID != "room" {
			t.Fatalf("join for segment %q", join.SegmentID)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for a cross-node join")
	}
} // end function TestDeliversAPresenceJoinFromAnotherNode

func TestDeliversAPresenceLeaveFromAnotherNode(t *testing.T) {
	peer := peerWebsocketURL(t)
	reference := uniqueChannelReference("xpres-leave")
	watcher := connectedChannel(t, reference)
	room := segment(t, watcher, "room")
	subscribePresence(t, room)
	time.Sleep(2 * time.Second)
	leaver := connectedChannelAt(t, peer, reference)

	joined := awaitPresence(t, room, func(event celeris.PresenceEvent) bool { return event.Joined }, "the cross-node join", 20*time.Second)
	subscribe(t, segment(t, leaver, "room"))
	join := joined()

	left := awaitPresence(t, room, func(event celeris.PresenceEvent) bool {
		return !event.Joined && event.ConnectionID == join.ConnectionID
	}, "the cross-node leave", 20*time.Second)

	leaver.Close()
	left()
} // end function TestDeliversAPresenceLeaveFromAnotherNode

func TestReplaysHistoryPublishedOnAnotherNode(t *testing.T) {
	peer := peerWebsocketURL(t)
	reference := uniqueChannelReference("xreplay")
	publisher := connectedChannel(t, reference)
	publishAll(t, publisher, "history", "h1", "h2", "h3")
	time.Sleep(2 * time.Second)

	receiver := connectedChannelAt(t, peer, reference, claim{"replay", true})
	history := segment(t, receiver, "history")
	replayed := collect(history)
	last := awaitMessage(t, history, withBody("h3"), "the cross-node replay", 25*time.Second)
	subscribe(t, history)
	last()
	time.Sleep(2 * time.Second)

	assertPayloads(t, replayed, "h1", "h2", "h3")
} // end function TestReplaysHistoryPublishedOnAnotherNode

func TestKeepsOneOriginsOrderAcrossNodes(t *testing.T) {
	peer := peerWebsocketURL(t)
	reference := uniqueChannelReference("xorder")
	publisher := connectedChannel(t, reference)
	receiver := connectedChannelAt(t, peer, reference)
	chat := segment(t, receiver, "chat")
	received := collect(chat)
	subscribe(t, chat)
	time.Sleep(2 * time.Second)
	bodies := make([]string, 30)

	for index := range bodies {
		bodies[index] = "o" + strconv.Itoa(index)
	}

	last := awaitMessage(t, chat, withBody("o29"), "the last ordered message", 30*time.Second)

	for batch := range slices.Chunk(bodies, 10) {
		publishAll(t, publisher, "chat", batch...)
		time.Sleep(1100 * time.Millisecond)
	}

	last()
	time.Sleep(2 * time.Second)

	assertPayloads(t, received, bodies...)
} // end function TestKeepsOneOriginsOrderAcrossNodes
