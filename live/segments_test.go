package live

import (
	"slices"
	"sync"
	"testing"
	"time"

	celeris "github.com/useceleris/sdk-go-client"
)

// What arrives on a segment is decided by the server's membership, never by a
// listener (SEG-01).

// payloads returns the text of every delivery collected so far.
func payloads(collected *collector) []string {
	var texts []string

	for _, message := range collected.all() {
		texts = append(texts, string(message.payload))
	}

	return texts
} // end function payloads

func publishText(t *testing.T, channel *celeris.Channel, segmentID, body string) {
	t.Helper()

	if err := segment(t, channel, segmentID).Publish(t.Context(), []byte(body)); err != nil {
		t.Fatal(err)
	}
} // end function publishText

// publishAndAwait publishes and waits until the receiver's segment delivers
// that payload.
func publishAndAwait(t *testing.T, publisher, receiver *celeris.Channel, segmentID, body string) {
	t.Helper()

	arrived := make(chan struct{}, 1)
	remove := segment(t, receiver, segmentID).OnMessage(func(payload []byte, _ celeris.MessageMetadata) {
		if string(payload) == body {
			select {
			case arrived <- struct{}{}:
			default:
			}
		}
	})

	defer remove()

	publishText(t, publisher, segmentID, body)

	select {
	case <-arrived:
	case <-time.After(15 * time.Second):
		t.Fatalf("timed out waiting for %q on %s", body, segmentID)
	}
} // end function publishAndAwait

// confirmQuiet proves the connection was live before a negative check: a
// control message on the default segment, which always delivers, then time
// for a stray delivery to land.
func confirmQuiet(t *testing.T, publisher, receiver *celeris.Channel) {
	t.Helper()

	publishAndAwait(t, publisher, receiver, "default", "control")
	time.Sleep(2500 * time.Millisecond)
} // end function confirmQuiet

func pair(t *testing.T, label string, receiverClaims ...claim) (publisher, receiver *celeris.Channel) {
	t.Helper()

	reference := uniqueChannelReference(label)

	return connectedChannel(t, reference), connectedChannel(t, reference, receiverClaims...)
} // end function pair

func tokenPermission(read, write bool) claim {
	return claim{"token_permission", map[string]bool{"read": read, "write": write}}
} // end function tokenPermission

func assertPayloads(t *testing.T, collected *collector, want ...string) {
	t.Helper()

	if got := payloads(collected); !slices.Equal(got, want) {
		t.Fatalf("received %q, want %q", got, want)
	}
} // end function assertPayloads

func TestDeliversNothingToAListenerWithoutASubscription(t *testing.T) {
	publisher, receiver := pair(t, "listener-only")
	chat := collect(segment(t, receiver, "chat"))

	publishText(t, publisher, "chat", "unheard")
	confirmQuiet(t, publisher, receiver)

	assertPayloads(t, chat)
} // end function TestDeliversNothingToAListenerWithoutASubscription

func TestDeliversAfterASubscriptionMadeBeforeConnecting(t *testing.T) {
	reference := uniqueChannelReference("before-connect")
	publisher := connectedChannel(t, reference)
	receiver := newChannel(t, qualificationClient(t, websocketURL()), reference)
	chat := collect(segment(t, receiver, "chat"))
	subscribe(t, segment(t, receiver, "chat"))

	if err := receiver.Connect(t.Context()); err != nil {
		t.Fatalf("connect: %v", err)
	}

	time.Sleep(1500 * time.Millisecond)
	publishAndAwait(t, publisher, receiver, "chat", "hello")

	assertPayloads(t, chat, "hello")
} // end function TestDeliversAfterASubscriptionMadeBeforeConnecting

func TestDeliversAfterASubscriptionMadeOnceConnected(t *testing.T) {
	publisher, receiver := pair(t, "after-connect")
	chat := collect(segment(t, receiver, "chat"))
	subscribe(t, segment(t, receiver, "chat"))
	time.Sleep(1500 * time.Millisecond)

	publishAndAwait(t, publisher, receiver, "chat", "hello")

	assertPayloads(t, chat, "hello")
} // end function TestDeliversAfterASubscriptionMadeOnceConnected

func TestDeliversToAListenerAttachedAfterSubscribing(t *testing.T) {
	publisher, receiver := pair(t, "listener-later")
	subscribe(t, segment(t, receiver, "chat"))
	time.Sleep(1500 * time.Millisecond)
	chat := collect(segment(t, receiver, "chat"))

	publishAndAwait(t, publisher, receiver, "chat", "hello")

	assertPayloads(t, chat, "hello")
} // end function TestDeliversToAListenerAttachedAfterSubscribing

func TestStopsOnCancelAndResumesOnANewSubscription(t *testing.T) {
	publisher, receiver := pair(t, "rejoin")
	chat := collect(segment(t, receiver, "chat"))
	first := subscribe(t, segment(t, receiver, "chat"))
	time.Sleep(1500 * time.Millisecond)
	publishAndAwait(t, publisher, receiver, "chat", "one")

	first.Cancel()
	time.Sleep(1500 * time.Millisecond)
	publishText(t, publisher, "chat", "two")
	confirmQuiet(t, publisher, receiver)
	assertPayloads(t, chat, "one")

	subscribe(t, segment(t, receiver, "chat"))
	time.Sleep(1500 * time.Millisecond)
	publishAndAwait(t, publisher, receiver, "chat", "three")

	assertPayloads(t, chat, "one", "three")
} // end function TestStopsOnCancelAndResumesOnANewSubscription

func TestLeavesOnlyWhenTheLastHandleCancels(t *testing.T) {
	publisher, receiver := pair(t, "refcount")
	chat := collect(segment(t, receiver, "chat"))
	first := subscribe(t, segment(t, receiver, "chat"))
	second := subscribe(t, segment(t, receiver, "chat"))
	time.Sleep(1500 * time.Millisecond)

	first.Cancel()
	time.Sleep(1500 * time.Millisecond)
	publishAndAwait(t, publisher, receiver, "chat", "one")

	second.Cancel()
	time.Sleep(1500 * time.Millisecond)
	publishText(t, publisher, "chat", "two")
	confirmQuiet(t, publisher, receiver)

	assertPayloads(t, chat, "one")
} // end function TestLeavesOnlyWhenTheLastHandleCancels

func TestRoutesEachSegmentsMessagesToItsOwnListenersOnly(t *testing.T) {
	publisher, receiver := pair(t, "demux")
	alpha := collect(segment(t, receiver, "alpha"))
	beta := collect(segment(t, receiver, "beta"))
	subscribe(t, segment(t, receiver, "alpha"))
	betaMembership := subscribe(t, segment(t, receiver, "beta"))
	time.Sleep(1500 * time.Millisecond)

	publishAndAwait(t, publisher, receiver, "alpha", "a")
	publishAndAwait(t, publisher, receiver, "beta", "b")
	assertPayloads(t, alpha, "a")
	assertPayloads(t, beta, "b")

	betaMembership.Cancel()
	time.Sleep(1500 * time.Millisecond)
	publishText(t, publisher, "beta", "late")
	publishAndAwait(t, publisher, receiver, "alpha", "still")
	time.Sleep(2500 * time.Millisecond)

	assertPayloads(t, alpha, "a", "still")
	assertPayloads(t, beta, "b")
} // end function TestRoutesEachSegmentsMessagesToItsOwnListenersOnly

func TestKeepsOtherListenersAndTheSubscriptionWhenOneListenerStops(t *testing.T) {
	publisher, receiver := pair(t, "dispose")
	stopped := &collector{}
	stopListening := segment(t, receiver, "chat").OnMessage(func(payload []byte, metadata celeris.MessageMetadata) {
		stopped.mutex.Lock()
		stopped.deliveries = append(stopped.deliveries, delivery{payload, metadata})
		stopped.mutex.Unlock()
	})

	kept := collect(segment(t, receiver, "chat"))
	subscribe(t, segment(t, receiver, "chat"))
	time.Sleep(1500 * time.Millisecond)
	publishAndAwait(t, publisher, receiver, "chat", "one")

	stopListening()
	publishAndAwait(t, publisher, receiver, "chat", "two")

	assertPayloads(t, stopped, "one")
	assertPayloads(t, kept, "one", "two")
} // end function TestKeepsOtherListenersAndTheSubscriptionWhenOneListenerStops

// Membership the server grants without a message subscription (SEG-01).

func TestDeliversToASegmentJoinedByPublishing(t *testing.T) {
	publisher, receiver := pair(t, "publish-join")
	chat := collect(segment(t, receiver, "chat"))
	publishText(t, receiver, "chat", "joining")
	time.Sleep(1500 * time.Millisecond)

	publishAndAwait(t, publisher, receiver, "chat", "after")

	assertPayloads(t, chat, "after")
} // end function TestDeliversToASegmentJoinedByPublishing

// Watching presence is not membership: it neither joins nor holds.
func TestNeverJoinsOrHoldsASegmentForAPresenceSubscription(t *testing.T) {
	publisher, receiver := pair(t, "presence-watch")
	chat := collect(segment(t, receiver, "chat"))

	if _, err := segment(t, receiver, "chat").SubscribePresence(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(1500 * time.Millisecond)
	publishText(t, publisher, "chat", "unheard")
	confirmQuiet(t, publisher, receiver)
	assertPayloads(t, chat)

	messages := subscribe(t, segment(t, receiver, "chat"))
	time.Sleep(1500 * time.Millisecond)
	publishAndAwait(t, publisher, receiver, "chat", "one")

	messages.Cancel()
	time.Sleep(1500 * time.Millisecond)
	publishText(t, publisher, "chat", "two")
	confirmQuiet(t, publisher, receiver)

	assertPayloads(t, chat, "one")
} // end function TestNeverJoinsOrHoldsASegmentForAPresenceSubscription

func TestLeavesASegmentJoinedByPublishingOnTheLastCancel(t *testing.T) {
	publisher, receiver := pair(t, "publish-leave")
	chat := collect(segment(t, receiver, "chat"))
	publishText(t, receiver, "chat", "joining")
	membership := subscribe(t, segment(t, receiver, "chat"))
	time.Sleep(1500 * time.Millisecond)

	membership.Cancel()
	time.Sleep(1500 * time.Millisecond)
	publishText(t, publisher, "chat", "late")
	confirmQuiet(t, publisher, receiver)

	assertPayloads(t, chat)
} // end function TestLeavesASegmentJoinedByPublishingOnTheLastCancel

// Read access is checked at join, so a token's permissions decide whether
// publishing makes a segment deliver.

func TestReceivesAfterPublishingWithAReadWriteToken(t *testing.T) {
	publisher, receiver := pair(t, "publish-read-write", tokenPermission(true, true))
	chat := collect(segment(t, receiver, "chat"))
	publishText(t, receiver, "chat", "joining")
	time.Sleep(1500 * time.Millisecond)

	publishAndAwait(t, publisher, receiver, "chat", "after")

	assertPayloads(t, chat, "after")
} // end function TestReceivesAfterPublishingWithAReadWriteToken

func TestReceivesNothingAfterPublishingWithAWriteOnlyToken(t *testing.T) {
	publisher, receiver := pair(t, "publish-write-only", tokenPermission(false, true))
	chat := collect(segment(t, receiver, "chat"))
	subscribe(t, segment(t, publisher, "chat"))
	time.Sleep(1500 * time.Millisecond)

	// The publish lands, which proves the write-only connection is up.
	publishAndAwait(t, receiver, publisher, "chat", "joining")
	publishText(t, publisher, "chat", "unheard")
	time.Sleep(2500 * time.Millisecond)

	assertPayloads(t, chat)
} // end function TestReceivesNothingAfterPublishingWithAWriteOnlyToken

func TestRefusesAReadOnlyTokensPublishAndDeliversOnceSubscribed(t *testing.T) {
	publisher, receiver := pair(t, "publish-read-only", tokenPermission(true, false))
	chat := collect(segment(t, receiver, "chat"))

	publishText(t, receiver, "chat", "refused")
	_ = nextError(t, receiver, serverErrorOfType(celeris.PermissionDeniedError), "the publish denial", 15*time.Second)
	publishText(t, publisher, "chat", "unheard")
	confirmQuiet(t, publisher, receiver)
	assertPayloads(t, chat)

	subscribe(t, segment(t, receiver, "chat"))
	time.Sleep(1500 * time.Millisecond)
	publishAndAwait(t, publisher, receiver, "chat", "heard")

	assertPayloads(t, chat, "heard")
} // end function TestRefusesAReadOnlyTokensPublishAndDeliversOnceSubscribed

// One channel is one WebSocket; its segments share it (SEG-01).
func TestMultiplexesAChannelsSegmentsOverOneConnection(t *testing.T) {
	reference := uniqueChannelReference("multiplex")
	publisher := connectedChannel(t, reference)
	receiver := connectedChannel(t, reference)
	subscribe(t, segment(t, receiver, "alpha"))
	subscribe(t, segment(t, receiver, "beta"))
	time.Sleep(3 * time.Second)

	connectionsIn := func(segmentID string) []string {
		page, err := segment(t, publisher, segmentID).PresenceList(t.Context(), 1, 25)

		if err != nil {
			t.Fatal(err)
		}

		var connectionIDs []string

		for _, connection := range page.Connections {
			connectionIDs = append(connectionIDs, connection.ConnectionID)
		}

		return connectionIDs
	}

	alpha := connectionsIn("alpha")

	if beta := connectionsIn("beta"); len(alpha) != 1 || !slices.Equal(beta, alpha) {
		t.Fatalf("alpha %q, beta %q, want one shared connection", alpha, beta)
	}

	second := connectedChannel(t, reference)
	subscribe(t, segment(t, second, "alpha"))
	time.Sleep(3 * time.Second)

	if both := connectionsIn("alpha"); len(both) != 2 || both[0] == both[1] {
		t.Fatalf("alpha %q, want two distinct connections", both)
	}
} // end function TestMultiplexesAChannelsSegmentsOverOneConnection

func TestChannelListenerCatchesDeliveriesNoSegmentListenerAskedFor(t *testing.T) {
	publisher, receiver := pair(t, "channel-listener")
	var mutex sync.Mutex
	var seen []string
	both := make(chan struct{})
	receiver.Events().OnMessage(func(payload []byte, metadata celeris.MessageMetadata) {
		mutex.Lock()
		defer mutex.Unlock()

		seen = append(seen, metadata.SegmentID+":"+string(payload))

		if len(seen) == 2 {
			close(both)
		}
	})

	publishText(t, receiver, "joined", "joining")
	time.Sleep(1500 * time.Millisecond)

	publishText(t, publisher, "joined", "x")
	publishText(t, publisher, "default", "y")

	select {
	case <-both:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for both channel-wide deliveries")
	}

	mutex.Lock()
	got := slices.Sorted(slices.Values(seen))
	mutex.Unlock()

	if want := []string{"default:y", "joined:x"}; !slices.Equal(got, want) {
		t.Fatalf("seen %q, want %q", got, want)
	}
} // end function TestChannelListenerCatchesDeliveriesNoSegmentListenerAskedFor

func TestRemovesOneChannelListenerAndLeavesEveryOtherListener(t *testing.T) {
	publisher, receiver := pair(t, "channel-listener-remove")
	removed := &collector{}
	removeChannelListener := receiver.Events().OnMessage(removed.record)
	kept := &collector{}
	keptArrivals := make(chan string, 8)
	receiver.Events().OnMessage(func(payload []byte, metadata celeris.MessageMetadata) {
		kept.record(payload, metadata)

		select {
		case keptArrivals <- string(payload):
		default:
		}
	})

	chat := collect(segment(t, receiver, "chat"))
	subscribe(t, segment(t, receiver, "chat"))
	time.Sleep(1500 * time.Millisecond)

	// Channel listeners run after the segment listeners, so wait for the last
	// channel listener before the next step.
	publishAndAwaitChannel := func(body string) {
		publishText(t, publisher, "chat", body)

		select {
		case got := <-keptArrivals:
			if got != body {
				t.Fatalf("received %q, want %q", got, body)
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("timed out waiting for %q on the channel listener", body)
		}
	}

	publishAndAwaitChannel("one")

	removeChannelListener()
	publishAndAwaitChannel("two")

	assertPayloads(t, removed, "one")
	assertPayloads(t, kept, "one", "two")
	assertPayloads(t, chat, "one", "two")
} // end function TestRemovesOneChannelListenerAndLeavesEveryOtherListener
