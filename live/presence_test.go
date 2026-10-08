package live

import (
	"errors"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	celeris "github.com/useceleris/sdk-go-client"
)

func TestDeliversTypedJoinAndLeaveNotificationsToWatchers(t *testing.T) {
	reference := uniqueChannelReference("watch")
	watcher := connectedChannel(t, reference)
	watched := segment(t, watcher, "room")

	if _, err := watched.SubscribePresence(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(1500 * time.Millisecond)
	joins := make(chan celeris.PresenceEvent, 8)
	leaves := make(chan celeris.PresenceEvent, 8)
	watched.OnPresence(func(event celeris.PresenceEvent) {
		if event.Joined {
			joins <- event
		} else {
			leaves <- event
		}
	})

	actor := connectedChannel(t, reference)
	subscribe(t, segment(t, actor, "room"))

	var join celeris.PresenceEvent

	select {
	case join = <-joins:
	case <-time.After(15 * time.Second):
		t.Fatal("no join notification")
	}

	if join.SegmentID != "room" || join.TokenReference == "" || join.ConnectionID == "" || join.Timestamp <= 0 {
		t.Fatalf("join %+v", join)
	}

	actor.Close()

	for {
		select {
		case leave := <-leaves:
			if leave.ConnectionID != join.ConnectionID {
				continue
			}

			if leave.SegmentID != "room" || leave.TokenReference != join.TokenReference {
				t.Fatalf("leave %+v", leave)
			}

			return
		case <-time.After(15 * time.Second):
			t.Fatal("no leave notification")
		}
	}
} // end function TestDeliversTypedJoinAndLeaveNotificationsToWatchers

func TestPagesPresenceSnapshotsWithRawMetadata(t *testing.T) {
	reference := uniqueChannelReference("plist")
	first := connectedChannel(t, reference)
	second := connectedChannel(t, reference)
	subscribe(t, segment(t, first, "room"))
	subscribe(t, segment(t, second, "room"))
	time.Sleep(2500 * time.Millisecond)

	page, err := segment(t, first, "room").PresenceList(t.Context(), 1, 10)

	if err != nil {
		t.Fatal(err)
	}

	if page.SegmentID != "room" || page.Total < 2 || len(page.Connections) < 2 || page.From != 1 {
		t.Fatalf("page %+v", page)
	}

	for _, connection := range page.Connections {
		if connection.ConnectionID == "" || connection.Timestamp <= 0 {
			t.Fatalf("connection %+v", connection)
		}
	}

	beyond, err := segment(t, first, "room").PresenceList(t.Context(), 50, 10)

	if err != nil || len(beyond.Connections) != 0 || beyond.From <= beyond.To {
		t.Fatalf("beyond %+v, %v", beyond, err)
	}
} // end function TestPagesPresenceSnapshotsWithRawMetadata

// Presence needs read access. The denial names the query by its request id,
// so the caller hears it at once instead of waiting out the deadline.
func TestRejectsAWriteOnlyTokensPresenceQueryAtOnce(t *testing.T) {
	writeOnly := connectedChannel(t, uniqueChannelReference("pdeny"), claim{"token_permission", map[string]bool{"read": false, "write": true}})
	reported := make(chan error, 4)
	writeOnly.Events().OnError(func(err error) { reported <- err })
	started := time.Now()

	_, err := segment(t, writeOnly, "room").PresenceList(t.Context(), 1, 10)

	serverError, ok := errors.AsType[*celeris.ServerError](err)

	if !ok || time.Since(started) > 2*time.Second {
		t.Fatalf("got %v after %v", err, time.Since(started))
	}

	got := []any{serverError.Type, serverError.SubType, serverError.Resource}

	if !reflect.DeepEqual(got, []any{celeris.PermissionDeniedError, "PRES_LIST", "1"}) {
		t.Fatalf("server error %v", got)
	}

	select {
	case err := <-reported:
		t.Fatalf("also reported to OnError: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	if writeOnly.State() != celeris.StateConnected {
		t.Fatalf("state %s", writeOnly.State())
	}
} // end function TestRejectsAWriteOnlyTokensPresenceQueryAtOnce

// Watching presence is not membership: a cancelled presence subscription
// stops its events while the message subscription keeps delivering.
func TestStopsPresenceAfterCancellationWhileTheSubscriptionStays(t *testing.T) {
	reference := uniqueChannelReference("unwatch")
	watcher := connectedChannel(t, reference)
	room := segment(t, watcher, "room")
	subscribe(t, room)
	watching, err := room.SubscribePresence()

	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1500 * time.Millisecond)
	watching.Cancel()
	time.Sleep(1500 * time.Millisecond)

	events := make(chan celeris.PresenceEvent, 8)
	room.OnPresence(func(event celeris.PresenceEvent) { events <- event })
	actor := connectedChannel(t, reference)
	subscribe(t, segment(t, actor, "room"))

	if err := segment(t, actor, "room").Publish(t.Context(), []byte("still-member")); err != nil {
		t.Fatal(err)
	}

	nextMessage(t, room, func(message delivery) bool { return string(message.payload) == "still-member" }, "delivery proving the subscription stayed", 15*time.Second)

	select {
	case event := <-events:
		t.Fatalf("presence after cancellation: %+v", event)
	default:
	}
} // end function TestStopsPresenceAfterCancellationWhileTheSubscriptionStays

// presenceRecorder records every presence event on a segment.
type presenceRecorder struct {
	mutex  sync.Mutex
	events []celeris.PresenceEvent
} // end struct presenceRecorder

func recordPresence(handle *celeris.Segment) *presenceRecorder {
	recorded := &presenceRecorder{}
	handle.OnPresence(func(event celeris.PresenceEvent) {
		recorded.mutex.Lock()
		recorded.events = append(recorded.events, event)
		recorded.mutex.Unlock()
	})

	return recorded
} // end function recordPresence

func (recorded *presenceRecorder) all() []celeris.PresenceEvent {
	recorded.mutex.Lock()
	defer recorded.mutex.Unlock()

	return slices.Clone(recorded.events)
} // end method all

func subscribePresence(t *testing.T, handle *celeris.Segment) {
	t.Helper()

	if _, err := handle.SubscribePresence(); err != nil {
		t.Fatal(err)
	}
} // end function subscribePresence

func awaitPresence(t *testing.T, handle *celeris.Segment, predicate func(celeris.PresenceEvent) bool, description string, timeout time.Duration) func() celeris.PresenceEvent {
	t.Helper()

	return awaitEvent(t, handle.OnPresence, predicate, description, timeout)
} // end function awaitPresence

func TestHidesAConnectionsOwnJoinAndShowsItToASiblingOfTheSameToken(t *testing.T) {
	reference := uniqueChannelReference("presence-self")
	first := connectedChannel(t, reference, claim{"reference", "alice"})
	second := connectedChannel(t, reference, claim{"reference", "alice"})
	firstRoom := segment(t, first, "room")
	secondRoom := segment(t, second, "room")
	firstSaw := recordPresence(firstRoom)
	secondSaw := recordPresence(secondRoom)
	subscribePresence(t, firstRoom)
	subscribePresence(t, secondRoom)
	settle()

	siblingJoin := awaitPresence(t, secondRoom, func(event celeris.PresenceEvent) bool { return event.Joined }, "the sibling's join", 15*time.Second)
	subscribe(t, firstRoom)
	firstJoin := siblingJoin()
	settle()

	if firstJoin.TokenReference != "alice" {
		t.Fatalf("join %+v", firstJoin)
	}

	if events := firstSaw.all(); len(events) != 0 {
		t.Fatalf("the joining connection saw %+v", events)
	}

	if events := secondSaw.all(); len(events) != 1 {
		t.Fatalf("the sibling saw %+v, want one join", events)
	}
} // end function TestHidesAConnectionsOwnJoinAndShowsItToASiblingOfTheSameToken

// One user with three tabs: each connection of one token reference is listed
// with its own connection id.
func TestListsEachConnectionOfOneTokenReferenceWithItsOwnConnectionID(t *testing.T) {
	reference := uniqueChannelReference("presence-same-reference")
	watcher := connectedChannel(t, reference, claim{"reference", "watcher"})
	room := segment(t, watcher, "room")
	joins := recordPresence(room)
	subscribePresence(t, room)
	settle()

	tabs := []*celeris.Channel{
		connectedChannel(t, reference, claim{"reference", "user_1"}),
		connectedChannel(t, reference, claim{"reference", "user_1"}),
		connectedChannel(t, reference, claim{"reference", "user_1"}),
	}

	for _, tab := range tabs {
		subscribe(t, segment(t, tab, "room"))
	}

	eventually(t, func() bool { return len(joins.all()) >= 3 }, "three joins", 20*time.Second)

	var joinIDs []string

	for _, event := range joins.all() {
		if !event.Joined || event.TokenReference != "user_1" {
			t.Fatalf("event %+v, want a join of user_1", event)
		}

		joinIDs = append(joinIDs, event.ConnectionID)
	}

	if distinct := slices.Compact(slices.Sorted(slices.Values(joinIDs))); len(distinct) != 3 {
		t.Fatalf("connection ids %v, want three distinct ones", joinIDs)
	}

	// The largest page size, 100, lists all three.
	assertListed(t, room, joinIDs)

	left := awaitPresence(t, room, func(event celeris.PresenceEvent) bool { return !event.Joined }, "the leave of one tab", 20*time.Second)
	tabs[0].Close()
	leave := left()

	if leave.TokenReference != "user_1" || !slices.Contains(joinIDs, leave.ConnectionID) {
		t.Fatalf("leave %+v, want one of %v", leave, joinIDs)
	}

	settle()
	assertListed(t, room, slices.DeleteFunc(slices.Clone(joinIDs), func(connectionID string) bool {
		return connectionID == leave.ConnectionID
	}))
} // end function TestListsEachConnectionOfOneTokenReferenceWithItsOwnConnectionID

// assertListed checks that the segment's first page of 100 lists exactly the
// given connections, each of token reference user_1.
func assertListed(t *testing.T, handle *celeris.Segment, connectionIDs []string) {
	t.Helper()

	page, err := handle.PresenceList(t.Context(), 1, 100)

	if err != nil {
		t.Fatal(err)
	}

	var listed []string

	for _, connection := range page.Connections {
		if connection.TokenReference != "user_1" {
			t.Fatalf("listed %+v, want user_1", connection)
		}

		listed = append(listed, connection.ConnectionID)
	}

	if int(page.Total) != len(connectionIDs) || !slices.Equal(slices.Sorted(slices.Values(listed)), slices.Sorted(slices.Values(connectionIDs))) {
		t.Fatalf("total %d, listed %v, want %v", page.Total, listed, connectionIDs)
	}
} // end function assertListed

func TestSendsALeaveWhenAMemberUnsubscribesAndStaysConnected(t *testing.T) {
	reference := uniqueChannelReference("presence-unsubscribe")
	watcher := connectedChannel(t, reference)
	actor := connectedChannel(t, reference, claim{"reference", "actor"})
	room := segment(t, watcher, "room")
	subscribePresence(t, room)
	settle()

	joined := awaitPresence(t, room, func(event celeris.PresenceEvent) bool {
		return event.Joined && event.TokenReference == "actor"
	}, "the actor's join", 15*time.Second)

	membership := subscribe(t, segment(t, actor, "room"))
	join := joined()
	left := awaitPresence(t, room, func(event celeris.PresenceEvent) bool {
		return !event.Joined && event.TokenReference == "actor"
	}, "the actor's leave", 15*time.Second)

	membership.Cancel()
	leave := left()

	if leave.ConnectionID != join.ConnectionID {
		t.Fatalf("leave from %q, join from %q", leave.ConnectionID, join.ConnectionID)
	}

	if state := actor.State(); state != celeris.StateConnected {
		t.Fatalf("state %s", state)
	}
} // end function TestSendsALeaveWhenAMemberUnsubscribesAndStaysConnected

func TestAnnouncesDefaultSegmentJoinsOnConnectAndLeavesOnCloseAndListsEveryConnection(t *testing.T) {
	reference := uniqueChannelReference("presence-default")
	watcher := connectedChannel(t, reference, claim{"reference", "watcher"})
	lobby := watcher.DefaultSegment()
	subscribePresence(t, lobby)
	settle()

	joined := awaitPresence(t, lobby, func(event celeris.PresenceEvent) bool {
		return event.Joined && event.TokenReference == "late"
	}, "the join from connect", 15*time.Second)

	late := connectedChannel(t, reference, claim{"reference", "late"})
	join := joined()

	if join.SegmentID != "default" {
		t.Fatalf("join %+v", join)
	}

	page, err := lobby.PresenceList(t.Context(), 1, 10)

	if err != nil {
		t.Fatal(err)
	}

	var tokenReferences []string

	for _, connection := range page.Connections {
		tokenReferences = append(tokenReferences, connection.TokenReference)
	}

	slices.Sort(tokenReferences)

	if want := []string{"late", "watcher"}; !slices.Equal(tokenReferences, want) {
		t.Fatalf("listed %q, want %q", tokenReferences, want)
	}

	left := awaitPresence(t, lobby, func(event celeris.PresenceEvent) bool {
		return !event.Joined && event.ConnectionID == join.ConnectionID
	}, "the leave from close", 15*time.Second)

	late.Close()
	left()
} // end function TestAnnouncesDefaultSegmentJoinsOnConnectAndLeavesOnCloseAndListsEveryConnection

func TestPagesThroughSeveralFullPagesAndPastTheEnd(t *testing.T) {
	reference := uniqueChannelReference("presence-pages")
	var members []*celeris.Channel

	for _, tokenReference := range []string{"m1", "m2", "m3"} {
		members = append(members, connectedChannel(t, reference, claim{"reference", tokenReference}))
	}

	for _, member := range members {
		subscribe(t, segment(t, member, "room"))
	}

	time.Sleep(3 * time.Second)

	var pages []celeris.PresencePage

	for page := int32(1); page <= 4; page++ {
		result, err := segment(t, members[0], "room").PresenceList(t.Context(), page, 1)

		if err != nil {
			t.Fatal(err)
		}

		pages = append(pages, result)
	}

	connectionIDs := map[string]bool{}
	var tokenReferences []string

	for index, page := range pages[:3] {
		number := int32(index + 1)

		if page.Total != 3 || page.PerPage != 1 || page.CurrentPage != number || page.From != number || page.To != number || len(page.Connections) != 1 {
			t.Fatalf("page %d: %+v", number, page)
		}

		connectionIDs[page.Connections[0].ConnectionID] = true
		tokenReferences = append(tokenReferences, page.Connections[0].TokenReference)
	}

	if beyond := pages[3]; beyond.Total != 3 || beyond.From != 4 || beyond.To != 3 || len(beyond.Connections) != 0 {
		t.Fatalf("page 4: %+v", beyond)
	}

	slices.Sort(tokenReferences)

	if len(connectionIDs) != 3 || !slices.Equal(tokenReferences, []string{"m1", "m2", "m3"}) {
		t.Fatalf("connections %v, token references %q", connectionIDs, tokenReferences)
	}
} // end function TestPagesThroughSeveralFullPagesAndPastTheEnd
