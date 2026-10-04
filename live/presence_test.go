package live

import (
	"errors"
	"reflect"
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
}

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
}

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

	if got := []any{serverError.Type, serverError.SubType, serverError.Resource}; !reflect.DeepEqual(got, []any{celeris.PermissionDeniedError, "PRES_LIST", "1"}) {
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
}

// PRES_SUB force-joins the segment; cancelling presence keeps the membership.
func TestStopsPresenceAfterCancellationWhileMembershipHolds(t *testing.T) {
	reference := uniqueChannelReference("unwatch")
	watcher := connectedChannel(t, reference)
	room := segment(t, watcher, "room")
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

	nextMessage(t, room, func(message delivery) bool { return string(message.payload) == "still-member" }, "delivery proving persistent membership", 15*time.Second)

	select {
	case event := <-events:
		t.Fatalf("presence after cancellation: %+v", event)
	default:
	}
}
