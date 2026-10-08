package live

import (
	"errors"
	"slices"
	"testing"
	"time"

	celeris "github.com/useceleris/sdk-go-client"
)

type segmentPermission struct {
	SegmentID string `json:"segment_id"`
	Read      bool   `json:"read"`
	Write     bool   `json:"write"`
} // end struct segmentPermission

// segmentPermissions is a token that can read "readonly", write "writeonly",
// and nothing else: no other segment, and not "default" either.
var segmentPermissions = []claim{
	{"reference", "limited"},
	{"token_permission", []segmentPermission{
		{SegmentID: "readonly", Read: true, Write: false},
		{SegmentID: "writeonly", Read: false, Write: true},
	}},
}

// awaitDenial registers at once and returns the wait for a permission denial
// that names this command and segment.
func awaitDenial(t *testing.T, channel *celeris.Channel, subType, segmentID string) func() {
	t.Helper()

	wait := awaitEvent(t, channel.Events().OnError, func(err error) bool {
		denial, ok := errors.AsType[*celeris.ServerError](err)

		return ok && denial.Type == celeris.PermissionDeniedError && denial.SubType == subType && denial.Resource == segmentID
	}, "a "+subType+" denial for "+segmentID, 15*time.Second)

	return func() {
		t.Helper()
		_ = wait()
	}
} // end function awaitDenial

func TestLetsAReadOnlySegmentReceiveAndRefusesItsPublish(t *testing.T) {
	reference := uniqueChannelReference("perm-read")
	publisher := connectedChannel(t, reference)
	limited := connectedChannel(t, reference, segmentPermissions...)
	readonly := segment(t, limited, "readonly")
	received := collect(readonly)
	subscribe(t, readonly)
	settle()

	delivered := awaitMessage(t, readonly, withBody("hello"), "the read-only delivery", 15*time.Second)
	publishText(t, publisher, "readonly", "hello")
	delivered()

	refused := awaitDenial(t, limited, "PUB", "readonly")
	publishText(t, limited, "readonly", "refused")
	refused()

	assertPayloads(t, received, "hello")

	if state := limited.State(); state != celeris.StateConnected {
		t.Fatalf("state %s", state)
	}
} // end function TestLetsAReadOnlySegmentReceiveAndRefusesItsPublish

func TestLetsAWriteOnlySegmentPublishAndReceiveNothing(t *testing.T) {
	reference := uniqueChannelReference("perm-write")
	reader := connectedChannel(t, reference)
	limited := connectedChannel(t, reference, segmentPermissions...)
	writeonly := segment(t, limited, "writeonly")
	received := collect(writeonly)
	subscribe(t, segment(t, reader, "writeonly"))
	subscribe(t, writeonly)
	settle()

	publishAndAwait(t, limited, reader, "writeonly", "from-limited")
	publishText(t, reader, "writeonly", "unheard")
	time.Sleep(2500 * time.Millisecond)

	assertPayloads(t, received)
} // end function TestLetsAWriteOnlySegmentPublishAndReceiveNothing

func TestRefusesPresenceOnASegmentWithoutReadAccess(t *testing.T) {
	limited := connectedChannel(t, uniqueChannelReference("perm-presence"), segmentPermissions...)

	refusedWatch := awaitDenial(t, limited, "PRES_SUB", "writeonly")
	subscribePresence(t, segment(t, limited, "writeonly"))
	refusedWatch()

	_, err := segment(t, limited, "writeonly").PresenceList(t.Context(), 1, 10)

	if rejection, ok := errors.AsType[*celeris.ServerError](err); !ok || rejection.Type != celeris.PermissionDeniedError || rejection.SubType != "PRES_LIST" {
		t.Fatalf("got %v", err)
	}

	page, err := segment(t, limited, "readonly").PresenceList(t.Context(), 1, 10)

	if err != nil || len(page.Connections) != 0 {
		t.Fatalf("page %+v, %v", page, err)
	}
} // end function TestRefusesPresenceOnASegmentWithoutReadAccess

func TestRefusesEveryCommandOnASegmentTheTokenDoesNotList(t *testing.T) {
	limited := connectedChannel(t, uniqueChannelReference("perm-unlisted"), segmentPermissions...)
	secret := segment(t, limited, "secret")

	refusedJoin := awaitDenial(t, limited, "SUB", "secret")
	subscribe(t, secret)
	refusedJoin()

	refusedPublish := awaitDenial(t, limited, "PUB", "secret")
	publishText(t, limited, "secret", "x")
	refusedPublish()

	refusedWatch := awaitDenial(t, limited, "PRES_SUB", "secret")
	subscribePresence(t, secret)
	refusedWatch()

	if state := limited.State(); state != celeris.StateConnected {
		t.Fatalf("state %s", state)
	}
} // end function TestRefusesEveryCommandOnASegmentTheTokenDoesNotList

func TestGivesAnUnlistedDefaultSegmentNoReadAndNoWriteAccess(t *testing.T) {
	reference := uniqueChannelReference("perm-default")
	publisher := connectedChannel(t, reference)
	limited := connectedChannel(t, reference, segmentPermissions...)
	lobby := collect(limited.DefaultSegment())
	readonly := segment(t, limited, "readonly")
	received := collect(readonly)
	subscribe(t, readonly)
	settle()

	publishText(t, publisher, "default", "unheard")
	publishAndAwait(t, publisher, limited, "readonly", "control")

	refused := awaitDenial(t, limited, "PUB", "default")
	publishText(t, limited, "default", "refused")
	refused()
	settle()

	assertPayloads(t, lobby)
	assertPayloads(t, received, "control")
} // end function TestGivesAnUnlistedDefaultSegmentNoReadAndNoWriteAccess

func TestShowsTheReferenceClaimInMessageMetadataPresenceEventsAndPresenceLists(t *testing.T) {
	reference := uniqueChannelReference("token-reference")
	watcher := connectedChannel(t, reference, claim{"reference", "watcher"})
	alice := connectedChannel(t, reference, claim{"reference", "alice"})
	room := segment(t, watcher, "room")
	subscribe(t, room)
	subscribePresence(t, room)
	settle()

	joined := awaitPresence(t, room, func(event celeris.PresenceEvent) bool {
		return event.Joined && event.TokenReference == "alice"
	}, "alice's join", 15*time.Second)

	subscribe(t, segment(t, alice, "room"))
	joined()

	message := awaitMessage(t, room, withBody("hi"), "alice's message", 15*time.Second)
	publishText(t, alice, "room", "hi")

	if got := message().metadata.TokenReference; got != "alice" {
		t.Fatalf("message token reference %q", got)
	}

	page, err := room.PresenceList(t.Context(), 1, 10)

	if err != nil {
		t.Fatal(err)
	}

	var tokenReferences []string

	for _, connection := range page.Connections {
		tokenReferences = append(tokenReferences, connection.TokenReference)
	}

	slices.Sort(tokenReferences)

	if want := []string{"alice", "watcher"}; !slices.Equal(tokenReferences, want) {
		t.Fatalf("listed %q, want %q", tokenReferences, want)
	}
} // end function TestShowsTheReferenceClaimInMessageMetadataPresenceEventsAndPresenceLists
