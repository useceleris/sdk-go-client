package celeris

import (
	"context"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

type presenceResponse struct {
	segmentID   string
	requestID   string
	total       int
	perPage     int
	currentPage int
	from        int
	to          int
	connections []PresenceConnection
} // end struct presenceResponse

func presenceResponseFrame(response presenceResponse) string {
	frame := "@PRES_LIST_RESPONSE\n+" + response.segmentID + "\n" + bulk(response.requestID)

	for _, figure := range []int{response.total, response.perPage, response.currentPage, response.from, response.to} {
		frame += ";" + strconv.Itoa(figure) + "\n"
	}

	frame += "*" + strconv.Itoa(len(response.connections)) + "\n"

	for _, connection := range response.connections {
		frame += "*3\n+" + connection.TokenReference + "\n+" + connection.ConnectionID + "\n:" + strconv.FormatInt(connection.Timestamp, 10) + "\n"
	}

	return frame
} // end function presenceResponseFrame

// response is a one-connection page answering request id.
func response(requestID string) presenceResponse {
	return presenceResponse{
		segmentID:   "chat",
		requestID:   requestID,
		total:       1,
		perPage:     25,
		currentPage: 1,
		from:        1,
		to:          1,
		connections: []PresenceConnection{{TokenReference: "user", ConnectionID: "connection-1", Timestamp: 123}},
	}
} // end function response

func presenceErrorFrame(errorType, requestID string) string {
	return errorFrame(errorType, "failed", "PRES_LIST", bulk(requestID))
} // end function presenceErrorFrame

func presenceNotifyFrame(segmentID string, joined bool, timestamp int) string {
	event := "0"

	if joined {
		event = "1"
	}

	return "@PRES_NOTIFY\n+" + segmentID + "\n+user\n+connection-1\n;" + event + "\n:" + strconv.Itoa(timestamp) + "\n"
} // end function presenceNotifyFrame

type queryOutcome struct {
	page PresencePage
	err  error
} // end struct queryOutcome

// queryAsync starts a presence query from its own goroutine and waits until
// it is sent or refused.
func queryAsync(ctx context.Context, handle *Segment, page, perPage int32) <-chan queryOutcome {
	result := make(chan queryOutcome, 1)

	go func() {
		presencePage, err := handle.PresenceList(ctx, page, perPage)
		result <- queryOutcome{presencePage, err}
	}()

	synctest.Wait()

	return result
} // end function queryAsync

func TestPresenceInterestsShareOneCount(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		first := subscribePresence(t, segment(t, channel, "chat"))
		second := subscribePresence(t, segment(t, channel, "chat"))
		first.Cancel()
		first.Cancel()
		synctest.Wait()
		assertCommands(t, socket, "@PRES_SUB\n$4\nchat\n")

		second.Cancel()
		synctest.Wait()
		assertCommands(t, socket, "@PRES_SUB\n$4\nchat\n", "@PRES_UNSUB\n$4\nchat\n")
	})
} // end function TestPresenceInterestsShareOneCount

func TestDefaultSegmentTakesPresenceCommands(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		subscribePresence(t, channel.DefaultSegment()).Cancel()
		synctest.Wait()
		assertCommands(t, socket, "@PRES_SUB\n$7\ndefault\n", "@PRES_UNSUB\n$7\ndefault\n")
	})
} // end function TestDefaultSegmentTakesPresenceCommands

func TestMessageCancelSendsUnsubscribeWhilePresenceIsHeld(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		messages := subscribe(t, segment(t, channel, "chat"))
		presence := subscribePresence(t, segment(t, channel, "chat"))
		messages.Cancel()
		synctest.Wait()
		assertCommands(t, socket, "@SUB\n$4\nchat\n", "@PRES_SUB\n$4\nchat\n", "@UNSUB\n$4\nchat\n")

		presence.Cancel()
		synctest.Wait()
		assertCommands(t, socket, "@SUB\n$4\nchat\n", "@PRES_SUB\n$4\nchat\n", "@UNSUB\n$4\nchat\n", "@PRES_UNSUB\n$4\nchat\n")
	})
} // end function TestMessageCancelSendsUnsubscribeWhilePresenceIsHeld

func TestPresenceInterestOnAClosedChannelFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _ := newTestChannel(t)
		channel.Close()
		_, err := segment(t, channel, "chat").SubscribePresence()
		assertCode(t, err, ErrNotConnected)
	})
} // end function TestPresenceInterestOnAClosedChannelFails

func TestPresenceQueryResolvesWithRawMetadata(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		pending := queryAsync(t.Context(), segment(t, channel, "chat"), 1, 25)
		assertCommands(t, socket, "@PRES_LIST\n$4\nchat\n;1\n;25\n$1\n1\n")

		connections := []PresenceConnection{
			{TokenReference: "user", ConnectionID: "connection-1", Timestamp: 123},
			{TokenReference: "user", ConnectionID: "connection-2", Timestamp: 456},
		}
		reply := response("1")
		reply.total, reply.to, reply.connections = 2, 2, connections
		socket.receive(presenceResponseFrame(reply))

		outcome := <-pending
		want := PresencePage{SegmentID: "chat", Total: 2, PerPage: 25, CurrentPage: 1, From: 1, To: 2, Connections: connections}

		if outcome.err != nil || !reflect.DeepEqual(outcome.page, want) {
			t.Fatalf("got %+v and %v", outcome.page, outcome.err)
		}
	})
} // end function TestPresenceQueryResolvesWithRawMetadata

func TestPresencePagePastTheEndKeepsFromAboveTo(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		pending := queryAsync(t.Context(), segment(t, channel, "chat"), 2, 25)
		reply := response("1")
		reply.currentPage, reply.from, reply.to, reply.connections = 2, 26, 1, nil
		socket.receive(presenceResponseFrame(reply))

		if outcome := <-pending; outcome.err != nil || outcome.page.From != 26 || outcome.page.To != 1 || len(outcome.page.Connections) != 0 {
			t.Fatalf("got %+v and %v", outcome.page, outcome.err)
		}
	})
} // end function TestPresencePagePastTheEndKeepsFromAboveTo

func TestOnePresenceQueryAtATime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		first := queryAsync(t.Context(), segment(t, channel, "chat"), 1, 25)

		_, err := segment(t, channel, "other").PresenceList(t.Context(), 1, 25)
		assertCode(t, err, ErrOperationInProgress)

		socket.receive(presenceResponseFrame(response("1")))

		if outcome := <-first; outcome.err != nil || outcome.page.SegmentID != "chat" {
			t.Fatalf("got %+v and %v", outcome.page, outcome.err)
		}
	})
} // end function TestOnePresenceQueryAtATime

func TestPresenceBoundsAreValidatedAndSpendTheirID(t *testing.T) {
	cases := []struct{ page, perPage int32 }{{0, 25}, {-1, 25}, {1, 0}, {1, 101}}

	for _, bounds := range cases {
		synctest.Test(t, func(t *testing.T) {
			channel, _, socket := connectTestChannel(t)
			_, err := segment(t, channel, "chat").PresenceList(t.Context(), bounds.page, bounds.perPage)
			assertCode(t, err, ErrConfiguration)
			assertCommands(t, socket)

			recovered := queryAsync(t.Context(), segment(t, channel, "chat"), 1, 25)
			socket.receive(presenceResponseFrame(response("2")))

			if outcome := <-recovered; outcome.err != nil {
				t.Fatal(outcome.err)
			}
		})
	}
} // end function TestPresenceBoundsAreValidatedAndSpendTheirID

func TestPresenceQueryFailsWhileNotConnected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		idle, _ := newTestChannel(t)
		_, err := segment(t, idle, "chat").PresenceList(t.Context(), 1, 25)
		assertCode(t, err, ErrNotConnected)

		channel, server, socket := connectTestChannel(t)
		server.set(func(server *fakeServer) { server.blockDials = true })
		socket.drop()
		synctest.Wait()

		_, err = segment(t, channel, "chat").PresenceList(t.Context(), 1, 25)
		assertCode(t, err, ErrNotConnected)
	})
} // end function TestPresenceQueryFailsWhileNotConnected

func TestPresenceQueryWithADoneContextSendsNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		_, err := segment(t, channel, "chat").PresenceList(ctx, 1, 25)
		assertCode(t, err, ErrCancelled)
		assertCommands(t, socket)

		next := queryAsync(t.Context(), segment(t, channel, "chat"), 1, 25)
		socket.receive(presenceResponseFrame(response("1")))

		if outcome := <-next; outcome.err != nil {
			t.Fatal(outcome.err)
		}
	})
} // end function TestPresenceQueryWithADoneContextSendsNothing

func TestPresenceTimeoutLeavesTheConnectionAndDropsTheLateReply(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		errorsSeen := recordErrors(channel)
		states := recordStates(channel)
		pending := queryAsync(t.Context(), segment(t, channel, "chat"), 1, 25)

		synctest.Sleep(10*time.Second - time.Millisecond)

		select {
		case <-pending:
			t.Fatal("settled before its deadline")
		default:
		}

		synctest.Sleep(time.Millisecond)
		assertCode(t, (<-pending).err, ErrTimeout)

		// A late reply carries its own query's request id, so it cannot be
		// mistaken for the next query's (QUERY-01).
		if len(states.all()) != 0 || socket.isClosed() {
			t.Fatal("the connection was disturbed")
		}

		next := queryAsync(t.Context(), segment(t, channel, "chat"), 1, 25)
		late := response("1")
		late.total = 1
		current := response("2")
		current.total = 7
		receiveAll(socket, presenceResponseFrame(late), presenceErrorFrame("InternalError", "1"), presenceResponseFrame(current))

		if outcome := <-next; outcome.err != nil || outcome.page.Total != 7 {
			t.Fatalf("got %+v and %v", outcome.page, outcome.err)
		}

		if len(errorsSeen.all()) != 0 {
			t.Fatalf("errors %v", errorsSeen.all())
		}
	})
} // end function TestPresenceTimeoutLeavesTheConnectionAndDropsTheLateReply

func TestPresenceQueryTimeoutIsConfigurable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, _ := connectClientChannel(t, ClientOptions{PresenceQueryTimeout: 2 * time.Second})
		err := expectPresenceTimeout(t, channel, 2*time.Second)

		if want := "Presence query timed out after 2s."; err.Error() != want {
			t.Fatalf("message %q, want %q", err.Error(), want)
		}
	})
} // end function TestPresenceQueryTimeoutIsConfigurable

func TestCancellingASentQueryFreesTheSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		ctx, cancel := context.WithCancel(t.Context())
		pending := queryAsync(ctx, segment(t, channel, "chat"), 1, 25)
		cancel()
		assertCode(t, (<-pending).err, ErrCancelled)

		if channel.State() != StateConnected {
			t.Fatalf("state %s", channel.State())
		}

		next := queryAsync(t.Context(), segment(t, channel, "chat"), 1, 25)
		current := response("2")
		current.total = 3
		receiveAll(socket, presenceResponseFrame(response("1")), presenceResponseFrame(current))

		if outcome := <-next; outcome.err != nil || outcome.page.Total != 3 {
			t.Fatalf("got %+v and %v", outcome.page, outcome.err)
		}
	})
} // end function TestCancellingASentQueryFreesTheSlot

func TestPresenceResponsesMatchByRequestIDAlone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		receiveAll(socket, presenceResponseFrame(response("9")))
		pending := queryAsync(t.Context(), segment(t, channel, "chat"), 2, 50)
		matching := response("1")
		matching.currentPage = 7
		receiveAll(socket, presenceResponseFrame(response("0")), presenceResponseFrame(response("10")), presenceResponseFrame(matching))

		if outcome := <-pending; outcome.err != nil || outcome.page.CurrentPage != 7 {
			t.Fatalf("got %+v and %v", outcome.page, outcome.err)
		}
	})
} // end function TestPresenceResponsesMatchByRequestIDAlone

func TestPresenceErrorNamingTheQueryRejectsItAtOnce(t *testing.T) {
	for _, errorType := range []string{"InternalError", "PermissionDeniedError"} {
		synctest.Test(t, func(t *testing.T) {
			channel, _, socket := connectTestChannel(t)
			errorsSeen := recordErrors(channel)
			pending := queryAsync(t.Context(), segment(t, channel, "chat"), 1, 25)
			socket.receive(presenceErrorFrame(errorType, "1"))

			want := &ServerError{Type: ServerErrorType(errorType), SubType: "PRES_LIST", Message: "failed", Resource: "1"}

			if outcome := <-pending; !reflect.DeepEqual(outcome.err, want) {
				t.Fatalf("%s: got %#v", errorType, outcome.err)
			}

			// Reported once, to the caller; the connection is untouched.
			synctest.Wait()

			if len(errorsSeen.all()) != 0 || channel.State() != StateConnected {
				t.Fatalf("errors %v, state %s", errorsSeen.all(), channel.State())
			}

			next := queryAsync(t.Context(), segment(t, channel, "chat"), 1, 25)
			socket.receive(presenceResponseFrame(response("2")))

			if outcome := <-next; outcome.err != nil {
				t.Fatal(outcome.err)
			}
		})
	}
} // end function TestPresenceErrorNamingTheQueryRejectsItAtOnce

func TestPresenceErrorForAnotherQueryIsDropped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		errorsSeen := recordErrors(channel)
		receiveAll(socket, presenceErrorFrame("InternalError", "1"))
		pending := queryAsync(t.Context(), segment(t, channel, "chat"), 1, 25)
		receiveAll(socket, presenceErrorFrame("InternalError", "0"), presenceResponseFrame(response("1")))

		if outcome := <-pending; outcome.err != nil || len(errorsSeen.all()) != 0 {
			t.Fatalf("got %v, errors %v", outcome.err, errorsSeen.all())
		}
	})
} // end function TestPresenceErrorForAnotherQueryIsDropped

func TestPendingQueryFailsOnConnectionLossAndOnClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lost, server, socket := connectTestChannel(t)
		server.set(func(server *fakeServer) { server.blockDials = true })
		pending := queryAsync(t.Context(), segment(t, lost, "chat"), 1, 25)
		socket.drop()

		err := (<-pending).err
		assertCode(t, err, ErrTransport)

		if want := "Connection lost during the presence query; query again once the channel reconnects."; err.Error() != want {
			t.Fatalf("message %q", err.Error())
		}

		lost.Close()

		closed, _, _ := connectTestChannel(t)
		pending = queryAsync(t.Context(), segment(t, closed, "chat"), 1, 25)
		closed.Close()
		err = (<-pending).err
		assertCode(t, err, ErrCancelled)

		if want := "Channel closed while the presence query was pending."; err.Error() != want {
			t.Fatalf("message %q", err.Error())
		}
	})
} // end function TestPendingQueryFailsOnConnectionLossAndOnClose

// A write the socket refuses breaks the connection: the query fails with the
// connection, and its request id stays spent since it may have gone out.
func TestRefusedQueryWriteSpendsItsID(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		socket.setFailWrites(true)
		pending := queryAsync(t.Context(), segment(t, channel, "chat"), 1, 25)
		assertCode(t, (<-pending).err, ErrTransport)
		synctest.Wait()

		next := queryAsync(t.Context(), segment(t, channel, "chat"), 1, 25)
		restored := server.socket(-1)
		assertCommands(t, restored, "@PRES_LIST\n$4\nchat\n;1\n;25\n$1\n2\n")
		restored.receive(presenceResponseFrame(response("2")))

		if outcome := <-next; outcome.err != nil {
			t.Fatal(outcome.err)
		}
	})
} // end function TestRefusedQueryWriteSpendsItsID

// The reply is routed by the goroutine running listeners, so a query made
// inside one cannot complete: it times out, deterministically.
func TestPresenceQueryInsideAListenerTimesOut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		outcomes := &recorder[error]{}
		channel.Events().OnNotice(func(ServerNotice) {
			_, err := segment(t, channel, "chat").PresenceList(t.Context(), 1, 25)
			outcomes.record(err)
		})

		socket.receive("@SERVER_MSG\n:1\n$0\n\n")
		synctest.Wait()
		socket.receive(presenceResponseFrame(response("1")))
		synctest.Sleep(10 * time.Second)

		if got := outcomes.all(); len(got) != 1 {
			t.Fatalf("outcomes %v", got)
		}

		assertCode(t, outcomes.all()[0], ErrTimeout)
	})
} // end function TestPresenceQueryInsideAListenerTimesOut

func TestNoticesAreDeliveredInOrderWithWorkingRemoval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		seen := &recorder[string]{}
		stopFirst := channel.Events().OnNotice(func(notice ServerNotice) {
			seen.record("first " + strconv.FormatInt(notice.Timestamp, 10) + " " + string(notice.Payload))
		})

		channel.Events().OnNotice(func(notice ServerNotice) {
			seen.record("second " + strconv.FormatInt(notice.Timestamp, 10) + " " + string(notice.Payload))
		})

		receiveAll(socket, "@SERVER_MSG\n:7\n$6\njoined\n")
		stopFirst()
		receiveAll(socket, "@SERVER_MSG\n:8\n$4\nleft\n")

		if want := []string{"first 7 joined", "second 7 joined", "second 8 left"}; !slices.Equal(seen.all(), want) {
			t.Fatalf("seen %v", seen.all())
		}
	})
} // end function TestNoticesAreDeliveredInOrderWithWorkingRemoval

func TestPresenceEventsReachOnlyTheirSegment(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		chat := &recorder[PresenceEvent]{}
		lobby := &recorder[PresenceEvent]{}
		segment(t, channel, "chat").OnPresence(chat.record)
		segment(t, channel, "lobby").OnPresence(lobby.record)
		receiveAll(socket, presenceNotifyFrame("chat", true, 7), presenceNotifyFrame("chat", false, 9), presenceNotifyFrame("unwatched", true, 1))

		want := []PresenceEvent{
			{SegmentID: "chat", TokenReference: "user", ConnectionID: "connection-1", Joined: true, Timestamp: 7},
			{SegmentID: "chat", TokenReference: "user", ConnectionID: "connection-1", Joined: false, Timestamp: 9},
		}

		if !reflect.DeepEqual(chat.all(), want) || len(lobby.all()) != 0 {
			t.Fatalf("chat %+v, lobby %+v", chat.all(), lobby.all())
		}
	})
} // end function TestPresenceEventsReachOnlyTheirSegment

func TestPanickingNoticeAndPresenceListenersAreContained(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		errorsSeen := recordErrors(channel)
		order := &recorder[string]{}
		channel.Events().OnNotice(func(ServerNotice) { panic("notice-secret") })
		channel.Events().OnNotice(func(ServerNotice) { order.record("notice after") })
		segment(t, channel, "chat").OnPresence(func(PresenceEvent) { panic("presence-secret") })
		segment(t, channel, "chat").OnPresence(func(PresenceEvent) { order.record("presence after") })
		receiveAll(socket, "@SERVER_MSG\n:1\n$0\n\n", presenceNotifyFrame("chat", true, 1))

		if !slices.Equal(order.all(), []string{"notice after", "presence after"}) || len(errorsSeen.all()) != 2 {
			t.Fatalf("order %v, errors %v", order.all(), errorsSeen.all())
		}

		for _, err := range errorsSeen.all() {
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("error leaks the panic: %v", err)
			}
		}
	})
} // end function TestPanickingNoticeAndPresenceListenersAreContained
