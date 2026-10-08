package celeris

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// receiveAll delivers each frame and waits until the channel has handled it.
func receiveAll(socket *fakeSocket, frames ...string) {
	for _, frame := range frames {
		socket.receive(frame)
		synctest.Wait()
	}
} // end function receiveAll

func recordMessageIDs(handle *Segment) *recorder[string] {
	identifiers := &recorder[string]{}
	handle.OnMessage(func(_ []byte, metadata MessageMetadata) { identifiers.record(metadata.MessageID) })

	return identifiers
} // end function recordMessageIDs

func TestSegmentsShareOneInterestCount(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		first := subscribe(t, segment(t, channel, "chat"))
		second := subscribe(t, segment(t, channel, "chat"))
		synctest.Wait()
		assertCommands(t, socket, "@SUB\n$4\nchat\n")

		first.Cancel()
		first.Cancel()
		synctest.Wait()
		assertCommands(t, socket, "@SUB\n$4\nchat\n")

		second.Cancel()
		synctest.Wait()
		assertCommands(t, socket, "@SUB\n$4\nchat\n", "@UNSUB\n$4\nchat\n")
		channel.Close()
	})
} // end function TestSegmentsShareOneInterestCount

func TestSegmentsMultiplexOverOneSocketAndAnotherChannelOpensAnother(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		subscribe(t, segment(t, channel, "alpha"))
		subscribe(t, segment(t, channel, "beta"))
		publish(t, segment(t, channel, "gamma"), "m-1", "x")
		synctest.Wait()

		if server.socketCount() != 1 || len(socket.commands()) != 3 {
			t.Fatalf("%d sockets, commands %q", server.socketCount(), socket.commands())
		}

		other, err := channel.client.Channel("room-2")

		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(other.Close)

		if err := other.Connect(t.Context()); err != nil {
			t.Fatalf("connect: %v", err)
		}

		if server.socketCount() != 2 {
			t.Fatalf("%d sockets, want 2", server.socketCount())
		}

		other.Close()
		channel.Close()
	})
} // end function TestSegmentsMultiplexOverOneSocketAndAnotherChannelOpensAnother

func TestDefaultSegmentIsNeverSubscribed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		subscribe(t, channel.DefaultSegment()).Cancel()
		synctest.Wait()
		assertCommands(t, socket)
		channel.Close()
	})
} // end function TestDefaultSegmentIsNeverSubscribed

func TestPublishCompletesOnLocalAcceptance(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)

		if err := channel.DefaultSegment().Publish(t.Context(), []byte("hi")); err != nil {
			t.Fatal(err)
		}

		publish(t, segment(t, channel, "chat"), "m-1", "yo")

		commands := socket.commands()
		generated := regexp.MustCompile("^@PUB\n\\$7\ndefault\n\\$32\n[0-9a-f]{32}\n\\$2\nhi\n$")

		if len(commands) != 2 || !generated.MatchString(commands[0]) || commands[1] != "@PUB\n$4\nchat\n$3\nm-1\n$2\nyo\n" {
			t.Fatalf("commands %q", commands)
		}

		channel.Close()
	})
} // end function TestPublishCompletesOnLocalAcceptance

// QUEUE-01: with no recovery in progress, a publish has nothing to wait for.
func TestPublishFailsWhileNoRecoveryIsInProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server := newTestChannel(t)
		lobby := channel.DefaultSegment()
		assertCode(t, lobby.Publish(t.Context(), []byte("before connect")), ErrNotConnected)

		server.set(func(server *fakeServer) { server.blockProvider = true })
		connecting := make(chan error, 1)

		go func() { connecting <- channel.Connect(t.Context()) }()

		synctest.Wait()
		assertCode(t, lobby.Publish(t.Context(), []byte("during connect")), ErrNotConnected)

		synctest.Sleep(15 * time.Second)
		assertCode(t, <-connecting, ErrTimeout)

		if channel.State() != StateFailed {
			t.Fatalf("state %s", channel.State())
		}

		assertCode(t, lobby.Publish(t.Context(), []byte("after failed")), ErrNotConnected)
		channel.Close()
		assertCode(t, lobby.Publish(t.Context(), []byte("after close")), ErrNotConnected)

		if server.socketCount() != 0 {
			t.Fatalf("%d sockets", server.socketCount())
		}
	})
} // end function TestPublishFailsWhileNoRecoveryIsInProgress

func TestPublishRefusesInvalidInputBeforeWriting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		lobby := channel.DefaultSegment()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		assertCode(t, lobby.Publish(ctx, []byte("x")), ErrCancelled)
		assertCode(t, lobby.PublishWithMessageID(t.Context(), []byte("x"), ""), ErrConfiguration)

		err := lobby.Publish(t.Context(), make([]byte, maximumCommandBytes))

		if !errors.Is(err, ErrConfiguration) || !strings.HasPrefix(err.Error(), "Encoded command exceeds 2 MiB.") {
			t.Fatalf("got %v", err)
		}

		assertCommands(t, socket)
		publish(t, lobby, "m-1", "")
		assertCommands(t, socket, "@PUB\n$7\ndefault\n$3\nm-1\n$0\n\n")
		channel.Close()
	})
} // end function TestPublishRefusesInvalidInputBeforeWriting

func TestPublishesWaitBehindAFullWriter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		lobby := channel.DefaultSegment()
		release := socket.holdWrites()
		var results []<-chan error

		// 64 fill the writer and 64 more wait in the queue.
		for index := range 128 {
			results = append(results, publishAsync(t, lobby, "m-"+strconv.Itoa(index), "x"))
			synctest.Wait()
		}

		err := lobby.PublishWithMessageID(t.Context(), []byte("x"), "m-128")
		assertCode(t, err, ErrBackpressure)

		if want := "The publish queue is full (size 64). Retry once some publishes have gone out."; err.Error() != want {
			t.Fatalf("message %q", err.Error())
		}

		release()

		for _, result := range results {
			if err := <-result; err != nil {
				t.Fatal(err)
			}
		}

		// They drain in the order they were published.
		var want []string

		for index := range 128 {
			want = append(want, publishFrame("default", "m-"+strconv.Itoa(index), "x"))
		}

		assertCommands(t, socket, want...)

		if channel.State() != StateConnected {
			t.Fatalf("state %s", channel.State())
		}

		channel.Close()
	})
} // end function TestPublishesWaitBehindAFullWriter

func TestPublishQueueSizeBoundsWaitingPublishes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectClientChannel(t, ClientOptions{PublishQueueSize: 1})
		lobby := channel.DefaultSegment()
		release := socket.holdWrites()
		var results []<-chan error

		// 64 fill the writer.
		for index := range 64 {
			results = append(results, publishAsync(t, lobby, "m-"+strconv.Itoa(index), "x"))
			synctest.Wait()
		}

		waiting := publishAsync(t, lobby, "waiting", "x")
		synctest.Wait()

		err := lobby.PublishWithMessageID(t.Context(), []byte("x"), "refused")
		assertCode(t, err, ErrBackpressure)

		if want := "The publish queue is full (size 1). Retry once some publishes have gone out."; err.Error() != want {
			t.Fatalf("message %q", err.Error())
		}

		release()

		for _, result := range append(results, waiting) {
			if err := <-result; err != nil {
				t.Fatal(err)
			}
		}

		commands := socket.commands()

		if len(commands) != 65 || commands[64] != publishFrame("default", "waiting", "x") {
			t.Fatalf("%d commands, last %q", len(commands), commands[len(commands)-1])
		}

		channel.Close()
	})
} // end function TestPublishQueueSizeBoundsWaitingPublishes

func TestMaximumSizeCommandFitsOnlyAnEmptyWriter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		lobby := segment(t, channel, "s")
		release := socket.holdWrites()
		small := publishAsync(t, lobby, "m", "x")
		synctest.Wait()

		// "@PUB\n$1\ns\n$1\nl\n$2097127\n" and the closing LF are 25 bytes.
		large := make(chan error, 1)

		go func() {
			large <- lobby.PublishWithMessageID(t.Context(), make([]byte, maximumCommandBytes-25), "l")
		}()

		synctest.Wait()

		channel.mutex.Lock()
		queued := len(channel.queue.publishes)
		channel.mutex.Unlock()

		if queued != 1 {
			t.Fatalf("%d publishes queued, want the large one waiting for room", queued)
		}

		release()

		if err := <-small; err != nil {
			t.Fatal(err)
		}

		if err := <-large; err != nil {
			t.Fatal(err)
		}

		if commands := socket.commands(); len(commands) != 2 || len(commands[1]) != maximumCommandBytes {
			t.Fatalf("%d commands", len(commands))
		}

		channel.Close()
	})
} // end function TestMaximumSizeCommandFitsOnlyAnEmptyWriter

// A write the socket refuses breaks the connection, so the publish reports
// DeliveryUnknown, is never resent, and the channel reconnects.
func TestFailedWriteIsDeliveryUnknown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		errorsSeen := recordErrors(channel)
		socket.setFailWrites(true)

		assertCode(t, channel.DefaultSegment().Publish(t.Context(), []byte("x")), ErrDeliveryUnknown)
		synctest.Wait()

		if channel.State() != StateConnected || server.socketCount() != 2 || len(errorsSeen.all()) != 0 {
			t.Fatalf("state %s, %d sockets, errors %v", channel.State(), server.socketCount(), errorsSeen.all())
		}

		assertCommands(t, server.socket(1))
		channel.Close()
	})
} // end function TestFailedWriteIsDeliveryUnknown

func TestCancellingAPublishWithdrawsItUntilItIsWritten(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		chat := segment(t, channel, "chat")
		release := socket.holdWrites()
		writing := publishAsync(t, chat, "m-1", "x")
		synctest.Wait()

		ctx, cancel := context.WithCancel(t.Context())
		handedOff := make(chan error, 1)

		go func() { handedOff <- chat.PublishWithMessageID(ctx, []byte("y"), "m-2") }()

		synctest.Wait()
		cancel()
		assertCode(t, <-handedOff, ErrCancelled)

		release()

		if err := <-writing; err != nil {
			t.Fatal(err)
		}

		assertCommands(t, socket, publishFrame("chat", "m-1", "x"))
		channel.Close()
	})
} // end function TestCancellingAPublishWithdrawsItUntilItIsWritten

func TestCancellingAPublishMidWriteIsDeliveryUnknown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		socket.holdWrites()
		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)

		go func() { result <- channel.DefaultSegment().PublishWithMessageID(ctx, []byte("x"), "m-1") }()

		synctest.Wait()
		cancel()
		assertCode(t, <-result, ErrDeliveryUnknown)
		channel.Close()
	})
} // end function TestCancellingAPublishMidWriteIsDeliveryUnknown

func TestInterestsAreRestoredOnConnectInRegistrationOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server := newTestChannel(t)
		subscribe(t, segment(t, channel, "beta"))
		subscribe(t, segment(t, channel, "alpha"))
		subscribe(t, channel.DefaultSegment())
		subscribePresence(t, segment(t, channel, "gamma"))
		subscribePresence(t, channel.DefaultSegment())

		if err := channel.Connect(t.Context()); err != nil {
			t.Fatal(err)
		}

		synctest.Wait()
		assertCommands(t, server.socket(0),
			"@SUB\n$4\nbeta\n",
			"@SUB\n$5\nalpha\n",
			"@PRES_SUB\n$5\ngamma\n",
			"@PRES_SUB\n$7\ndefault\n",
		)
		channel.Close()
	})
} // end function TestInterestsAreRestoredOnConnectInRegistrationOrder

func TestSubscriptionsWaitBehindAFullWriterAheadOfPublishes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		errorsSeen := recordErrors(channel)
		release := socket.holdWrites()
		var results []<-chan error

		for index := range 64 {
			results = append(results, publishAsync(t, channel.DefaultSegment(), "m-"+strconv.Itoa(index), "x"))
			synctest.Wait()
		}

		results = append(results, publishAsync(t, segment(t, channel, "lobby"), "m-lobby", "y"))
		synctest.Wait()
		subscribe(t, segment(t, channel, "chat"))
		release()

		for _, result := range results {
			if err := <-result; err != nil {
				t.Fatal(err)
			}
		}

		if got := socket.commands()[64:]; !slices.Equal(got, []string{"@SUB\n$4\nchat\n", publishFrame("lobby", "m-lobby", "y")}) {
			t.Fatalf("commands %q", got)
		}

		if len(errorsSeen.all()) != 0 {
			t.Fatalf("errors %v", errorsSeen.all())
		}

		channel.Close()
	})
} // end function TestSubscriptionsWaitBehindAFullWriterAheadOfPublishes

func TestMoreThan64SubscriptionsAllRestore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server := newTestChannel(t)

		for index := range 65 {
			subscribe(t, segment(t, channel, "segment-"+strconv.Itoa(index)))
		}

		if err := channel.Connect(t.Context()); err != nil {
			t.Fatal(err)
		}

		synctest.Wait()

		if commands := server.socket(0).commands(); len(commands) != 65 || commands[64] != "@SUB\n$10\nsegment-64\n" {
			t.Fatalf("%d commands", len(commands))
		}

		channel.Close()
	})
} // end function TestMoreThan64SubscriptionsAllRestore

func TestSubscribeOnAClosedChannelFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _ := newTestChannel(t)
		channel.Close()

		_, err := segment(t, channel, "chat").Subscribe()
		assertCode(t, err, ErrNotConnected)

		if want := "Channel is closed; create a new one with Client.Channel."; err.Error() != want {
			t.Fatalf("message %q", err.Error())
		}
	})
} // end function TestSubscribeOnAClosedChannelFails

func TestAListenerAloneSendsNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		segment(t, channel, "chat").OnMessage(func([]byte, MessageMetadata) {})
		channel.Events().OnMessage(func([]byte, MessageMetadata) {})
		synctest.Wait()
		assertCommands(t, socket)
		channel.Close()
	})
} // end function TestAListenerAloneSendsNothing

func TestDeliveriesReachOnlyTheirSegment(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		chat := recordMessageIDs(segment(t, channel, "chat"))
		lobby := recordMessageIDs(channel.DefaultSegment())

		receiveAll(socket, messageFrame("chat", "id-1", "hi"), messageFrame("default", "id-2", "yo"), messageFrame("other", "id-3", "no"))

		if !slices.Equal(chat.all(), []string{"id-1"}) || !slices.Equal(lobby.all(), []string{"id-2"}) {
			t.Fatalf("chat %v, lobby %v", chat.all(), lobby.all())
		}

		channel.Close()
	})
} // end function TestDeliveriesReachOnlyTheirSegment

func TestDeliveriesReachEveryHandleWithEveryField(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)

		type delivery struct {
			payload  string
			metadata MessageMetadata
		}

		seen := &recorder[delivery]{}

		for range 2 {
			segment(t, channel, "chat").OnMessage(func(payload []byte, metadata MessageMetadata) {
				seen.record(delivery{string(payload), metadata})
			})
		}

		receiveAll(socket, messageFrame("chat", "id-1", "hi"))
		want := delivery{"hi", MessageMetadata{TokenReference: "user", SegmentID: "chat", MessageID: "id-1", Timestamp: 1}}

		if got := seen.all(); len(got) != 2 || got[0] != want || got[1] != want {
			t.Fatalf("seen %+v", got)
		}

		channel.Close()
	})
} // end function TestDeliveriesReachEveryHandleWithEveryField

func TestDeliveriesAreDeduplicatedEvenWithoutListeners(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		receiveAll(socket, messageFrame("chat", "id-1", "hi"))
		delivered := recordMessageIDs(segment(t, channel, "chat"))
		receiveAll(socket, messageFrame("chat", "id-1", "hi"), messageFrame("chat", "id-2", "hi"), messageFrame("chat", "id-2", "hi"))

		if !slices.Equal(delivered.all(), []string{"id-2"}) {
			t.Fatalf("delivered %v", delivered.all())
		}

		channel.Close()
	})
} // end function TestDeliveriesAreDeduplicatedEvenWithoutListeners

func TestDeduplicationWindowEvictsTheOldest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		delivered := recordMessageIDs(segment(t, channel, "chat"))

		for index := range 1025 {
			receiveAll(socket, messageFrame("chat", "id-"+strconv.Itoa(index), "x"))
		}

		receiveAll(socket, messageFrame("chat", "id-0", "x"))

		if got := delivered.all(); len(got) != 1026 || got[1025] != "id-0" {
			t.Fatalf("%d delivered", len(got))
		}

		channel.Close()
	})
} // end function TestDeduplicationWindowEvictsTheOldest

func TestDeduplicationWindowSizeIsConfigurable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectClientChannel(t, ClientOptions{DeduplicationWindowSize: 1})
		delivered := recordMessageIDs(segment(t, channel, "chat"))

		// The second "a" is dropped. "b" evicts "a", so the third is delivered.
		receiveAll(socket, messageFrame("chat", "a", "x"), messageFrame("chat", "a", "x"), messageFrame("chat", "b", "x"), messageFrame("chat", "a", "x"))

		if !slices.Equal(delivered.all(), []string{"a", "b", "a"}) {
			t.Fatalf("delivered %v", delivered.all())
		}

		channel.Close()
	})
} // end function TestDeduplicationWindowSizeIsConfigurable

func TestDeduplicationSurvivesReconnectAndClearsOnConnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		delivered := recordMessageIDs(segment(t, channel, "chat"))
		subscribe(t, segment(t, channel, "chat"))
		receiveAll(socket, messageFrame("chat", "id-1", "x"))
		socket.drop()
		synctest.Wait()
		receiveAll(server.socket(1), messageFrame("chat", "id-1", "x"))

		if !slices.Equal(delivered.all(), []string{"id-1"}) {
			t.Fatalf("delivered %v", delivered.all())
		}

		channel.Close()

		fresh, _, freshSocket := connectTestChannel(t)
		redelivered := recordMessageIDs(segment(t, fresh, "chat"))
		receiveAll(freshSocket, messageFrame("chat", "id-1", "x"))

		if !slices.Equal(redelivered.all(), []string{"id-1"}) {
			t.Fatalf("redelivered %v", redelivered.all())
		}

		fresh.Close()
	})
} // end function TestDeduplicationSurvivesReconnectAndClearsOnConnect

func TestNullIDDeliveryIsDroppedWithoutDroppingTheConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		errorsSeen := recordErrors(channel)
		delivered := recordMessageIDs(segment(t, channel, "chat"))
		receiveAll(socket, messageFrame("chat", "", "x"))

		reported := errorsSeen.all()

		if len(delivered.all()) != 0 || len(reported) != 1 {
			t.Fatalf("delivered %v, errors %v", delivered.all(), reported)
		}

		assertProtocolError(t, reported[0], "Server message is missing its identifier.", "MessageID", 0)

		if channel.State() != StateConnected || socket.isClosed() {
			t.Fatal("connection dropped")
		}

		channel.Close()
	})
} // end function TestNullIDDeliveryIsDroppedWithoutDroppingTheConnection

func TestUnknownCommandsAreSkippedSilently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		errorsSeen := recordErrors(channel)
		states := recordStates(channel)
		delivered := recordMessageIDs(segment(t, channel, "chat"))
		receiveAll(socket, "@NODE_PUB\n+node-1\n$4\nbody\n", messageFrame("chat", "id-1", "x"))

		if len(errorsSeen.all()) != 0 || len(states.all()) != 0 || !slices.Equal(delivered.all(), []string{"id-1"}) {
			t.Fatalf("errors %v, states %v, delivered %v", errorsSeen.all(), states.all(), delivered.all())
		}

		channel.Close()
	})
} // end function TestUnknownCommandsAreSkippedSilently

func TestUndecodableMessagesAreReportedAndTheConnectionStays(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		errorsSeen := recordErrors(channel)
		delivered := recordMessageIDs(segment(t, channel, "chat"))
		receiveAll(socket, "*2\n@FUTURE\n+a\n"+messageFrame("chat", "id-0", "x"), messageFrame("chat", "id-1", "x"))
		socket.incoming <- fakeFrame{binary: false, data: []byte("text")}
		synctest.Wait()

		reported := errorsSeen.all()

		if len(reported) != 2 || !slices.Equal(delivered.all(), []string{"id-1"}) || channel.State() != StateConnected {
			t.Fatalf("errors %v, delivered %v", reported, delivered.all())
		}

		assertProtocolError(t, reported[0], "Unknown command inside array has ambiguous boundaries.", "Command", 3)
		assertProtocolError(t, reported[1], "Expected a binary WebSocket message.", "Message", 0)
		channel.Close()
	})
} // end function TestUndecodableMessagesAreReportedAndTheConnectionStays

func TestNoticesAreRawAndServerErrorsKeepTheConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		errorsSeen := recordErrors(channel)
		notices := &recorder[ServerNotice]{}
		channel.Events().OnNotice(notices.record)
		delivered := recordMessageIDs(segment(t, channel, "chat"))

		receiveAll(socket,
			"@SERVER_MSG\n:1\n$29\nSuccessfully connected to \"x\"\n",
			errorFrame("RateLimitError", "slow down", "", ""),
			messageFrame("chat", "id-1", "x"),
		)

		if got := notices.all(); len(got) != 1 || string(got[0].Payload) != `Successfully connected to "x"` || got[0].Timestamp != 1 {
			t.Fatalf("notices %+v", got)
		}

		want := &ServerError{Type: RateLimitError, Message: "slow down"}

		if got := errorsSeen.all(); len(got) != 1 || !reflect.DeepEqual(got[0], want) {
			t.Fatalf("errors %#v", got)
		}

		if channel.State() != StateConnected || !slices.Equal(delivered.all(), []string{"id-1"}) {
			t.Fatal("connection or delivery affected")
		}

		channel.Close()
	})
} // end function TestNoticesAreRawAndServerErrorsKeepTheConnection

func TestPermissionDenialNamesItsCommandAndSegment(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		errorsSeen := recordErrors(channel)

		// A denied publish completes locally; the error arrives afterwards.
		publish(t, segment(t, channel, "chat"), "m-1", "x")
		receiveAll(socket, errorFrame("PermissionDeniedError", "denied", "PUB", "$4\nchat\n"))

		want := &ServerError{Type: PermissionDeniedError, SubType: "PUB", Message: "denied", Resource: "chat"}

		if got := errorsSeen.all(); len(got) != 1 || !reflect.DeepEqual(got[0], want) {
			t.Fatalf("errors %#v", got)
		}

		channel.Close()
	})
} // end function TestPermissionDenialNamesItsCommandAndSegment

func TestEveryServerErrorTypeSurfacesWithEveryField(t *testing.T) {
	for _, errorType := range []string{"ParserError", "SendError", "PermissionDeniedError", "RateLimitError", "MessageSizeLimitError", "InternalError", "SomeFutureError"} {
		synctest.Test(t, func(t *testing.T) {
			channel, _, socket := connectTestChannel(t)
			errorsSeen := recordErrors(channel)
			receiveAll(socket, errorFrame(errorType, "what happened", "SUB", "*2\n+a\n:7\n"))

			want := &ServerError{Type: ServerErrorType(errorType), SubType: "SUB", Message: "what happened", Resource: []any{"a", int64(7)}}

			if got := errorsSeen.all(); len(got) != 1 || !reflect.DeepEqual(got[0], want) {
				t.Fatalf("%s: errors %#v", errorType, got)
			}

			if channel.State() != StateConnected {
				t.Fatalf("%s: state %s", errorType, channel.State())
			}

			channel.Close()
		})
	}
} // end function TestEveryServerErrorTypeSurfacesWithEveryField

func TestMalformedServerErrorTextStillSurfaces(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		errorsSeen := recordErrors(channel)
		receiveAll(socket, "-Err\n+SendError\n$-1\n$6\nbad \xff\xfe\n$-1\n")

		got := errorsSeen.all()

		if len(got) != 1 {
			t.Fatalf("errors %v", got)
		}

		serverError, ok := errors.AsType[*ServerError](got[0])

		if !ok || serverError.Type != SendError || !strings.HasPrefix(serverError.Message, "bad ") {
			t.Fatalf("errors %v", got)
		}

		channel.Close()
	})
} // end function TestMalformedServerErrorTextStillSurfaces

func TestPanickingMessageListenersAreContained(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		errorsSeen := recordErrors(channel)
		order := &recorder[string]{}
		chat := segment(t, channel, "chat")
		removeSecond := func() {}
		chat.OnMessage(func([]byte, MessageMetadata) {
			order.record("first")
			removeSecond()
			panic("listener-secret")
		})

		removeSecond = chat.OnMessage(func([]byte, MessageMetadata) { order.record("second") })
		chat.OnMessage(func([]byte, MessageMetadata) { order.record("third") })
		receiveAll(socket, messageFrame("chat", "id-1", "x"))

		if !slices.Equal(order.all(), []string{"first", "third"}) || len(errorsSeen.all()) != 1 || channel.State() != StateConnected {
			t.Fatalf("order %v, errors %v", order.all(), errorsSeen.all())
		}

		channel.Close()
	})
} // end function TestPanickingMessageListenersAreContained

func recordChannelMessageIDs(channel *Channel) *recorder[string] {
	identifiers := &recorder[string]{}
	channel.Events().OnMessage(func(_ []byte, metadata MessageMetadata) { identifiers.record(metadata.MessageID) })

	return identifiers
} // end function recordChannelMessageIDs

func TestChannelListenerReceivesEverySegment(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		seen := &recorder[string]{}
		channel.Events().OnMessage(func(payload []byte, metadata MessageMetadata) {
			seen.record(metadata.SegmentID + ":" + string(payload))
		})

		segment(t, channel, "chat").OnMessage(func([]byte, MessageMetadata) {})

		receiveAll(socket, messageFrame("chat", "id-1", "hi"), messageFrame("default", "id-2", "yo"), messageFrame("joined-by-publish", "id-3", "ok"))

		if want := []string{"chat:hi", "default:yo", "joined-by-publish:ok"}; !slices.Equal(seen.all(), want) {
			t.Fatalf("seen %v", seen.all())
		}

		channel.Close()
	})
} // end function TestChannelListenerReceivesEverySegment

func TestSegmentListenersRunBeforeChannelListeners(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		order := &recorder[string]{}
		channel.Events().OnMessage(func([]byte, MessageMetadata) { order.record("channel") })
		segment(t, channel, "chat").OnMessage(func([]byte, MessageMetadata) { order.record("segment") })

		receiveAll(socket, messageFrame("chat", "id-1", "x"))

		if !slices.Equal(order.all(), []string{"segment", "channel"}) {
			t.Fatalf("order %v", order.all())
		}

		channel.Close()
	})
} // end function TestSegmentListenersRunBeforeChannelListeners

func TestChannelListenerSeesADuplicateOnceAndNeverAMissingID(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		delivered := recordChannelMessageIDs(channel)

		receiveAll(socket, messageFrame("chat", "id-1", "x"), messageFrame("other", "id-1", "x"), messageFrame("chat", "", "x"))

		if !slices.Equal(delivered.all(), []string{"id-1"}) {
			t.Fatalf("delivered %v", delivered.all())
		}

		channel.Close()
	})
} // end function TestChannelListenerSeesADuplicateOnceAndNeverAMissingID

func TestPanickingChannelListenerIsContainedAndRemovable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		errorsSeen := recordErrors(channel)
		stopPanicking := channel.Events().OnMessage(func([]byte, MessageMetadata) { panic("listener-secret") })
		delivered := &recorder[string]{}
		stopRecording := channel.Events().OnMessage(func(_ []byte, metadata MessageMetadata) { delivered.record(metadata.MessageID) })

		receiveAll(socket, messageFrame("chat", "id-1", "x"))
		stopPanicking()
		stopRecording()
		receiveAll(socket, messageFrame("chat", "id-2", "x"))

		reported := errorsSeen.all()

		if !slices.Equal(delivered.all(), []string{"id-1"}) || len(reported) != 1 || channel.State() != StateConnected {
			t.Fatalf("delivered %v, errors %v", delivered.all(), reported)
		}

		assertCode(t, reported[0], ErrTransport)

		if want := "A listener callback panicked; the channel recovered and kept running."; reported[0].Error() != want {
			t.Fatalf("reported %q", reported[0].Error())
		}

		channel.Close()
	})
} // end function TestPanickingChannelListenerIsContainedAndRemovable

func TestRemovingAChannelListenerRemovesOnlyThatListener(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		delivered := &recorder[string]{}
		removeChannelListener := channel.Events().OnMessage(func([]byte, MessageMetadata) { delivered.record("removed") })
		channel.Events().OnMessage(func([]byte, MessageMetadata) { delivered.record("channel") })
		chat := segment(t, channel, "chat")
		chat.OnMessage(func([]byte, MessageMetadata) { delivered.record("segment") })
		subscribe(t, chat)

		removeChannelListener()
		removeChannelListener()
		receiveAll(socket, messageFrame("chat", "id-1", "x"))

		if !slices.Equal(delivered.all(), []string{"segment", "channel"}) {
			t.Fatalf("delivered %v", delivered.all())
		}

		assertCommands(t, socket, "@SUB\n$4\nchat\n")
		channel.Close()
	})
} // end function TestRemovingAChannelListenerRemovesOnlyThatListener

// The server decides what arrives; the SDK never gates on subscriptions.
func TestDeliveriesIgnoreTheSubscriptionState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		chat := segment(t, channel, "chat")
		delivered := recordMessageIDs(chat)
		subscribe(t, chat).Cancel()

		receiveAll(socket, messageFrame("chat", "id-1", "x"))

		if !slices.Equal(delivered.all(), []string{"id-1"}) {
			t.Fatalf("delivered %v", delivered.all())
		}

		channel.Close()
	})
} // end function TestDeliveriesIgnoreTheSubscriptionState

func TestBatchesFanOutInArrivalOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		delivered := recordMessageIDs(segment(t, channel, "chat"))
		receiveAll(socket, "*2\n"+messageFrame("chat", "al-1", "a")+messageFrame("chat", "al-2", "b"))

		if !slices.Equal(delivered.all(), []string{"al-1", "al-2"}) {
			t.Fatalf("delivered %v", delivered.all())
		}

		channel.Close()
	})
} // end function TestBatchesFanOutInArrivalOrder

// LIMIT-01: a received message is never size-checked.
func TestDeliveriesOverOneMebibyteArrive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		sizes := &recorder[int]{}
		segment(t, channel, "chat").OnMessage(func(payload []byte, _ MessageMetadata) { sizes.record(len(payload)) })
		receiveAll(socket, messageFrame("chat", "id-1", strings.Repeat("x", 1024*1024)))

		if got := sizes.all(); len(got) != 1 || got[0] != 1024*1024 {
			t.Fatalf("sizes %v", got)
		}

		channel.Close()
	})
} // end function TestDeliveriesOverOneMebibyteArrive

func TestTimestampsKeepTheirFullRange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		timestamps := &recorder[int64]{}
		segment(t, channel, "chat").OnMessage(func(_ []byte, metadata MessageMetadata) { timestamps.record(metadata.Timestamp) })
		receiveAll(socket, "@MSG\n+u\n+chat\n+m\n:9223372036854775807\n$0\n\n")

		if got := timestamps.all(); len(got) != 1 || got[0] != 9223372036854775807 {
			t.Fatalf("timestamps %v", got)
		}

		channel.Close()
	})
} // end function TestTimestampsKeepTheirFullRange
