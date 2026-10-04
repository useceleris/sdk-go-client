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
)

// receiveAll delivers each frame and waits until the channel has handled it.
func receiveAll(socket *fakeSocket, frames ...string) {
	for _, frame := range frames {
		socket.receive(frame)
		synctest.Wait()
	}
}

func recordMessageIDs(handle *Segment) *recorder[string] {
	identifiers := &recorder[string]{}
	handle.OnMessage(func(_ []byte, metadata MessageMetadata) { identifiers.record(metadata.MessageID) })

	return identifiers
}

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
}

func TestDefaultSegmentIsNeverSubscribed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		subscribe(t, channel.DefaultSegment()).Cancel()
		synctest.Wait()
		assertCommands(t, socket)
		channel.Close()
	})
}

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
}

func TestPublishFailsWhileNotConnected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		idle, _ := newTestChannel(t)
		assertCode(t, idle.DefaultSegment().Publish(t.Context(), []byte("x")), ErrNotConnected)

		channel, server, socket := connectTestChannel(t)
		chat := segment(t, channel, "chat")
		server.set(func(server *fakeServer) { server.blockDials = true })
		socket.drop()
		synctest.Wait()

		if channel.State() != StateReconnecting {
			t.Fatalf("state %s", channel.State())
		}

		assertCode(t, chat.Publish(t.Context(), []byte("x")), ErrNotConnected)
		channel.Close()
		assertCode(t, chat.Publish(t.Context(), []byte("x")), ErrNotConnected)
		assertCommands(t, socket)
	})
}

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
}

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

		if want := "64 publishes are already waiting to be sent. Retry once some have gone out."; err.Error() != want {
			t.Fatalf("message %q", err.Error())
		}

		release()

		for _, result := range results {
			if err := <-result; err != nil {
				t.Fatal(err)
			}
		}

		if len(socket.commands()) != 128 || channel.State() != StateConnected {
			t.Fatalf("%d commands, state %s", len(socket.commands()), channel.State())
		}

		channel.Close()
	})
}

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
}

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
}

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
}

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
}

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
}

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
}

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
}

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
}

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
}

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
}

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
}

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
}

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
}

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
}

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
}

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
}

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
}

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
}

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
}

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
}

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
}

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
}

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
}

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
}
