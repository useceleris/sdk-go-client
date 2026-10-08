package celeris

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestConnectMovesThroughConnectingToConnected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _ := newTestChannel(t)
		states := recordStates(channel)

		if channel.State() != StateIdle {
			t.Fatalf("state %s, want idle", channel.State())
		}

		if err := channel.Connect(t.Context()); err != nil {
			t.Fatal(err)
		}

		if channel.State() != StateConnected {
			t.Fatalf("state %s, want connected", channel.State())
		}

		assertStates(t, states, StateConnecting, StateConnected)
		channel.Close()
	})
} // end function TestConnectMovesThroughConnectingToConnected

func TestConstructionDoesNoNetworkWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server := newTestChannel(t)
		other, _ := channel.client.Channel("room-1")
		segment(t, channel, "chat")
		channel.DefaultSegment()

		if other == channel || server.socketCount() != 0 || len(server.credentialRequests()) != 0 {
			t.Fatal("construction did network work or reused a channel")
		}
	})
} // end function TestConstructionDoesNoNetworkWork

func TestConcurrentConnectIsRejected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server := newTestChannel(t)
		server.set(func(server *fakeServer) { server.blockDials = true })

		go func() { _ = channel.Connect(t.Context()) }()

		synctest.Wait()
		assertCode(t, channel.Connect(t.Context()), ErrOperationInProgress)
		server.set(func(server *fakeServer) { server.blockDials = false })
		channel.Close()
		synctest.Wait()

		channel, _, _ = connectTestChannel(t)
		assertCode(t, channel.Connect(t.Context()), ErrOperationInProgress)
		channel.Close()
	})
} // end function TestConcurrentConnectIsRejected

func TestConnectAfterCloseIsRejected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _ := newTestChannel(t)
		channel.Close()
		assertCode(t, channel.Connect(t.Context()), ErrNotConnected)

		if channel.State() != StateClosed {
			t.Fatalf("state %s, want closed", channel.State())
		}
	})
} // end function TestConnectAfterCloseIsRejected

func TestInitialFailureIsReportedOnceToTheCaller(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server := newTestChannel(t)
		errorsSeen := recordErrors(channel)
		server.set(func(server *fakeServer) { server.failProvider = true })

		err := channel.Connect(t.Context())
		assertCode(t, err, ErrTransport)

		if strings.Contains(err.Error(), "synthetic-secret") || errors.Unwrap(err) != nil {
			t.Fatalf("provider error leaked: %v", err)
		}

		if channel.State() != StateFailed || len(errorsSeen.all()) != 0 {
			t.Fatalf("state %s and errors %v, want failed and none", channel.State(), errorsSeen.all())
		}

		// An explicit connect starts over from failed.
		server.set(func(server *fakeServer) { server.failProvider = false })

		if err := channel.Connect(t.Context()); err != nil || channel.State() != StateConnected {
			t.Fatalf("restart: %v, state %s", err, channel.State())
		}

		channel.Close()
	})
} // end function TestInitialFailureIsReportedOnceToTheCaller

func TestDialFailureNeverQuotesTheCredentialURL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server := newTestChannel(t)
		server.set(func(server *fakeServer) { server.failDials = 1 })

		err := channel.Connect(t.Context())
		assertCode(t, err, ErrTransport)

		if strings.Contains(err.Error(), "payload-1") || strings.Contains(err.Error(), "signature") || errors.Unwrap(err) != nil {
			t.Fatalf("dial error leaked: %v", err)
		}
	})
} // end function TestDialFailureNeverQuotesTheCredentialURL

func TestCancellingConnectAbandonsTheAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server := newTestChannel(t)
		server.set(func(server *fakeServer) { server.blockProvider = true })
		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)

		go func() { result <- channel.Connect(ctx) }()

		synctest.Wait()
		cancel()
		assertCode(t, <-result, ErrCancelled)

		if channel.State() != StateFailed || server.socketCount() != 0 {
			t.Fatalf("state %s with %d sockets", channel.State(), server.socketCount())
		}
	})
} // end function TestCancellingConnectAbandonsTheAttempt

func TestConnectDeadlineCoversCredentialsAndHandshake(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server := newClientChannel(t, ClientOptions{ConnectTimeout: 5 * time.Second})
		err := expectConnectTimeout(t, channel, server, 5*time.Second)

		if want := "Connection attempt timed out after 5s."; err.Error() != want {
			t.Fatalf("message %q, want %q", err.Error(), want)
		}

		server.set(func(server *fakeServer) { server.blockDials = true })
		start := time.Now()

		assertCode(t, channel.Connect(t.Context()), ErrTimeout)

		if elapsed := time.Since(start); elapsed != 5*time.Second {
			t.Fatalf("handshake timed out after %v, want 5s", elapsed)
		}

		if channel.State() != StateFailed {
			t.Fatalf("state %s, want failed", channel.State())
		}
	})
} // end function TestConnectDeadlineCoversCredentialsAndHandshake

func TestCallerDeadlineIsATimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server := newTestChannel(t)
		server.set(func(server *fakeServer) { server.blockProvider = true })
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()

		assertCode(t, channel.Connect(ctx), ErrTimeout)
	})
} // end function TestCallerDeadlineIsATimeout

func TestProviderThatIgnoresItsContextCannotHoldTheAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server := newTestChannel(t)
		release := make(chan struct{})
		channel.client.credentialProvider = func(context.Context, CredentialRequest) (Credentials, error) {
			<-release

			return testCredentials, nil
		}

		assertCode(t, channel.Connect(t.Context()), ErrTimeout)

		// Its late result is discarded and opens nothing.
		close(release)
		synctest.Wait()

		if server.socketCount() != 0 || channel.State() != StateFailed {
			t.Fatalf("late credentials opened a socket")
		}
	})
} // end function TestProviderThatIgnoresItsContextCannotHoldTheAttempt

func TestProviderPanicIsATransportFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _ := newTestChannel(t)
		channel.client.credentialProvider = func(context.Context, CredentialRequest) (Credentials, error) {
			panic("synthetic-secret")
		}

		assertCode(t, channel.Connect(t.Context()), ErrTransport)
	})
} // end function TestProviderPanicIsATransportFailure

func TestInvalidCredentialsAreAConfigurationError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server := newTestChannel(t)
		server.set(func(server *fakeServer) { server.credentials = Credentials{Payload: "secret-payload"} })

		err := channel.Connect(t.Context())
		assertConfigurationError(t, err, "Invalid credentials. Signature: Must not be empty.")
	})
} // end function TestInvalidCredentialsAreAConfigurationError

func TestInitialCredentialRequestCarriesNoOutage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		want := []CredentialRequest{{ChannelReference: "room-1"}}

		if got := server.credentialRequests(); !reflect.DeepEqual(got, want) {
			t.Fatalf("requests %+v, want %+v", got, want)
		}

		if !strings.HasPrefix(socket.url, "wss://example.test/channel/room-1?payload=payload-1&signature=signature-1") {
			t.Fatalf("url %q", socket.url)
		}

		channel.Close()
	})
} // end function TestInitialCredentialRequestCarriesNoOutage

func TestStateListenersRunInOrderAndRemoveCleanly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _ := newTestChannel(t)
		order := &recorder[string]{}
		removeFirst := channel.Events().OnStateChange(func(ChannelState) { order.record("first") })
		channel.Events().OnStateChange(func(ChannelState) { order.record("second") })

		if err := channel.Connect(t.Context()); err != nil {
			t.Fatal(err)
		}

		removeFirst()
		removeFirst()
		channel.Close()

		if got, want := order.all(), []string{"first", "second", "first", "second", "second", "second"}; !slices.Equal(got, want) {
			t.Fatalf("order %v, want %v", got, want)
		}
	})
} // end function TestStateListenersRunInOrderAndRemoveCleanly

func TestListenerRemovedMidDispatchIsSkipped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _ := newTestChannel(t)
		order := &recorder[string]{}
		removeSecond := func() {}
		channel.Events().OnStateChange(func(ChannelState) {
			order.record("first")
			removeSecond()
		})

		removeSecond = channel.Events().OnStateChange(func(ChannelState) { order.record("second") })
		shared := func(ChannelState) { order.record("shared") }
		channel.Events().OnStateChange(shared)
		removeDuplicate := channel.Events().OnStateChange(shared)

		channel.mutex.Lock()
		channel.queueStateChange(StateConnecting)
		channel.mutex.Unlock()
		channel.dispatchEvents()

		if got, want := order.all(), []string{"first", "shared", "shared"}; !slices.Equal(got, want) {
			t.Fatalf("order %v, want %v", got, want)
		}

		removeDuplicate()
		channel.Close()

		if got, want := order.all()[3:], []string{"first", "shared", "first", "shared"}; !slices.Equal(got, want) {
			t.Fatalf("order %v, want %v", got, want)
		}
	})
} // end function TestListenerRemovedMidDispatchIsSkipped

func TestPanickingListenersAreContainedAndReportedOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _ := newTestChannel(t)
		errorsSeen := recordErrors(channel)
		order := &recorder[string]{}
		channel.Events().OnError(func(error) { panic("error-listener-secret") })
		channel.Events().OnStateChange(func(ChannelState) { panic("listener-secret") })
		channel.Events().OnStateChange(func(ChannelState) { order.record("after") })

		channel.mutex.Lock()
		channel.queueStateChange(StateConnecting)
		channel.mutex.Unlock()
		channel.dispatchEvents()

		if got := order.all(); !slices.Equal(got, []string{"after"}) {
			t.Fatalf("order %v", got)
		}

		reported := errorsSeen.all()

		if len(reported) != 1 {
			t.Fatalf("reported %v, want one error", reported)
		}

		assertCode(t, reported[0], ErrTransport)

		if want := "A listener callback panicked; the channel recovered and kept running."; reported[0].Error() != want || strings.Contains(reported[0].Error(), "secret") {
			t.Fatalf("reported %q", reported[0].Error())
		}
	})
} // end function TestPanickingListenersAreContainedAndReportedOnce

func TestNilListenersAreRefused(t *testing.T) {
	channel, _ := newTestChannel(t)

	for name, register := range map[string]func(){
		"OnError":   func() { channel.Events().OnError(nil) },
		"OnMessage": func() { channel.Events().OnMessage(nil) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("a nil %s listener was accepted", name)
				}
			}()

			register()
		}()
	}
} // end function TestNilListenersAreRefused

func TestChannelReferencesAreValidated(t *testing.T) {
	client, err := NewClient(ClientOptions{CredentialProvider: (&fakeServer{}).provide})

	if err != nil {
		t.Fatal(err)
	}

	for reference, message := range map[string]string{
		"":                        "Invalid channel reference. Must not be empty.",
		"bad ref!":                "Invalid channel reference. Must contain only ASCII letters, digits, hyphens (-) or underscores (_).",
		strings.Repeat("a", 256):  "Invalid channel reference. Must be at most 255 characters.",
		"room-\xed\xa0\x80":       "Invalid channel reference. Must contain only ASCII letters, digits, hyphens (-) or underscores (_).",
		"room/../../other-secret": "Invalid channel reference. Must contain only ASCII letters, digits, hyphens (-) or underscores (_).",
	} {
		_, err := client.Channel(reference)
		assertConfigurationError(t, err, message)
	}

	if _, err := client.Channel(strings.Repeat("a", 255) + ""); err != nil {
		t.Fatalf("255 characters refused: %v", err)
	}
} // end function TestChannelReferencesAreValidated

func TestSegmentIDsAreValidated(t *testing.T) {
	channel, _ := newTestChannel(t)

	_, err := channel.Segment("")
	assertConfigurationError(t, err, "Invalid segment ID. Must not be empty.")

	for _, identifier := range append([]string{"a\n", "a\r"}, invalidIdentifierVectors...) {
		_, err := channel.Segment(identifier)
		assertConfigurationError(t, err, "Invalid segment ID. Must not contain CR, LF or invalid UTF-8.")
	}

	if channel.DefaultSegment().ID() != "default" {
		t.Fatal("default segment id")
	}
} // end function TestSegmentIDsAreValidated
