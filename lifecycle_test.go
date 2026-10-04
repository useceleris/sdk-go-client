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
}

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
}

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
}

func TestConnectAfterCloseIsRejected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _ := newTestChannel(t)
		channel.Close()
		assertCode(t, channel.Connect(t.Context()), ErrNotConnected)

		if channel.State() != StateClosed {
			t.Fatalf("state %s, want closed", channel.State())
		}
	})
}

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
}

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
}

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
}

func TestConnectDeadlineCoversCredentialsAndHandshake(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server := newTestChannel(t)
		channel.client.connectTimeout = 5 * time.Second
		server.set(func(server *fakeServer) { server.blockProvider = true })
		start := time.Now()

		assertCode(t, channel.Connect(t.Context()), ErrTimeout)

		if elapsed := time.Since(start); elapsed != 5*time.Second {
			t.Fatalf("timed out after %v, want 5s", elapsed)
		}

		server.set(func(server *fakeServer) {
			server.blockProvider = false
			server.blockDials = true
		})

		assertCode(t, channel.Connect(t.Context()), ErrTimeout)

		if channel.State() != StateFailed {
			t.Fatalf("state %s, want failed", channel.State())
		}
	})
}

func TestCallerDeadlineIsATimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server := newTestChannel(t)
		server.set(func(server *fakeServer) { server.blockProvider = true })
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()

		assertCode(t, channel.Connect(ctx), ErrTimeout)
	})
}

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
}

func TestProviderPanicIsATransportFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _ := newTestChannel(t)
		channel.client.credentialProvider = func(context.Context, CredentialRequest) (Credentials, error) {
			panic("synthetic-secret")
		}

		assertCode(t, channel.Connect(t.Context()), ErrTransport)
	})
}

func TestInvalidCredentialsAreAConfigurationError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server := newTestChannel(t)
		server.set(func(server *fakeServer) { server.credentials = Credentials{Payload: "secret-payload"} })

		err := channel.Connect(t.Context())
		assertConfigurationError(t, err, "Invalid credentials. Signature: Must not be empty.")
	})
}

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
}

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
}

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
}

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
}

func TestNilListenersAreRefused(t *testing.T) {
	channel, _ := newTestChannel(t)

	defer func() {
		if recover() == nil {
			t.Fatal("a nil listener was accepted")
		}
	}()

	channel.Events().OnError(nil)
}

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
}

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
}

func TestClientOptionsAreValidated(t *testing.T) {
	provider := (&fakeServer{}).provide
	cases := []struct {
		options ClientOptions
		message string
	}{
		{ClientOptions{}, "Invalid client options. CredentialProvider: Required."},
		{ClientOptions{CredentialProvider: provider, ConnectTimeout: -1, PresenceQueryTimeout: -1}, "Invalid client options. ConnectTimeout: Must not be negative. PresenceQueryTimeout: Must not be negative."},
		{ClientOptions{CredentialProvider: provider, BaseURL: "https://example.test"}, "Invalid connection URL. BaseURL must use wss://, or ws:// for a loopback host when AllowInsecureLoopback is true."},
	}

	for _, test := range cases {
		_, err := NewClient(test.options)
		assertConfigurationError(t, err, test.message)
	}

	client, err := NewClient(ClientOptions{CredentialProvider: provider})

	if err != nil {
		t.Fatal(err)
	}

	// ENDPOINT-01: consumers do not configure where Celeris lives.
	if client.baseURL.String() != "wss://realtime.useceleris.com" || client.connectTimeout != 15*time.Second || client.presenceQueryTimeout != 10*time.Second {
		t.Fatalf("defaults %v %v %v", client.baseURL, client.connectTimeout, client.presenceQueryTimeout)
	}
}
