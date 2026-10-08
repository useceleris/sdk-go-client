package celeris

import (
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// CONFIG-01: every option is validated by NewClient and honoured through it.

func TestClientOptionsAreValidated(t *testing.T) {
	provider := (&fakeServer{}).provide
	cases := []struct {
		options ClientOptions
		message string
	}{
		{ClientOptions{}, "Invalid client options. CredentialProvider: Required."},
		{ClientOptions{CredentialProvider: provider, ConnectTimeout: -1, ReconnectTimeout: -1, PresenceQueryTimeout: -1, PublishQueueSize: -1, DeduplicationWindowSize: -1, MaximumReconnectAttempts: -1}, "Invalid client options. ConnectTimeout: Must be 0 for the default, or from 1 ms to 15 minutes. ReconnectTimeout: Must be 0 for the default, or from 1 ms to 15 minutes. PresenceQueryTimeout: Must be 0 for the default, or from 1 ms to 15 minutes. PublishQueueSize: Must not be negative. DeduplicationWindowSize: Must not be negative. MaximumReconnectAttempts: Must be 0 for the default, or from 1 to 100."},
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
	if client.baseURL.String() != "wss://realtime.useceleris.com" {
		t.Fatalf("base URL %v", client.baseURL)
	}
} // end function TestClientOptionsAreValidated

func TestTimeoutOptionsAcceptOneMillisecondToFifteenMinutes(t *testing.T) {
	provider := (&fakeServer{}).provide
	timeouts := map[string]func(options *ClientOptions, timeout time.Duration){
		"ConnectTimeout":       func(options *ClientOptions, timeout time.Duration) { options.ConnectTimeout = timeout },
		"ReconnectTimeout":     func(options *ClientOptions, timeout time.Duration) { options.ReconnectTimeout = timeout },
		"PresenceQueryTimeout": func(options *ClientOptions, timeout time.Duration) { options.PresenceQueryTimeout = timeout },
	}

	for field, set := range timeouts {
		for _, timeout := range []time.Duration{-time.Millisecond, -time.Nanosecond, time.Millisecond - time.Nanosecond, 15*time.Minute + time.Millisecond, 15*time.Minute + time.Nanosecond} {
			options := ClientOptions{CredentialProvider: provider}
			set(&options, timeout)
			_, err := NewClient(options)
			assertConfigurationError(t, err, "Invalid client options. "+field+": Must be 0 for the default, or from 1 ms to 15 minutes.")
		}

		for _, timeout := range []time.Duration{0, time.Millisecond, 15 * time.Minute} {
			options := ClientOptions{CredentialProvider: provider}
			set(&options, timeout)

			if _, err := NewClient(options); err != nil {
				t.Errorf("%s %v refused: %v", field, timeout, err)
			}
		}
	}
} // end function TestTimeoutOptionsAcceptOneMillisecondToFifteenMinutes

func TestSizeOptionsMustNotBeNegative(t *testing.T) {
	provider := (&fakeServer{}).provide

	_, err := NewClient(ClientOptions{CredentialProvider: provider, PublishQueueSize: -1})
	assertConfigurationError(t, err, "Invalid client options. PublishQueueSize: Must not be negative.")

	_, err = NewClient(ClientOptions{CredentialProvider: provider, DeduplicationWindowSize: -1})
	assertConfigurationError(t, err, "Invalid client options. DeduplicationWindowSize: Must not be negative.")

	if _, err := NewClient(ClientOptions{CredentialProvider: provider, PublishQueueSize: 1, DeduplicationWindowSize: 1}); err != nil {
		t.Fatalf("sizes of 1 refused: %v", err)
	}
} // end function TestSizeOptionsMustNotBeNegative

func TestMaximumReconnectAttemptsAcceptsOneToOneHundred(t *testing.T) {
	provider := (&fakeServer{}).provide

	for _, maximum := range []int{-1, 101} {
		_, err := NewClient(ClientOptions{CredentialProvider: provider, MaximumReconnectAttempts: maximum})
		assertConfigurationError(t, err, "Invalid client options. MaximumReconnectAttempts: Must be 0 for the default, or from 1 to 100.")
	}

	for _, maximum := range []int{0, 1, 100} {
		if _, err := NewClient(ClientOptions{CredentialProvider: provider, MaximumReconnectAttempts: maximum}); err != nil {
			t.Errorf("MaximumReconnectAttempts %d refused: %v", maximum, err)
		}
	}
} // end function TestMaximumReconnectAttemptsAcceptsOneToOneHundred

// halfJitterDelays are the waits before each reconnect attempt with jitter
// fixed at half: 250 ms doubling, capped at half of 30 s.
var halfJitterDelays = []time.Duration{
	250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
	15 * time.Second, 15 * time.Second, 15 * time.Second, 15 * time.Second, 15 * time.Second,
}

// dropIntoRefusedReconnects connects a channel through NewClient with jitter
// fixed at half, then drops its connection while every dial fails.
func dropIntoRefusedReconnects(t *testing.T, options ClientOptions) (*Channel, *fakeServer, *recorder[ChannelState], *recorder[error]) {
	t.Helper()

	channel, server, socket := connectClientChannel(t, options)
	channel.client.random = func() float64 { return 0.5 }
	states := recordStates(channel)
	errorsSeen := recordErrors(channel)
	server.set(func(server *fakeServer) { server.failDials = 1000 })
	socket.drop()
	synctest.Wait()

	return channel, server, states, errorsSeen
} // end function dropIntoRefusedReconnects

// expectRefusedAttemptAfter checks that the next reconnect attempt asks for
// credentials exactly delay after the previous one ended.
func expectRefusedAttemptAfter(t *testing.T, server *fakeServer, delay time.Duration) {
	t.Helper()

	before := len(server.credentialRequests())
	synctest.Sleep(delay - time.Millisecond)

	if len(server.credentialRequests()) != before {
		t.Fatalf("attempt before %v", delay)
	}

	synctest.Sleep(time.Millisecond)

	if len(server.credentialRequests()) != before+1 {
		t.Fatalf("no attempt after %v", delay)
	}
} // end function expectRefusedAttemptAfter

func countReconnectRequests(server *fakeServer) int {
	count := 0

	for _, request := range server.credentialRequests() {
		if request.Reconnect {
			count++
		}
	}

	return count
} // end function countReconnectRequests

func TestMaximumReconnectAttemptsEndsRecoveryAfterThatManyFailures(t *testing.T) {
	cases := map[string]struct{ maximum, failures int }{
		"one":     {1, 1},
		"three":   {3, 3},
		"default": {0, 10},
	}

	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				channel, server, states, errorsSeen := dropIntoRefusedReconnects(t, ClientOptions{MaximumReconnectAttempts: test.maximum})

				// Before each attempt the channel is still reconnecting, and
				// the attempt comes on time.
				for failure := range test.failures {
					if channel.State() != StateReconnecting {
						t.Fatalf("state %s after %d failures", channel.State(), failure)
					}

					expectRefusedAttemptAfter(t, server, halfJitterDelays[failure])
				}

				if channel.State() != StateFailed {
					t.Fatalf("state %s after %d failures", channel.State(), test.failures)
				}

				reported := errorsSeen.all()

				if len(reported) != 1 || !errors.Is(reported[0], ErrTransport) {
					t.Fatalf("errors %v", reported)
				}

				assertStates(t, states, StateReconnecting, StateFailed)
				synctest.Sleep(time.Hour)

				if got := countReconnectRequests(server); got != test.failures {
					t.Fatalf("%d reconnect requests, want %d", got, test.failures)
				}
			})
		})
	}
} // end function TestMaximumReconnectAttemptsEndsRecoveryAfterThatManyFailures

func TestMaximumOf100ReconnectAttemptsOutlastsTheDefault(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, _, errorsSeen := dropIntoRefusedReconnects(t, ClientOptions{MaximumReconnectAttempts: 100})

		for _, delay := range halfJitterDelays[:10] {
			expectRefusedAttemptAfter(t, server, delay)
		}

		if channel.State() != StateReconnecting {
			t.Fatalf("state %s after 10 failures", channel.State())
		}

		expectRefusedAttemptAfter(t, server, halfJitterDelays[10])

		if channel.State() != StateReconnecting || len(errorsSeen.all()) != 0 {
			t.Fatalf("state %s, errors %v after 11 failures", channel.State(), errorsSeen.all())
		}
	})
} // end function TestMaximumOf100ReconnectAttemptsOutlastsTheDefault

// With a maximum of 2, one failure and then a recovery leave one attempt in
// the budget unless the connection stays up for 60 s.
func TestRetryBudgetResetAppliesToTheConfiguredMaximum(t *testing.T) {
	// Without a reset, the retry index carries on from 1.
	cases := map[string]struct {
		connected time.Duration
		delays    []time.Duration
	}{
		"outage within 60 s":   {time.Second, halfJitterDelays[1:2]},
		"outage after 60 s up": {time.Minute, halfJitterDelays[:2]},
	}

	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				channel, server, _, errorsSeen := dropIntoRefusedReconnects(t, ClientOptions{MaximumReconnectAttempts: 2})
				expectRefusedAttemptAfter(t, server, halfJitterDelays[0])
				server.set(func(server *fakeServer) { server.failDials = 0 })
				socket := expectAttemptAfter(t, server, halfJitterDelays[1])

				if channel.State() != StateConnected {
					t.Fatalf("state %s after recovery", channel.State())
				}

				synctest.Sleep(test.connected)
				server.set(func(server *fakeServer) { server.failDials = 1000 })
				socket.drop()
				synctest.Wait()

				for failure, delay := range test.delays {
					if channel.State() != StateReconnecting {
						t.Fatalf("state %s after %d failures in the new outage", channel.State(), failure)
					}

					expectRefusedAttemptAfter(t, server, delay)
				}

				if channel.State() != StateFailed {
					t.Fatalf("state %s after %d failures in the new outage", channel.State(), len(test.delays))
				}

				if reported := errorsSeen.all(); len(reported) != 1 || !errors.Is(reported[0], ErrTransport) {
					t.Fatalf("errors %v", reported)
				}
			})
		})
	}
} // end function TestRetryBudgetResetAppliesToTheConfiguredMaximum

func TestLoopbackOptInThroughTheConstructor(t *testing.T) {
	provider := (&fakeServer{}).provide
	refused := "Invalid connection URL. BaseURL must use wss://, or ws:// for a loopback host when AllowInsecureLoopback is true."

	_, err := NewClient(ClientOptions{CredentialProvider: provider, BaseURL: "ws://localhost:19002"})
	assertConfigurationError(t, err, refused)

	for _, allow := range []bool{false, true} {
		_, err := NewClient(ClientOptions{CredentialProvider: provider, BaseURL: "ws://example.test", AllowInsecureLoopback: allow})
		assertConfigurationError(t, err, refused)
	}

	synctest.Test(t, func(t *testing.T) {
		_, _, socket := connectClientChannel(t, ClientOptions{BaseURL: "ws://localhost:19002", AllowInsecureLoopback: true})

		if !strings.HasPrefix(socket.url, "ws://localhost:19002/channel/room-1?") {
			t.Fatalf("url %q", socket.url)
		}
	})
} // end function TestLoopbackOptInThroughTheConstructor

// Each case lets a connect run past its deadline, then connects, queries
// presence and drops the connection to time the first reconnect attempt.
// Messages name the bound as Go prints a duration, so 15 minutes is "15m0s".
func TestTimeoutsHoldAtNonDefaultValues(t *testing.T) {
	cases := map[string]struct {
		options                      ClientOptions
		connect, reconnect, presence time.Duration
	}{
		"defaults":                         {ClientOptions{}, 15 * time.Second, 15 * time.Second, 10 * time.Second},
		"reconnect equal to connect":       {ClientOptions{ConnectTimeout: 3 * time.Second, ReconnectTimeout: 3 * time.Second}, 3 * time.Second, 3 * time.Second, 10 * time.Second},
		"reconnect longer than connect":    {ClientOptions{ConnectTimeout: 2 * time.Second, ReconnectTimeout: 7 * time.Second}, 2 * time.Second, 7 * time.Second, 10 * time.Second},
		"reconnect shorter than connect":   {ClientOptions{ConnectTimeout: 5 * time.Second, ReconnectTimeout: 3 * time.Second}, 5 * time.Second, 3 * time.Second, 10 * time.Second},
		"absent reconnect follows connect": {ClientOptions{ConnectTimeout: 4 * time.Second}, 4 * time.Second, 4 * time.Second, 10 * time.Second},
		"presence":                         {ClientOptions{PresenceQueryTimeout: 2 * time.Second}, 15 * time.Second, 15 * time.Second, 2 * time.Second},
		"largest":                          {ClientOptions{ConnectTimeout: 15 * time.Minute, ReconnectTimeout: 15 * time.Minute, PresenceQueryTimeout: 15 * time.Minute}, 15 * time.Minute, 15 * time.Minute, 15 * time.Minute},
	}

	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				channel, server := newClientChannel(t, test.options)
				err := expectConnectTimeout(t, channel, server, test.connect)

				if want := "Connection attempt timed out after " + test.connect.String() + "."; err.Error() != want {
					t.Fatalf("message %q, want %q", err.Error(), want)
				}

				if err := channel.Connect(t.Context()); err != nil {
					t.Fatal(err)
				}

				err = expectPresenceTimeout(t, channel, test.presence)

				if want := "Presence query timed out after " + test.presence.String() + "."; err.Error() != want {
					t.Fatalf("message %q, want %q", err.Error(), want)
				}

				expectReconnectTimeout(t, server, test.reconnect)
			})
		})
	}
} // end function TestTimeoutsHoldAtNonDefaultValues
