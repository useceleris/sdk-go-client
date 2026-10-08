package live

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	celeris "github.com/useceleris/sdk-go-client"
)

// lifecycleSetup is a channel behind a fresh dropping proxy that records its
// credential requests, state changes and reported errors.
type lifecycleSetup struct {
	proxy   *droppingProxy
	channel *celeris.Channel

	mutex    sync.Mutex
	requests []celeris.CredentialRequest
	states   []celeris.ChannelState
	reported []error
} // end struct lifecycleSetup

// proxiedChannel leaves the channel unconnected. options sets any client
// option except the provider and the base URL.
func proxiedChannel(t *testing.T, label string, options celeris.ClientOptions) *lifecycleSetup {
	t.Helper()

	setup := &lifecycleSetup{proxy: newDroppingProxy(t)}
	options.BaseURL = setup.proxy.url()
	options.AllowInsecureLoopback = true
	options.CredentialProvider = func(_ context.Context, request celeris.CredentialRequest) (celeris.Credentials, error) {
		setup.mutex.Lock()
		setup.requests = append(setup.requests, request)
		setup.mutex.Unlock()

		return signCredentials(clientID(), signingSecret()), nil
	}

	client, err := celeris.NewClient(options)

	if err != nil {
		t.Fatal(err)
	}

	setup.channel = newChannel(t, client, uniqueChannelReference(label))
	setup.channel.Events().OnStateChange(func(state celeris.ChannelState) {
		setup.mutex.Lock()
		setup.states = append(setup.states, state)
		setup.mutex.Unlock()
	})

	setup.channel.Events().OnError(func(err error) {
		setup.mutex.Lock()
		setup.reported = append(setup.reported, err)
		setup.mutex.Unlock()
	})

	return setup
} // end function proxiedChannel

func (setup *lifecycleSetup) credentialRequests() []celeris.CredentialRequest {
	setup.mutex.Lock()
	defer setup.mutex.Unlock()

	return slices.Clone(setup.requests)
} // end method credentialRequests

func (setup *lifecycleSetup) stateChanges() []celeris.ChannelState {
	setup.mutex.Lock()
	defer setup.mutex.Unlock()

	return slices.Clone(setup.states)
} // end method stateChanges

func (setup *lifecycleSetup) reportedErrors() []error {
	setup.mutex.Lock()
	defer setup.mutex.Unlock()

	return slices.Clone(setup.reported)
} // end method reportedErrors

func (setup *lifecycleSetup) connect(t *testing.T) {
	t.Helper()

	if err := setup.channel.Connect(t.Context()); err != nil {
		t.Fatal(err)
	}
} // end method connect

// startOutage cuts every connection and refuses new ones.
func (setup *lifecycleSetup) startOutage() {
	setup.proxy.setRefusing(true)
	setup.proxy.dropAll()
} // end method startOutage

// connectInBackground starts a connect and returns the wait for its error.
func connectInBackground(ctx context.Context, channel *celeris.Channel) func() error {
	result := make(chan error, 1)

	go func() { result <- channel.Connect(ctx) }()

	return func() error { return <-result }
} // end function connectInBackground

func untilState(t *testing.T, channel *celeris.Channel, state celeris.ChannelState, timeout time.Duration) {
	t.Helper()

	eventually(t, func() bool { return channel.State() == state }, "the "+string(state)+" state", timeout)
} // end function untilState

func assertState(t *testing.T, channel *celeris.Channel, want celeris.ChannelState) {
	t.Helper()

	if state := channel.State(); state != want {
		t.Fatalf("state %s, want %s", state, want)
	}
} // end function assertState

func TestFailsAConnectAtItsConnectTimeoutWhenTheServerNeverAnswers(t *testing.T) {
	setup := proxiedChannel(t, "lifecycle-timeout", celeris.ClientOptions{ConnectTimeout: 2 * time.Second})
	setup.proxy.setBlackhole(true)

	started := time.Now()
	err := setup.channel.Connect(t.Context())
	elapsed := time.Since(started)

	if !errors.Is(err, celeris.ErrTimeout) || err.Error() != "Connection attempt timed out after 2s." {
		t.Fatalf("got %v", err)
	}

	if elapsed < 1900*time.Millisecond || elapsed >= 4*time.Second {
		t.Fatalf("failed after %v", elapsed)
	}

	assertState(t, setup.channel, celeris.StateFailed)

	if states := setup.stateChanges(); !slices.Equal(states, []celeris.ChannelState{celeris.StateConnecting, celeris.StateFailed}) {
		t.Fatalf("states %v", states)
	}

	// One failure, one report: the caller only (LIFE-02).
	if reported := setup.reportedErrors(); len(reported) != 0 {
		t.Fatalf("also reported to OnError: %v", reported)
	}
} // end function TestFailsAConnectAtItsConnectTimeoutWhenTheServerNeverAnswers

func TestTimesOutAtA1msConnectTimeoutAgainstTheRealServer(t *testing.T) {
	client, err := celeris.NewClient(celeris.ClientOptions{
		CredentialProvider: func(context.Context, celeris.CredentialRequest) (celeris.Credentials, error) {
			return signCredentials(clientID(), signingSecret()), nil
		},
		BaseURL:               websocketURL(),
		AllowInsecureLoopback: true,
		ConnectTimeout:        time.Millisecond,
	})

	if err != nil {
		t.Fatal(err)
	}

	channel := newChannel(t, client, uniqueChannelReference("lifecycle-1ms"))

	if err := channel.Connect(t.Context()); !errors.Is(err, celeris.ErrTimeout) {
		t.Fatalf("got %v", err)
	}

	assertState(t, channel, celeris.StateFailed)
} // end function TestTimesOutAtA1msConnectTimeoutAgainstTheRealServer

func TestCancelsAConnectFromItsContext(t *testing.T) {
	setup := proxiedChannel(t, "lifecycle-abort", celeris.ClientOptions{})
	setup.proxy.setBlackhole(true)
	ctx, cancel := context.WithCancel(t.Context())

	defer cancel()

	connected := connectInBackground(ctx, setup.channel)
	time.Sleep(500 * time.Millisecond)
	cancel()

	if err := connected(); !errors.Is(err, celeris.ErrCancelled) {
		t.Fatalf("got %v", err)
	}

	assertState(t, setup.channel, celeris.StateFailed)
} // end function TestCancelsAConnectFromItsContext

func TestClosesAChannelThatIsStillConnecting(t *testing.T) {
	setup := proxiedChannel(t, "lifecycle-close-connecting", celeris.ClientOptions{})
	setup.proxy.setBlackhole(true)

	connected := connectInBackground(t.Context(), setup.channel)
	time.Sleep(500 * time.Millisecond)
	setup.channel.Close()

	if err := connected(); !errors.Is(err, celeris.ErrCancelled) {
		t.Fatalf("got %v", err)
	}

	assertState(t, setup.channel, celeris.StateClosed)

	if err := setup.channel.Connect(t.Context()); !errors.Is(err, celeris.ErrNotConnected) {
		t.Fatalf("connect after close: %v", err)
	}
} // end function TestClosesAChannelThatIsStillConnecting

func TestClosesAChannelThatIsReconnectingAndStopsItsRetries(t *testing.T) {
	setup := proxiedChannel(t, "lifecycle-close-reconnecting", celeris.ClientOptions{})
	setup.connect(t)
	setup.startOutage()
	untilState(t, setup.channel, celeris.StateReconnecting, 5*time.Second)
	time.Sleep(time.Second)

	setup.channel.Close()
	requestsAtClose := len(setup.credentialRequests())
	setup.proxy.setRefusing(false)
	time.Sleep(5 * time.Second)

	assertState(t, setup.channel, celeris.StateClosed)

	if requests := len(setup.credentialRequests()); requests != requestsAtClose {
		t.Fatalf("%d credential requests after close, %d at close", requests, requestsAtClose)
	}
} // end function TestClosesAChannelThatIsReconnectingAndStopsItsRetries

// QUEUE-01: a publish while reconnecting waits for the reconnect (see
// reconnect_test.go); once recovery has stopped in failed, nothing is in
// progress to wait for.
func TestRejectsAPublishAfterFailed(t *testing.T) {
	setup := proxiedChannel(t, "lifecycle-publish-failed", celeris.ClientOptions{MaximumReconnectAttempts: 1})
	setup.connect(t)
	setup.startOutage()
	untilState(t, setup.channel, celeris.StateFailed, 60*time.Second)

	err := segment(t, setup.channel, "chat").Publish(t.Context(), []byte("x"))

	if !errors.Is(err, celeris.ErrNotConnected) || err.Error() != "Channel is not connected; it is failed." {
		t.Fatalf("got %v", err)
	}
} // end function TestRejectsAPublishAfterFailed

func TestRestartsFromFailedWithAnExplicitConnectAndSendsHeldSubscriptions(t *testing.T) {
	setup := proxiedChannel(t, "lifecycle-restart", celeris.ClientOptions{})
	chat := segment(t, setup.channel, "chat")
	received := collect(chat)
	subscribe(t, chat)
	setup.proxy.setRefusing(true)

	if err := setup.channel.Connect(t.Context()); !errors.Is(err, celeris.ErrTransport) {
		t.Fatalf("got %v", err)
	}

	assertState(t, setup.channel, celeris.StateFailed)

	setup.proxy.setRefusing(false)
	setup.connect(t)
	requests := setup.credentialRequests()
	publisher := connectedChannel(t, requests[0].ChannelReference)
	settle()
	publishAndAwait(t, publisher, setup.channel, "chat", "after-restart")

	if len(requests) != 2 || requests[0].Reconnect || requests[1].Reconnect {
		t.Fatalf("requests %+v, want two initial ones", requests)
	}

	assertPayloads(t, received, "after-restart")
} // end function TestRestartsFromFailedWithAnExplicitConnectAndSendsHeldSubscriptions

func TestFailsAfterTenFailedReconnectAttemptsAndReportsIt(t *testing.T) {
	setup := proxiedChannel(t, "lifecycle-exhaustion", celeris.ClientOptions{})
	setup.connect(t)
	failed := awaitEvent(t, setup.channel.Events().OnStateChange, func(state celeris.ChannelState) bool { return state == celeris.StateFailed }, "the failed state", 200*time.Second)
	setup.startOutage()
	failed()

	if reconnects := reconnectRequests(setup.credentialRequests()); len(reconnects) != 10 {
		t.Fatalf("%d reconnect credential requests, want 10", len(reconnects))
	}

	if reported := setup.reportedErrors(); len(reported) != 1 || !errors.Is(reported[0], celeris.ErrTransport) {
		t.Fatalf("reported %v, want one Transport error", reported)
	}

	states := setup.stateChanges()

	if !slices.Equal(states[len(states)-2:], []celeris.ChannelState{celeris.StateReconnecting, celeris.StateFailed}) {
		t.Fatalf("states %v", states)
	}
} // end function TestFailsAfterTenFailedReconnectAttemptsAndReportsIt

func TestFailsAfterTheConfiguredMaximumOf2ReconnectAttempts(t *testing.T) {
	setup := proxiedChannel(t, "lifecycle-maximum", celeris.ClientOptions{MaximumReconnectAttempts: 2})
	setup.connect(t)
	failed := awaitEvent(t, setup.channel.Events().OnStateChange, func(state celeris.ChannelState) bool { return state == celeris.StateFailed }, "the failed state", 60*time.Second)
	setup.startOutage()
	failed()

	if reconnects := reconnectRequests(setup.credentialRequests()); len(reconnects) != 2 {
		t.Fatalf("%d reconnect credential requests, want 2", len(reconnects))
	}

	if reported := setup.reportedErrors(); len(reported) != 1 || !errors.Is(reported[0], celeris.ErrTransport) {
		t.Fatalf("reported %v, want one Transport error", reported)
	}

	states := setup.stateChanges()

	if !slices.Equal(states[len(states)-2:], []celeris.ChannelState{celeris.StateReconnecting, celeris.StateFailed}) {
		t.Fatalf("states %v", states)
	}
} // end function TestFailsAfterTheConfiguredMaximumOf2ReconnectAttempts

func TestResetsTheRetryBudgetAfterSixtySecondsConnected(t *testing.T) {
	setup := proxiedChannel(t, "lifecycle-budget", celeris.ClientOptions{})
	setup.connect(t)

	// A first outage uses at least two failed attempts before it recovers:
	// once a third reconnect request is made, the first two have failed.
	setup.startOutage()
	eventually(t, func() bool { return len(setup.credentialRequests()) >= 4 }, "two failed reconnect attempts", 20*time.Second)
	firstRecovery := awaitEvent(t, setup.channel.Events().OnRecovery, func(celeris.RecoveryEvent) bool { return true }, "the first recovery", 30*time.Second)
	setup.proxy.setRefusing(false)

	if recovery := firstRecovery(); recovery.RetryIndex < 2 {
		t.Fatalf("first recovery %+v, want a retry index of at least 2", recovery)
	}

	// After sixty seconds connected, the next outage starts a new budget.
	time.Sleep(61 * time.Second)
	secondRecovery := awaitEvent(t, setup.channel.Events().OnRecovery, func(celeris.RecoveryEvent) bool { return true }, "the second recovery", 30*time.Second)
	setup.proxy.dropAll()

	if recovery := secondRecovery(); recovery.RetryIndex != 0 {
		t.Fatalf("second recovery %+v, want a retry index of 0", recovery)
	}
} // end function TestResetsTheRetryBudgetAfterSixtySecondsConnected

func TestKeepsAnIdleConnectionOpenPastTheServers60SecondHeartbeat(t *testing.T) {
	reference := uniqueChannelReference("lifecycle-idle")
	publisher := connectedChannel(t, reference)
	receiver := connectedChannel(t, reference)
	stateChanges := make(chan celeris.ChannelState, 16)
	receiver.Events().OnStateChange(func(state celeris.ChannelState) { stateChanges <- state })
	subscribe(t, segment(t, receiver, "chat"))

	// The client answers the server's pings and sends its own; nothing else
	// is sent.
	time.Sleep(95 * time.Second)
	publishAndAwait(t, publisher, receiver, "chat", "still-here")

	select {
	case state := <-stateChanges:
		t.Fatalf("the idle channel became %s", state)
	default:
	}

	assertState(t, receiver, celeris.StateConnected)
} // end function TestKeepsAnIdleConnectionOpenPastTheServers60SecondHeartbeat
