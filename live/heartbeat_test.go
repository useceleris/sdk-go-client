package live

import (
	"context"
	"testing"
	"time"

	celeris "github.com/useceleris/sdk-go-client"
)

// HEARTBEAT-01: the server closes a connection it has heard no ping or pong
// from for 60 seconds, and the client answers the server's pings only while it
// reads. A listener holding delivery for 90 seconds keeps its connection,
// because the client pings on its own, and the time it holds the receiver
// never counts against a ping.
func TestAListenerBlockingNinetySecondsKeepsItsConnection(t *testing.T) {
	reference := uniqueChannelReference("heartbeat")
	blocking := connectedChannel(t, reference)
	peer := connectedChannel(t, reference)
	blockingChat := segment(t, blocking, "chat")
	peerChat := segment(t, peer, "chat")
	subscribe(t, blockingChat)
	subscribe(t, peerChat)

	stateChanges := make(chan celeris.ChannelState, 16)
	blocking.Events().OnStateChange(func(state celeris.ChannelState) { stateChanges <- state })

	listenerStarted := make(chan struct{}, 1)
	listenerReturned := make(chan struct{}, 1)
	blockingChat.OnMessage(func(payload []byte, _ celeris.MessageMetadata) {
		if string(payload) != "block" {
			return
		}

		listenerStarted <- struct{}{}
		time.Sleep(90 * time.Second)
		listenerReturned <- struct{}{}
	})

	peerReceived := make(chan struct{}, 1)
	peerChat.OnMessage(func(payload []byte, _ celeris.MessageMetadata) {
		if string(payload) != "after" {
			return
		}

		select {
		case peerReceived <- struct{}{}:
		default:
		}
	})

	time.Sleep(1500 * time.Millisecond)

	if err := peerChat.Publish(t.Context(), []byte("block")); err != nil {
		t.Fatal(err)
	}

	select {
	case <-listenerStarted:
	case <-time.After(15 * time.Second):
		t.Fatal("the blocking listener never ran")
	}

	select {
	case <-listenerReturned:
	case <-time.After(2 * time.Minute):
		t.Fatal("the blocking listener never returned")
	}

	// A close the server sent while the listener ran is read now.
	time.Sleep(3 * time.Second)

	select {
	case state := <-stateChanges:
		t.Fatalf("the blocking channel became %s", state)
	default:
	}

	if err := blockingChat.Publish(t.Context(), []byte("after")); err != nil {
		t.Fatal(err)
	}

	select {
	case <-peerReceived:
	case <-time.After(15 * time.Second):
		t.Fatal("the publish after the listener returned was not delivered")
	}

	if state := blocking.State(); state != celeris.StateConnected {
		t.Fatalf("the blocking channel is %s", state)
	}
} // end function TestAListenerBlockingNinetySecondsKeepsItsConnection

// HEARTBEAT-01: a path that silently stops carrying anything, while TCP to the
// proxy stays up, is found dead once a ping goes 15 seconds of reading time
// unanswered. The next ping goes out within 20 seconds, so the channel starts
// recovering 15 to 35 seconds after the path died, asking to replay the whole
// silence.
func TestASilentlyDeadPathIsFoundAndRecovered(t *testing.T) {
	proxy := newDroppingProxy(t)
	reconnects := make(chan celeris.CredentialRequest, 4)
	client := clientWith(t, proxy.url(), func(_ context.Context, request celeris.CredentialRequest) (celeris.Credentials, error) {
		if request.Reconnect {
			reconnects <- request
		}

		return signCredentials(clientID(), signingSecret()), nil
	})

	channel := newChannel(t, client, uniqueChannelReference("blackhole"))

	if err := channel.Connect(t.Context()); err != nil {
		t.Fatal(err)
	}

	stateChanges := make(chan celeris.ChannelState, 16)
	channel.Events().OnStateChange(func(state celeris.ChannelState) { stateChanges <- state })
	proxy.blackholeAll()
	diedAt := time.Now()
	deadline := time.After(45 * time.Second)
	var foundDeadAfter time.Duration

	for _, want := range []celeris.ChannelState{celeris.StateReconnecting, celeris.StateConnected} {
		select {
		case state := <-stateChanges:
			if state != want {
				t.Fatalf("the channel became %s, want %s", state, want)
			}
		case <-deadline:
			t.Fatalf("not %s within 45 s of the path dying", want)
		}

		elapsed := time.Since(diedAt)
		t.Logf("%s %v after the path died", want, elapsed.Round(time.Millisecond))

		if want != celeris.StateReconnecting {
			continue
		}

		foundDeadAfter = elapsed

		// A ping in flight when the path died counts from when it was sent.
		if elapsed < 15*time.Second-500*time.Millisecond {
			t.Fatalf("found dead after %v, before a ping could go 15 s unanswered", elapsed)
		}
	}

	// The outage began when the server was last heard from, before the path
	// died, so the lookback covers the silence plus five seconds.
	select {
	case request := <-reconnects:
		t.Logf("replay lookback %v", request.ReplayLookback)

		if request.ReplayLookback < foundDeadAfter+5*time.Second-time.Second {
			t.Fatalf("replay lookback %v misses a silence of %v", request.ReplayLookback, foundDeadAfter)
		}
	default:
		t.Fatal("recovered without a reconnect credential request")
	}

	if err := channel.DefaultSegment().Publish(t.Context(), []byte("after")); err != nil {
		t.Fatal(err)
	}
} // end function TestASilentlyDeadPathIsFoundAndRecovered
