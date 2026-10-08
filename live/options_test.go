package live

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	celeris "github.com/useceleris/sdk-go-client"
)

// channelWith is a connected channel whose client has these non-default
// options, at baseURL.
func channelWith(t *testing.T, baseURL, reference string, options celeris.ClientOptions, claims ...claim) *celeris.Channel {
	t.Helper()

	options.BaseURL = baseURL
	options.AllowInsecureLoopback = true
	options.CredentialProvider = func(context.Context, celeris.CredentialRequest) (celeris.Credentials, error) {
		return signCredentials(clientID(), signingSecret(), claims...), nil
	}

	client, err := celeris.NewClient(options)

	if err != nil {
		t.Fatal(err)
	}

	channel := newChannel(t, client, reference)

	if err := channel.Connect(t.Context()); err != nil {
		t.Fatalf("connect: %v", err)
	}

	return channel
} // end function channelWith

func TestTimesOutAPresenceQueryAtA1msPresenceTimeoutAndStaysConnected(t *testing.T) {
	channel := channelWith(t, websocketURL(), uniqueChannelReference("option-presence"), celeris.ClientOptions{PresenceQueryTimeout: time.Millisecond})

	_, err := segment(t, channel, "room").PresenceList(t.Context(), 1, 10)

	if !errors.Is(err, celeris.ErrTimeout) || err.Error() != "Presence query timed out after 1ms." {
		t.Fatalf("got %v", err)
	}

	assertState(t, channel, celeris.StateConnected)
} // end function TestTimesOutAPresenceQueryAtA1msPresenceTimeoutAndStaysConnected

func TestDeliversReplayedIDsAgainBeyondADeduplicationWindowOf1(t *testing.T) {
	reference := uniqueChannelReference("option-window")
	publisher := connectedChannel(t, reference)
	receiver := channelWith(t, websocketURL(), reference, celeris.ClientOptions{DeduplicationWindowSize: 1}, claim{"replay", true})
	history := segment(t, receiver, "history")
	received := collect(history)
	first := subscribe(t, history)
	settle()

	publishText(t, publisher, "history", "one")
	publishAndAwait(t, publisher, receiver, "history", "two")

	// The window holds only "two". The re-join replays "one", which pushes
	// "two" out of the window, so the replayed "two" is delivered again too.
	first.Cancel()
	settle()
	subscribe(t, history)
	time.Sleep(4 * time.Second)

	assertPayloads(t, received, "one", "two", "one", "two")
} // end function TestDeliversReplayedIDsAgainBeyondADeduplicationWindowOf1

func TestRefusesTheSecondWaitingPublishWithAPublishQueueOf1(t *testing.T) {
	proxy := newDroppingProxy(t)
	channel := channelWith(t, proxy.url(), uniqueChannelReference("option-queue"), celeris.ClientOptions{PublishQueueSize: 1})
	bulk := segment(t, channel, "bulk")

	// The proxy stops reading, so the socket buffer fills and publishes wait.
	proxy.stallUpstream(true)
	payload := make([]byte, 900*1024)
	outcomes := make(chan string, 40)

	for range 40 {
		go func() {
			if err := bulk.Publish(t.Context(), payload); err != nil {
				outcomes <- err.Error()
			} else {
				outcomes <- "sent"
			}
		}()
	}

	time.Sleep(2 * time.Second)
	proxy.stallUpstream(false)
	var results []string

	for range 40 {
		select {
		case outcome := <-outcomes:
			results = append(results, outcome)
		case <-time.After(60 * time.Second):
			t.Fatalf("%d of 40 publishes settled", len(results))
		}
	}

	if !slices.Contains(results, "The publish queue is full (size 1). Retry once some publishes have gone out.") || !slices.Contains(results, "sent") {
		t.Fatalf("results %q", results)
	}
} // end function TestRefusesTheSecondWaitingPublishWithAPublishQueueOf1
