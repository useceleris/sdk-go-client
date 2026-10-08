package live

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	celeris "github.com/useceleris/sdk-go-client"
)

func TestConnectsWithValidCredentialsAndReceivesTheGreetings(t *testing.T) {
	channel := newChannel(t, qualificationClient(t, websocketURL()), uniqueChannelReference("auth"))
	var mutex sync.Mutex
	var notices []string
	channel.Events().OnNotice(func(notice celeris.ServerNotice) {
		mutex.Lock()
		notices = append(notices, string(notice.Payload))
		mutex.Unlock()
	})

	if err := channel.Connect(t.Context()); err != nil {
		t.Fatal(err)
	}

	waitFor(t, channel.Events().OnNotice, func(celeris.ServerNotice) bool {
		mutex.Lock()
		defer mutex.Unlock()

		return len(notices) >= 2
	}, "the connect and default-subscribe greetings", 15*time.Second)

	mutex.Lock()
	joined := strings.Join(notices, "\n")
	mutex.Unlock()

	if !strings.Contains(joined, "Successfully connected") || !strings.Contains(joined, `segment "default"`) {
		t.Fatalf("notices %q", joined)
	}
} // end function TestConnectsWithValidCredentialsAndReceivesTheGreetings

// DEV-02: a refused handshake is never labelled an authorization failure.
func TestRefusedCredentialsAreTransportFailures(t *testing.T) {
	cases := map[string]celeris.Credentials{
		"invalid signature": signCredentials(clientID(), "wrong-secret"),
		"unknown client":    signCredentials("no-such-client", signingSecret()),
		"expired":           signCredentials(clientID(), signingSecret(), claim{"timestamp", time.Now().Add(-61 * time.Second).UnixMilli()}),
		"an hour old":       signCredentials(clientID(), signingSecret(), claim{"timestamp", time.Now().Add(-59 * time.Minute).UnixMilli()}),
		"future":            signCredentials(clientID(), signingSecret(), claim{"timestamp", time.Now().Add(5 * time.Minute).UnixMilli()}),
		"other channel":     signCredentials(clientID(), signingSecret(), claim{"channel_references", []string{"some-other-channel"}}),

		"an empty reference":                   signCredentials(clientID(), signingSecret(), claim{"reference", ""}),
		"a payload that is not JSON":           signRawPayload(clientID(), signingSecret(), "not json"),
		"a payload without a timestamp":        signRawPayload(clientID(), signingSecret(), `{"reference":"x"}`),
		"a timestamp that is a string":         signRawPayload(clientID(), signingSecret(), `{"timestamp":"now"}`),
		"a timestamp 30 seconds in the future": signCredentials(clientID(), signingSecret(), claim{"timestamp", time.Now().Add(30 * time.Second).UnixMilli()}),
	}

	for name, credentials := range cases {
		t.Run(name, func(t *testing.T) {
			client := clientWith(t, websocketURL(), func(context.Context, celeris.CredentialRequest) (celeris.Credentials, error) {
				return credentials, nil
			})

			channel := newChannel(t, client, uniqueChannelReference("refused"))
			err := channel.Connect(t.Context())

			if !errors.Is(err, celeris.ErrTransport) || channel.State() != celeris.StateFailed {
				t.Fatalf("got %v, state %s", err, channel.State())
			}

			if strings.Contains(err.Error(), credentials.Signature) {
				t.Fatalf("error leaks the credentials: %v", err)
			}
		})
	}
} // end function TestRefusedCredentialsAreTransportFailures

func TestAcceptsATimestampInsideTheSixtySecondWindow(t *testing.T) {
	connectedChannel(t, uniqueChannelReference("window"), claim{"timestamp", time.Now().Add(-30 * time.Second).UnixMilli()})
} // end function TestAcceptsATimestampInsideTheSixtySecondWindow

func TestAcceptsAChannelInsideTheTokensRestriction(t *testing.T) {
	reference := uniqueChannelReference("allowed")

	if channel := connectedChannel(t, reference, claim{"channel_references", []string{reference}}); channel.State() != celeris.StateConnected {
		t.Fatalf("state %s", channel.State())
	}
} // end function TestAcceptsAChannelInsideTheTokensRestriction

func TestAcceptsAnEmptyChannelRestrictionWhichPermitsEveryChannel(t *testing.T) {
	channel := connectedChannel(t, uniqueChannelReference("any"), claim{"channel_references", []string{}})

	if channel.State() != celeris.StateConnected {
		t.Fatalf("state %s", channel.State())
	}
} // end function TestAcceptsAnEmptyChannelRestrictionWhichPermitsEveryChannel

func TestAcceptsAChannelThatIsOneOfSeveralInTheRestriction(t *testing.T) {
	reference := uniqueChannelReference("several")

	channel := connectedChannel(t, reference, claim{"channel_references", []string{"some-other-channel", reference}})

	if channel.State() != celeris.StateConnected {
		t.Fatalf("state %s", channel.State())
	}
} // end function TestAcceptsAChannelThatIsOneOfSeveralInTheRestriction

// Credentials are requested fresh for every attempt (D-001).
func TestRequestsFreshCredentialsForEveryConnect(t *testing.T) {
	var mutex sync.Mutex
	var requests []celeris.CredentialRequest
	client := clientWith(t, websocketURL(), func(_ context.Context, request celeris.CredentialRequest) (celeris.Credentials, error) {
		mutex.Lock()
		requests = append(requests, request)
		mutex.Unlock()

		return signCredentials(clientID(), signingSecret()), nil
	})

	for range 2 {
		channel := newChannel(t, client, uniqueChannelReference("fresh"))

		if err := channel.Connect(t.Context()); err != nil {
			t.Fatal(err)
		}

		channel.Close()
	}

	mutex.Lock()
	defer mutex.Unlock()

	if len(requests) != 2 || requests[0].Reconnect || requests[1].Reconnect {
		t.Fatalf("requests %+v", requests)
	}
} // end function TestRequestsFreshCredentialsForEveryConnect
