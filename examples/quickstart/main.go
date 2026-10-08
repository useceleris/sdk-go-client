// Command quickstart connects to Celeris, publishes on a segment it
// subscribes to, and reads who is present.
//
// A trusted server signs credentials; this client fetches them from your
// credential endpoint, such as the server module's credential-endpoint
// example, and never sees a signing secret. Run it with
// CELERIS_CREDENTIAL_URL pointing at that endpoint, CELERIS_SESSION holding
// the user's bearer token, and CELERIS_WS_URL at a local ws:// stack.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	celeris "github.com/useceleris/sdk-go-client"
)

// fetchCredentials asks your credential endpoint for fresh credentials. It is
// called once per connection attempt, reconnects included. The endpoint
// authenticates the user from their session and decides the claims; this
// example sends a bearer token from CELERIS_SESSION.
func fetchCredentials(ctx context.Context, request celeris.CredentialRequest) (celeris.Credentials, error) {
	body, err := json.Marshal(map[string]any{
		"channelReference": request.ChannelReference,
		"replayLookbackMs": request.ReplayLookback.Milliseconds(),
	})

	if err != nil {
		return celeris.Credentials{}, err
	}

	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, os.Getenv("CELERIS_CREDENTIAL_URL"), bytes.NewReader(body))

	if err != nil {
		return celeris.Credentials{}, err
	}

	httpRequest.Header.Set("Authorization", "Bearer "+os.Getenv("CELERIS_SESSION"))
	httpRequest.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(httpRequest)

	if err != nil {
		return celeris.Credentials{}, err
	}

	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK {
		return celeris.Credentials{}, fmt.Errorf("credential endpoint answered %s", response.Status)
	}

	var credentials celeris.Credentials
	err = json.NewDecoder(response.Body).Decode(&credentials)

	return credentials, err
} // end function fetchCredentials

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
} // end function main

func run(ctx context.Context) error {
	client, err := celeris.NewClient(celeris.ClientOptions{
		CredentialProvider: fetchCredentials,
		BaseURL:            os.Getenv("CELERIS_WS_URL"),
		// A local ws:// stack; production uses the built-in wss:// endpoint.
		AllowInsecureLoopback: true,
	})

	if err != nil {
		return err
	}

	channel, err := client.Channel("quickstart-" + strconv.FormatInt(time.Now().UnixMilli(), 10))

	if err != nil {
		return err
	}

	defer channel.Close()

	channel.Events().OnStateChange(func(state celeris.ChannelState) { fmt.Println("state:", state) })
	channel.Events().OnError(func(err error) { fmt.Println("error:", err) })

	if err := channel.Connect(ctx); err != nil {
		return err
	}

	chat, err := channel.Segment("chat")

	if err != nil {
		return err
	}

	var delivered atomic.Int32

	chat.OnMessage(func(payload []byte, metadata celeris.MessageMetadata) {
		// Payload first; metadata carries the sender, the id and the time.
		fmt.Println("received", metadata.MessageID, string(payload))
		delivered.Add(1)
	})

	membership, err := chat.Subscribe()

	if err != nil {
		return err
	}

	defer membership.Cancel()

	time.Sleep(time.Second)

	payload, err := celeris.JSONPayload(map[string]string{"hello": "world"})

	if err != nil {
		return err
	}

	// Returning means the local socket accepted the bytes, never a receipt.
	if err := chat.Publish(ctx, payload); err != nil {
		return err
	}

	for range 60 {
		if delivered.Load() > 0 {
			break
		}

		time.Sleep(250 * time.Millisecond)
	}

	// Presence: who is in the segment right now. One query may be in flight
	// per channel.
	page, err := chat.PresenceList(ctx, 1, 10)

	if err != nil {
		return err
	}

	fmt.Printf("example: ok delivered=%d present=%d\n", delivered.Load(), page.Total)

	return nil
} // end function run
