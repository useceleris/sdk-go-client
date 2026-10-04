package celeris_test

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"time"

	celeris "github.com/useceleris/sdk-go-client"
)

// fetchCredentials stands for a call to your own credential endpoint, which
// signs credentials with the server SDK.
func fetchCredentials(context.Context, celeris.CredentialRequest) (celeris.Credentials, error) {
	return celeris.Credentials{}, errors.New("call your credential endpoint here")
}

func Example() {
	client, err := celeris.NewClient(celeris.ClientOptions{CredentialProvider: fetchCredentials})

	if err != nil {
		log.Fatal(err)
	}

	channel, err := client.Channel("room-42")

	if err != nil {
		log.Fatal(err)
	}

	defer channel.Close()

	chat, err := channel.Segment("chat")

	if err != nil {
		log.Fatal(err)
	}

	chat.OnMessage(func(payload []byte, metadata celeris.MessageMetadata) {
		fmt.Println(metadata.MessageID, string(payload))
	})

	if _, err := chat.Subscribe(); err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	if err := channel.Connect(ctx); err != nil {
		log.Fatal(err)
	}

	if err := chat.Publish(ctx, celeris.TextPayload("hello")); err != nil {
		log.Fatal(err)
	}
}

// Listeners run one at a time and never under an SDK lock, so they may call
// back into the channel. A presence query's reply is routed by the goroutine
// running listeners, so query from a goroutine of your own.
func ExampleSegment_PresenceList() {
	var chat *celeris.Segment // from channel.Segment("chat")

	chat.OnPresence(func(celeris.PresenceEvent) {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			page, err := chat.PresenceList(ctx, 1, 25)

			if err != nil {
				log.Println("presence:", err)

				return
			}

			fmt.Println(page.Total, "connections present")
		}()
	})
}

func ExampleChannelEventHandler_OnRecovery() {
	var channel *celeris.Channel // from client.Channel("room-42")

	channel.Events().OnRecovery(func(event celeris.RecoveryEvent) {
		// Replay is a bounded window: gaps and duplicates are always possible.
		fmt.Println("recovered on retry", event.RetryIndex)
	})
}

// Match the SDK's own failures by code, and the server's by type.
func ExampleErrorCode() {
	describe := func(err error) string {
		if serverError, ok := errors.AsType[*celeris.ServerError](err); ok && serverError.Type == celeris.MessageSizeLimitError {
			return "shrink the payload"
		}

		switch {
		case errors.Is(err, celeris.ErrBackpressure):
			return "slow down and retry"
		case errors.Is(err, celeris.ErrNotConnected):
			return "wait for the connection"
		default:
			return "unexpected: " + err.Error()
		}
	}

	fmt.Println(describe(&celeris.Error{Code: celeris.ErrBackpressure}))
	fmt.Println(describe(&celeris.ServerError{Type: celeris.MessageSizeLimitError, Message: "Message size limit exceeded"}))
	// Output:
	// slow down and retry
	// shrink the payload
}

func ExampleJSONPayload() {
	payload, err := celeris.JSONPayload(map[string]any{"text": "<hi>", "count": 2})

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(payload))
	// Output: {"count":2,"text":"<hi>"}
}

func ExampleReadJSON() {
	type message struct {
		Text string `json:"text"`
	}

	decoded, err := celeris.ReadJSON[message]([]byte(`{"text":"hello"}`))

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(decoded.Text)

	_, err = celeris.ReadJSON[message]([]byte("not json"))
	fmt.Println(err)
	// Output:
	// hello
	// Payload is valid UTF-8 but not valid JSON.
}

// Wrap any serializer in the shape of the built-in helpers. Its own errors
// pass through unchanged.
func ExampleNewPayloadCodec() {
	type position struct{ X, Y int }

	codec, err := celeris.NewPayloadCodec(
		func(value position) ([]byte, error) {
			var encoded bytes.Buffer
			err := gob.NewEncoder(&encoded).Encode(value)

			return encoded.Bytes(), err
		},
		func(payload []byte) (position, error) {
			var value position
			err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&value)

			return value, err
		},
	)

	if err != nil {
		log.Fatal(err)
	}

	payload, err := codec.EncodePayload(position{X: 3, Y: 4})

	if err != nil {
		log.Fatal(err)
	}

	decoded, err := codec.ReadPayload(payload)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(decoded.X, decoded.Y)
	// Output: 3 4
}

func ExampleReadText() {
	text, err := celeris.ReadText(celeris.TextPayload("héllo"))
	fmt.Println(text, err)

	_, err = celeris.ReadText([]byte{0xff})
	fmt.Println(err)
	// Output:
	// héllo <nil>
	// Payload is not valid UTF-8, so it cannot be read as text.
}

func ExampleCredentials() {
	credentials := celeris.Credentials{Payload: "opaque", Signature: "opaque"}
	fmt.Println(credentials)
	fmt.Printf("%#v\n", credentials)
	// Output:
	// celeris.Credentials{redacted}
	// celeris.Credentials{redacted}
}
