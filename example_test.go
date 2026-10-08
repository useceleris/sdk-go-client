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
} // end function fetchCredentials

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
} // end function Example

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
} // end function ExampleSegment_PresenceList

// A state listener sees every transition, in the order it happened.
func ExampleChannelEventHandler_OnStateChange() {
	var channel *celeris.Channel // from client.Channel("room-42")

	channel.Events().OnStateChange(func(state celeris.ChannelState) {
		switch state {
		case celeris.StateReconnecting:
			fmt.Println("connection lost; recovering")
		case celeris.StateFailed:
			fmt.Println("gave up; call Connect to try again")
		}
	})
} // end function ExampleChannelEventHandler_OnStateChange

// Failures no caller is waiting for arrive here: errors the server sent, and
// the SDK's own, such as a message that could not be decoded.
func ExampleChannelEventHandler_OnError() {
	var channel *celeris.Channel // from client.Channel("room-42")

	channel.Events().OnError(func(err error) {
		if serverError, ok := errors.AsType[*celeris.ServerError](err); ok {
			fmt.Println("server:", serverError.Type, serverError.SubType, serverError.Resource)

			return
		}

		fmt.Println("sdk:", err)
	})
} // end function ExampleChannelEventHandler_OnError

// A channel listener sees every delivery, from any segment, after that
// segment's own listeners.
func ExampleChannelEventHandler_OnMessage() {
	var channel *celeris.Channel // from client.Channel("room-42")

	removeChannelListener := channel.Events().OnMessage(func(payload []byte, metadata celeris.MessageMetadata) {
		fmt.Println(metadata.SegmentID, string(payload))
	})

	defer removeChannelListener()
} // end function ExampleChannelEventHandler_OnMessage

// Listeners receive the payload, then its metadata. Registering returns a
// function that removes the listener; a subscription is cancelled on its own.
func ExampleSegment_OnMessage() {
	var chat *celeris.Segment // from channel.Segment("chat")

	stop := chat.OnMessage(func(payload []byte, metadata celeris.MessageMetadata) {
		sent := time.UnixMilli(metadata.Timestamp)
		fmt.Println(metadata.TokenReference, "at", sent.Format(time.Kitchen), "said", string(payload))
	})

	defer stop()

	subscription, err := chat.Subscribe()

	if err != nil {
		log.Fatal(err)
	}

	defer subscription.Cancel()
} // end function ExampleSegment_OnMessage

// Returning nil means the local socket accepted the bytes: there is no
// receipt. ErrDeliveryUnknown means acceptance is uncertain, and the SDK never
// resends such a publish.
func ExampleSegment_PublishWithMessageID() {
	var chat *celeris.Segment // from channel.Segment("chat")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := chat.PublishWithMessageID(ctx, celeris.TextPayload("Order 1042 shipped"), "order-1042-shipped")

	switch {
	case err == nil:
		fmt.Println("handed to the socket")
	case errors.Is(err, celeris.ErrBackpressure):
		fmt.Println("the publish queue is full (PublishQueueSize, 64 by default); retry later")
	case errors.Is(err, celeris.ErrDeliveryUnknown):
		fmt.Println("may or may not have been sent; do not resend blindly")
	default:
		fmt.Println("not sent:", err)
	}
} // end function ExampleSegment_PublishWithMessageID

// A presence subscription delivers joins and leaves. It delivers no
// messages: watching presence is not membership.
func ExampleSegment_SubscribePresence() {
	var chat *celeris.Segment // from channel.Segment("chat")

	chat.OnPresence(func(event celeris.PresenceEvent) {
		if event.Joined {
			fmt.Println(event.TokenReference, "joined on connection", event.ConnectionID)
		} else {
			fmt.Println(event.TokenReference, "left from connection", event.ConnectionID)
		}
	})

	presence, err := chat.SubscribePresence()

	if err != nil {
		log.Fatal(err)
	}

	defer presence.Cancel()
} // end function ExampleSegment_SubscribePresence

// Read every page of a segment's presence.
func ExampleSegment_PresenceList_allPages() {
	var chat *celeris.Segment // from channel.Segment("chat")

	ctx := context.Background()

	for page := int32(1); ; page++ {
		result, err := chat.PresenceList(ctx, page, 100)

		if err != nil {
			log.Fatal(err)
		}

		for _, connection := range result.Connections {
			fmt.Println(connection.TokenReference, connection.ConnectionID)
		}

		if len(result.Connections) == 0 || result.To >= result.Total {
			break
		}
	}
} // end function ExampleSegment_PresenceList_allPages

// Encode a typed value as JSON, and read it back in a listener.
func ExampleJSONPayload_segment() {
	var chat *celeris.Segment // from channel.Segment("chat")

	ctx := context.Background()

	type typing struct {
		Active bool `json:"active"`
	}

	chat.OnMessage(func(payload []byte, _ celeris.MessageMetadata) {
		event, err := celeris.ReadJSON[typing](payload)

		if err != nil {
			return // not a typing event
		}

		fmt.Println("typing:", event.Active)
	})

	payload, err := celeris.JSONPayload(typing{Active: true})

	if err != nil {
		log.Fatal(err)
	}

	if err := chat.Publish(ctx, payload); err != nil {
		log.Fatal(err)
	}
} // end function ExampleJSONPayload_segment

func ExampleChannelEventHandler_OnRecovery() {
	var channel *celeris.Channel // from client.Channel("room-42")

	channel.Events().OnRecovery(func(event celeris.RecoveryEvent) {
		// Replay is a bounded window: gaps and duplicates are always possible.
		fmt.Println("recovered on retry", event.RetryIndex)
	})
} // end function ExampleChannelEventHandler_OnRecovery

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
} // end function ExampleErrorCode

func ExampleJSONPayload() {
	payload, err := celeris.JSONPayload(map[string]any{"text": "<hi>", "count": 2})

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(payload))
	// Output: {"count":2,"text":"<hi>"}
} // end function ExampleJSONPayload

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
} // end function ExampleReadJSON

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
} // end function ExampleNewPayloadCodec

func ExampleReadText() {
	text, err := celeris.ReadText(celeris.TextPayload("héllo"))
	fmt.Println(text, err)

	_, err = celeris.ReadText([]byte{0xff})
	fmt.Println(err)
	// Output:
	// héllo <nil>
	// Payload is not valid UTF-8, so it cannot be read as text.
} // end function ExampleReadText

func ExampleCredentials() {
	credentials := celeris.Credentials{Payload: "opaque", Signature: "opaque"}
	fmt.Println(credentials)
	fmt.Printf("%#v\n", credentials)
	// Output:
	// celeris.Credentials{redacted}
	// celeris.Credentials{redacted}
} // end function ExampleCredentials
