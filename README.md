# Celeris Go client

[![Go Reference](https://pkg.go.dev/badge/github.com/useceleris/sdk-go-client.svg)](https://pkg.go.dev/github.com/useceleris/sdk-go-client)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Realtime client for Celeris channels: connection lifecycle with automatic recovery, segment messaging, and presence.

**The model in three sentences.** A `Channel` is one WebSocket client: creating another `Channel`, even for the same reference, opens another socket. Every segment of that channel is multiplexed over that single connection, and connecting automatically makes you a member of the `"default"` segment. `Segment` handles are lightweight: create as many as you like, they share the socket and one interest count.

## Install

```sh
go get github.com/useceleris/sdk-go-client
```

Go 1.27 or newer. The package name is `celeris`, and its only dependency is `github.com/coder/websocket`.

## Quickstart

```go
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
```

[examples/quickstart](examples/quickstart/main.go) is the same flow as a complete program, with presence.

## Credentials

The client never signs. A `CredentialProvider` fetches fresh credentials from your own authenticated endpoint, which signs them with [sdk-go-server](https://github.com/useceleris/sdk-go-server). Never ship a signing secret in an application.

```go
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
}
```

The provider runs once per connection attempt, reconnects included, and must honour `ctx`: the connect deadline and `Close` both cancel it. On a reconnect, `request.Reconnect` is true and `ReplayLookback` suggests how much history to replay: the outage so far plus five seconds. Your endpoint decides whether to grant it. A provider's error is reported as `ErrTransport`, `ErrTimeout` or `ErrCancelled`, and its text is never passed on. `Credentials` print as redacted through `fmt` and `slog`.

## Channels and connection

`Connect` returns once the socket is up and your subscriptions are queued to be restored. A first connect that fails is not retried: the channel becomes `failed` and you may call `Connect` again. Only a connection that was up recovers automatically. `Close` is terminal and idempotent, returns within about five seconds, and fails waiting publishes and presence queries with `ErrCancelled`.

States run `idle` → `connecting` → `connected`, then `reconnecting` and back to `connected` on recovery, `failed` when recovery gives up, and `closing` → `closed` on `Close`.

```go
channel.Events().OnStateChange(func(state celeris.ChannelState) {
	switch state {
	case celeris.StateReconnecting:
		fmt.Println("connection lost; recovering")
	case celeris.StateFailed:
		fmt.Println("gave up; call Connect to try again")
	}
})
```

A channel reference is 1 to 255 ASCII letters, digits, `-` or `_`. A segment id is any non-empty valid UTF-8 without CR or LF.

## Subscribing and receiving

`Subscribe` registers interest in a segment. The first interest subscribes it, the last `Cancel` unsubscribes it, and subscriptions are restored after every reconnect. The protocol has no acknowledgement, so a denial arrives later through `OnError`. The default segment is joined on connect and never needs a subscription.

```go
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
```

The payload is the SDK's own copy, shared by every listener of that delivery: treat it as read-only. Each message id is delivered once within a 1024-id window.

## Publishing

`Publish` sends with a generated message id; `PublishWithMessageID` sends with yours. Returning `nil` means the local socket accepted the bytes, not that anyone received them.

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

err := chat.PublishWithMessageID(ctx, celeris.TextPayload("Order 1042 shipped"), "order-1042-shipped")

switch {
case err == nil:
	fmt.Println("handed to the socket")
case errors.Is(err, celeris.ErrBackpressure):
	fmt.Println("64 publishes are waiting; retry later")
case errors.Is(err, celeris.ErrDeliveryUnknown):
	fmt.Println("may or may not have been sent; resend with the same id")
default:
	fmt.Println("not sent:", err)
}
```

There is no offline queue: publishing while disconnected returns `ErrNotConnected`. A publish over your plan's payload cap still returns `nil` and is refused afterwards with a `MessageSizeLimitError` through `OnError`. Publishing to a segment joins it server-side.

## Payloads

Payloads are opaque bytes. `TextPayload` and `ReadText` handle UTF-8 text, `JSONPayload` and `ReadJSON[T]` handle JSON:

```go
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
```

`NewPayloadCodec` gives any other serializer, such as protobuf, MessagePack or CBOR, the same shape, without this module depending on it:

```go
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
```

`ReadJSON` asserts a type; it does not validate. Check payloads from peers you do not control.

## Presence

A presence subscription delivers joins and leaves, and also keeps the segment joined for messages:

```go
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
```

`PresenceList` reads one page of who is present, up to 100 per page:

```go
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
```

One query may be in flight per channel; another returns `ErrOperationInProgress`, and a query times out after `PresenceQueryTimeout` (10 s by default) without affecting the connection. Its reply is routed by the goroutine that runs listeners, so a query made inside a listener always times out: start it from a goroutine of your own.

## Errors

The SDK's own failures are `*celeris.Error` values, matched by code with `errors.Is`. Their messages name what failed and the rule it broke, never your input values, credentials or server bytes.

| Code                     | Meaning                                                                         |
| ------------------------ | ------------------------------------------------------------------------------- |
| `ErrConfiguration`       | Invalid options, identifiers or payloads, or credentials a provider returned    |
| `ErrTimeout`             | A connect attempt, presence query or context deadline ran out                   |
| `ErrCancelled`           | Cancelled by its context or by `Close`                                          |
| `ErrTransport`           | The provider failed, the handshake was refused, or the socket broke             |
| `ErrNotConnected`        | The operation needs a connection the channel does not have                      |
| `ErrBackpressure`        | 64 publishes are waiting, or sending is paused after a rate limit               |
| `ErrOperationInProgress` | A second `Connect`, or a second presence query, while the first runs            |
| `ErrDeliveryUnknown`     | A publish that may or may not have left the socket; never resent automatically  |
| `ErrProtocol`            | A server message could not be decoded; it is dropped and the connection stays up |

A refused handshake is `ErrTransport`: the SDK never reports an authentication failure. Errors the server sends arrive through `OnError` as `*celeris.ServerError`, with its `Type` (`PermissionDeniedError`, `RateLimitError`, `MessageSizeLimitError`, `ParserError`, `SendError` or `InternalError`), `SubType` (the command it answers), `Message` and `Resource`:

```go
channel.Events().OnError(func(err error) {
	if serverError, ok := errors.AsType[*celeris.ServerError](err); ok {
		fmt.Println("server:", serverError.Type, serverError.SubType, serverError.Resource)

		return
	}

	fmt.Println("sdk:", err)
})
```

`OnNotice` delivers the server's raw notices, such as greetings, as prose. Never branch on their text.

## Reconnection and recovery

A connection that drops is recovered automatically: up to 10 retries with full jitter up to 30 s, each with fresh credentials and a replay lookback covering the outage. The retry budget resets after a connection stays up for 60 s. A silent dead link is noticed after about 30 s through TCP keepalive. When recovery gives up, `OnError` reports why and the channel becomes `failed`.

```go
channel.Events().OnRecovery(func(event celeris.RecoveryEvent) {
	// Replay is a bounded window: gaps and duplicates are always possible.
	fmt.Println("recovered on retry", event.RetryIndex)
})
```

Recovery restores every subscription and reports **possible gaps and duplicates** every time. Replayed messages already seen are dropped within the 1024-id window; duplicates beyond it reach your listeners.

## Delivery semantics, honestly

- There is **no server receipt or ack** anywhere in the protocol. `Publish` returning means the local socket accepted the bytes.
- No offline queue, no durable history, no global ordering. Publishes still waiting when the connection drops fail with `ErrNotConnected`.
- A `RateLimitError` never names the command it dropped, so the client pauses and resends what it sent in the last two seconds: subscriptions as their current state, then up to 64 publishes, each at most once and with its original id so receivers drop a copy that already arrived. After eight limits in a row the limit is treated as a used-up quota: resending stops, and dropped subscriptions are retried on a slow probe, after a minute and doubling to an hour. Resends count toward usage.
- Subscriptions and publishes wait for room when the writer is full instead of failing. A subscription change goes out ahead of publishes, but never ahead of a publish to its own segment that was queued before it.

## Limits and defaults

| What             | Value                                                                                             |
| ---------------- | ------------------------------------------------------------------------------------------------- |
| Outbound command | 2 MiB encoded, refused before any write                                                           |
| Plan payload cap | 64 KiB free, 128 KiB standard, 512 KiB pro, 1024 KiB prime; enforced by the server                |
| Writer bounds    | 64 commands / 2 MiB not yet written; 64 queued publishes, plus resends                            |
| Rate-limit pause | 1 s plus full jitter growing with consecutive limits, at most 31 s                                |
| Resends          | last 2 s of commands, at most 64 publishes, each once                                             |
| Quota probe      | after 8 limits in a row: dropped subscriptions retried after 1 min, doubling to 1 h               |
| Connect deadline | `ConnectTimeout`, default 15 s, covering credentials and the handshake                            |
| Presence query   | one in flight per channel; `PresenceQueryTimeout`, default 10 s; a timeout never drops the connection |
| Reconnect        | 10 retries, full jitter up to 30 s, reset after 60 s connected                                    |
| Dead connection  | noticed after about 30 s of silence (TCP keepalive), then recovered                              |
| Dedup window     | 1024 message ids per channel                                                                      |
| Close            | at most about 5 s, even when the server does not answer                                           |

Received messages are never size-checked: they are already in memory when they arrive. A publish rejected for your plan's cap still counts toward your usage.

## Goroutines, contexts and cancellation

A `Client`, its channels and their segments are safe for concurrent use. Listeners run one at a time, in the order events happened, never under an SDK lock: a slow listener slows delivery rather than growing a queue, and a listener may call `Publish`, `Subscribe`, `Close` or `Connect`. Its own calls never wait for their events; those follow once it returns. The exception is `PresenceList`, as above.

`ctx` bounds only the call it is passed to; it never closes the channel. Cancelling `Connect` abandons the attempt and leaves the channel `failed`. Cancelling `Publish` takes back a publish the socket has not started writing (`ErrCancelled`, or `ErrTimeout` for a deadline); one the socket is already writing reports `ErrDeliveryUnknown`. Cancelling `PresenceList` frees the query slot.

## Local development and self-hosting

The production endpoint is built in. Set `BaseURL` only for a local stack or another deployment; `ws://` is accepted only for a loopback host, and only when you opt in:

```go
client, err := celeris.NewClient(celeris.ClientOptions{
	CredentialProvider: fetchCredentials,
	BaseURL:            os.Getenv("CELERIS_WS_URL"),
	// A local ws:// stack; production uses the built-in wss:// endpoint.
	AllowInsecureLoopback: true,
})
```

TLS is always verified, through the module's own HTTP transport: changes an application makes to `http.DefaultTransport` cannot weaken it.

## Versioning

Releases follow semantic versioning. Before v1.0.0, a minor release may change the API.

## More documentation

- [Package reference](https://pkg.go.dev/github.com/useceleris/sdk-go-client), with runnable examples
- [Go guide](https://useceleris.com/docs/sdks/go) and [Go client API reference](https://useceleris.com/docs/api-reference/go-client)
- [sdk-go-server](https://github.com/useceleris/sdk-go-server), for signing credentials on your server

## Development

`make check` runs formatting, `go vet`, golangci-lint, govulncheck, module tidiness, and the unit suites under the race detector, including the package checks: pinned exports, no signing facility reachable, nothing printed, and every snippet in this README compiled. `make live` runs the acceptance suites against a real Celeris stack; they read `CELERIS_WS_URL`, `CELERIS_CLIENT_ID` and `CELERIS_SIGNING_SECRET` from a gitignored `.env` or the environment. `make fuzz` fuzzes the decoder for a minute.

Read [CONVENTIONS.md](CONVENTIONS.md) before contributing, and [SECURITY.md](SECURITY.md) before reporting a vulnerability.

## License

[Apache 2.0](LICENSE).
