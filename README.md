# Celeris Go client

Realtime client for Celeris channels: connection lifecycle with automatic recovery, segment messaging, and presence.

**The model in three sentences.** A `Channel` is one WebSocket client: creating another `Channel`, even for the same reference, opens another socket. Every segment of that channel is multiplexed over that single connection, and connecting automatically makes you a member of the `"default"` segment. `Segment` handles are lightweight: create as many as you like, they share the socket and one interest count.

## Install

```sh
go get github.com/useceleris/sdk-go-client
```

Go 1.27 or newer. The package name is `celeris`.

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

`fetchCredentials` is a `celeris.CredentialProvider`: it calls your own authenticated endpoint, which signs credentials with [sdk-go-server](https://github.com/useceleris/sdk-go-server), and decodes its JSON response into `celeris.Credentials`. Never ship a signing secret in client code. The endpoint is built in; set `BaseURL` only for a local or self-hosted stack.

Payloads are opaque bytes: `TextPayload`/`JSONPayload` and `ReadText`/`ReadJSON` cover the common cases, and `NewPayloadCodec` wraps any other serializer (protobuf, MessagePack, CBOR) without the module depending on one. The package documentation carries runnable examples of each, and [examples/quickstart](examples/quickstart/main.go) runs against a real Celeris stack in the live suite.

## Delivery semantics, honestly

- `Publish` returns when the local socket accepted the bytes. There is **no server receipt or ack** anywhere in the protocol. Every publish carries a message id, yours (`PublishWithMessageID`) or a generated one.
- Lost connections retry automatically (10 attempts, full jitter, fresh credentials, replay lookback). Recovery restores your subscriptions and reports **possible gaps and duplicates**; a bounded 1024-id window deduplicates replayed messages, and duplicates beyond it remain possible.
- A `RateLimitError` never names the command it dropped, so the client pauses and resends what it sent in the last two seconds: subscriptions first, as their current state, then up to the last 64 publishes, each at most once and with its original id so receivers drop a copy that had already arrived. After eight limits in a row the client treats the limit as a used-up quota: it stops resending, and re-sends the subscriptions it dropped on a slow probe (after a minute, doubling to at most an hour) until commands go two seconds without a limit. Resends count toward usage.
- Subscriptions and publishes wait for room when the writer is full instead of failing. A subscription change goes out ahead of publishes, but never ahead of a publish to its own segment that was queued before it. `Publish` fails with `ErrBackpressure` only when 64 publishes are already waiting; a presence query does when the writer is full or sending is paused.
- No offline queue (publishes still waiting when the connection drops fail with `ErrNotConnected`), no durable history, no global ordering.
- Errors the server sends arrive through `Events().OnError` as a `*celeris.ServerError` carrying the server's `Type`, `SubType` (the command it answers), `Message` and `Resource` (what that command names, such as the segment). A denied or oversized publish still returns normally. A failed presence query is the exception: its error names the query, so `PresenceList` returns it at once.
- Publishing to a segment joins it server-side; subscribing to presence also joins it for messages.

## Limits and defaults

| What             | Value                                                                                 |
| ---------------- | ------------------------------------------------------------------------------------- |
| Outbound command | 2 MiB encoded, refused before any write                                               |
| Plan payload cap | enforced by the server per plan; see below                                            |
| Writer bounds    | 64 commands / 2 MiB not yet written; 64 queued publishes, plus resends                |
| Rate-limit pause | 1 s plus full jitter growing with consecutive limits, at most 31 s                    |
| Resends          | last 2 s of commands, at most 64 publishes, each once                                 |
| Quota probe      | after 8 limits in a row: dropped subscriptions retried after 1 min, doubling to 1 h   |
| Connect deadline | `ConnectTimeout`, default 15 s, covering credentials and the handshake                |
| Presence query   | one in flight per channel, `PresenceQueryTimeout` default 10 s; a timeout never drops the connection |
| Reconnect        | 10 retries, full jitter up to 30 s, reset after 60 s connected                        |
| Dedup window     | 1024 message ids per channel                                                          |
| Close            | at most about 5 s, even when the server does not answer                               |

Received messages are never size-checked: they are already in memory when they arrive. Each plan caps publish payloads: 64 KiB free, 128 KiB standard, 512 KiB pro, 1024 KiB prime. A publish over your plan's cap returns locally and is rejected afterwards with a `MessageSizeLimitError`, and it still counts toward your usage.

## Goroutines, contexts and cancellation

A `Channel` and its segments are safe for concurrent use. Listeners run one at a time, in the order events happened, never under an SDK lock: a slow listener slows delivery rather than growing a queue, and a listener may call `Publish`, `Subscribe`, `Close` or `Connect`. Its own calls never wait for their events; those follow once it returns. The one exception is `PresenceList`, whose reply cannot be routed until the listener returns: call it from a goroutine of your own.

`ctx` bounds the call it is passed to. Cancelling `Connect` abandons the attempt and leaves the channel `failed`. Cancelling `Publish` withdraws a publish the socket has not started writing (`ErrCancelled`, or `ErrTimeout` for a deadline); one the socket is already writing reports `ErrDeliveryUnknown`. Cancelling `PresenceList` frees the query slot. The `CredentialProvider` receives the attempt's context, which `Close` and the connect deadline both cancel.

Match failures with `errors.Is(err, celeris.ErrBackpressure)` and friends, and server errors with `errors.AsType[*celeris.ServerError](err)`. SDK errors carry no input values, credentials or server bytes.

## Development

`make check` runs the whole check: formatting, `go vet`, golangci-lint, govulncheck, module tidiness, and the unit suites under the race detector, including the package checks (pinned exports, no signing facility reachable, nothing printed). `make live` runs the acceptance suites against a real Celeris stack; they read `CELERIS_WS_URL`, `CELERIS_CLIENT_ID` and `CELERIS_SIGNING_SECRET` from a gitignored `.env` or the environment. `make fuzz` fuzzes the decoder for a minute.

Read [CONVENTIONS.md](CONVENTIONS.md) before contributing, and [SECURITY.md](SECURITY.md) before reporting a vulnerability.

## License

[Apache 2.0](LICENSE).
