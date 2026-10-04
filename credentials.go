package celeris

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"
	"unicode/utf8"
)

// Credentials authorize one connection attempt. Both values are opaque: the
// SDK never parses, re-serializes or generates them. A trusted server signs
// them; a client only passes them on.
//
// Credentials never print: fmt verbs and [slog] show them as redacted. The
// JSON tags let a credential endpoint encode them directly and a client decode
// its response.
type Credentials struct {
	Payload   string `json:"payload"`
	Signature string `json:"signature"`
}

const redactedCredentials = "celeris.Credentials{redacted}"

func (Credentials) String() string {
	return redactedCredentials
}

// Format redacts the credentials for every fmt verb, %#v and %d included.
func (Credentials) Format(state fmt.State, _ rune) {
	_, _ = io.WriteString(state, redactedCredentials)
}

// LogValue redacts the credentials in [slog] output.
func (Credentials) LogValue() slog.Value {
	return slog.StringValue("redacted")
}

// CredentialRequest describes the attempt credentials are wanted for. They
// are requested fresh for every attempt, reconnects included (D-001).
type CredentialRequest struct {
	ChannelReference string

	// Reconnect is false for the attempt Connect starts and true for every
	// automatic reconnect.
	Reconnect bool

	// DisconnectedAt is when the connection dropped. It is zero unless
	// Reconnect is true.
	DisconnectedAt time.Time

	// ReplayLookback suggests how much history to replay: the outage so far
	// plus five seconds of overlap. It is zero unless Reconnect is true. The
	// signing server decides whether to sign any replay into the credentials.
	ReplayLookback time.Duration
}

// CredentialProvider fetches credentials for one attempt, typically from your
// own credential endpoint. It must honour ctx: the attempt's deadline and
// [Channel.Close] both cancel it, and a result it returns afterwards is
// discarded.
//
// Any error it returns is reported as [ErrTransport], [ErrTimeout] or
// [ErrCancelled]; its text is never inspected or passed on.
type CredentialProvider func(ctx context.Context, request CredentialRequest) (Credentials, error)

// validateCredentials checks what a provider returned, naming the field that
// failed but never its value.
func validateCredentials(credentials Credentials) error {
	var failures []string

	for _, field := range []struct {
		name  string
		value string
	}{
		{"Payload", credentials.Payload},
		{"Signature", credentials.Signature},
	} {
		if field.value == "" {
			failures = append(failures, failure(field.name, "Must not be empty"))
		} else if !utf8.ValidString(field.value) {
			failures = append(failures, failure(field.name, "Must not contain invalid UTF-8"))
		}
	}

	if failures != nil {
		return configurationError("credentials", failures...)
	}

	return nil
}
