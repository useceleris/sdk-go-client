package celeris

import (
	"math/rand/v2"
	"net/url"
	"time"
)

// ClientOptions configures a [Client]. Only CredentialProvider is required.
type ClientOptions struct {
	// CredentialProvider fetches fresh credentials for every connection
	// attempt.
	CredentialProvider CredentialProvider

	// BaseURL defaults to the production endpoint. Set it only for local, CI
	// or self-hosted targets.
	BaseURL string

	// AllowInsecureLoopback permits a ws:// BaseURL for a loopback host.
	AllowInsecureLoopback bool

	// ConnectTimeout covers credential acquisition and the handshake
	// together. Zero means 15 seconds; otherwise it is 1 ms to 15 minutes.
	ConnectTimeout time.Duration

	// ReconnectTimeout covers credential acquisition and the handshake
	// together for a reconnect attempt. Zero means ConnectTimeout; otherwise
	// it is 1 ms to 15 minutes.
	ReconnectTimeout time.Duration

	// PresenceQueryTimeout bounds each presence query. Zero means 10 seconds;
	// otherwise it is 1 ms to 15 minutes.
	PresenceQueryTimeout time.Duration

	// PublishQueueSize caps the publishes waiting for room in the writer.
	// Zero means 64.
	PublishQueueSize int

	// DeduplicationWindowSize is how many delivered message ids each channel
	// remembers, to drop replayed duplicates. Zero means 1024.
	DeduplicationWindowSize int

	// MaximumReconnectAttempts is how many failed reconnect attempts, in one
	// retry budget, end recovery in StateFailed. Zero means 10; otherwise it
	// is 1 to 100.
	MaximumReconnectAttempts int
} // end struct ClientOptions

// Client creates channels. It holds configuration only and does no network
// work. It is safe for concurrent use.
type Client struct {
	credentialProvider       CredentialProvider
	baseURL                  *url.URL
	connectTimeout           time.Duration
	reconnectTimeout         time.Duration
	presenceQueryTimeout     time.Duration
	publishQueueSize         int
	deduplicationWindowSize  int
	maximumReconnectAttempts int

	dial   dialer
	random func() float64
} // end struct Client

// NewClient validates options and returns a client. It fails with
// [ErrConfiguration] for a missing provider, a nonzero timeout outside 1 ms to
// 15 minutes, a negative size, a maximum reconnect attempts outside 0 to 100,
// or a base URL that is not wss://, or ws:// for
// a loopback host when allowed.
func NewClient(options ClientOptions) (*Client, error) {
	const timeoutRule = "Must be 0 for the default, or from 1 ms to 15 minutes"

	var failures []string

	if options.CredentialProvider == nil {
		failures = append(failures, failure("CredentialProvider", "Required"))
	}

	if !validTimeout(options.ConnectTimeout) {
		failures = append(failures, failure("ConnectTimeout", timeoutRule))
	}

	if !validTimeout(options.ReconnectTimeout) {
		failures = append(failures, failure("ReconnectTimeout", timeoutRule))
	}

	if !validTimeout(options.PresenceQueryTimeout) {
		failures = append(failures, failure("PresenceQueryTimeout", timeoutRule))
	}

	if options.PublishQueueSize < 0 {
		failures = append(failures, failure("PublishQueueSize", "Must not be negative"))
	}

	if options.DeduplicationWindowSize < 0 {
		failures = append(failures, failure("DeduplicationWindowSize", "Must not be negative"))
	}

	if options.MaximumReconnectAttempts < 0 || options.MaximumReconnectAttempts > maximumRetriesCeiling {
		failures = append(failures, failure("MaximumReconnectAttempts", "Must be 0 for the default, or from 1 to 100"))
	}

	if failures != nil {
		return nil, configurationError("client options", failures...)
	}

	baseURL := options.BaseURL

	if baseURL == "" {
		baseURL = defaultBaseURL
	}

	parsed, err := validateBaseURL(baseURL, options.AllowInsecureLoopback)

	if err != nil {
		return nil, err
	}

	client := &Client{
		credentialProvider:       options.CredentialProvider,
		baseURL:                  parsed,
		connectTimeout:           options.ConnectTimeout,
		reconnectTimeout:         options.ReconnectTimeout,
		presenceQueryTimeout:     options.PresenceQueryTimeout,
		publishQueueSize:         options.PublishQueueSize,
		deduplicationWindowSize:  options.DeduplicationWindowSize,
		maximumReconnectAttempts: options.MaximumReconnectAttempts,
		dial:                     dialWebSocket(),
		random:                   rand.Float64,
	}

	if client.connectTimeout == 0 {
		client.connectTimeout = defaultConnectTimeout
	}

	if client.reconnectTimeout == 0 {
		client.reconnectTimeout = client.connectTimeout
	}

	if client.presenceQueryTimeout == 0 {
		client.presenceQueryTimeout = defaultPresenceQueryTimeout
	}

	if client.publishQueueSize == 0 {
		client.publishQueueSize = maximumPendingCommands
	}

	if client.deduplicationWindowSize == 0 {
		client.deduplicationWindowSize = deduplicationWindowSize
	}

	if client.maximumReconnectAttempts == 0 {
		client.maximumReconnectAttempts = maximumRetries
	}

	return client, nil
} // end function NewClient

// validTimeout accepts zero, which means the default, or 1 ms to 15 minutes
// (CONFIG-01).
func validTimeout(timeout time.Duration) bool {
	return timeout == 0 || timeout >= time.Millisecond && timeout <= maximumTimeout
} // end function validTimeout

// Channel returns a handle for the channel with the given reference. It does
// no network work; each call returns a new channel with its own connection.
// It fails with [ErrConfiguration] unless the reference is 1 to 255 ASCII
// letters, digits, hyphens or underscores.
func (client *Client) Channel(reference string) (*Channel, error) {
	if rule := channelReferenceRule(reference); rule != "" {
		return nil, configurationError("channel reference", failure("", rule))
	}

	return newChannel(client, reference), nil
} // end method Channel
