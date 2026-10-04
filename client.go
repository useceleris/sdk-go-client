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
	// together. Zero means 15 seconds.
	ConnectTimeout time.Duration

	// PresenceQueryTimeout bounds each presence query. Zero means 10 seconds.
	PresenceQueryTimeout time.Duration
}

// Client creates channels. It holds configuration only and does no network
// work. It is safe for concurrent use.
type Client struct {
	credentialProvider   CredentialProvider
	baseURL              *url.URL
	connectTimeout       time.Duration
	presenceQueryTimeout time.Duration

	dial   dialer
	random func() float64
}

// NewClient validates options and returns a client. It fails with
// [ErrConfiguration] for a missing provider, a negative timeout, or a base URL
// that is not wss://, or ws:// for a loopback host when allowed.
func NewClient(options ClientOptions) (*Client, error) {
	var failures []string

	if options.CredentialProvider == nil {
		failures = append(failures, failure("CredentialProvider", "Required"))
	}

	if options.ConnectTimeout < 0 {
		failures = append(failures, failure("ConnectTimeout", "Must not be negative"))
	}

	if options.PresenceQueryTimeout < 0 {
		failures = append(failures, failure("PresenceQueryTimeout", "Must not be negative"))
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
		credentialProvider:   options.CredentialProvider,
		baseURL:              parsed,
		connectTimeout:       options.ConnectTimeout,
		presenceQueryTimeout: options.PresenceQueryTimeout,
		dial:                 dialWebSocket(),
		random:               rand.Float64,
	}

	if client.connectTimeout == 0 {
		client.connectTimeout = defaultConnectTimeout
	}

	if client.presenceQueryTimeout == 0 {
		client.presenceQueryTimeout = defaultPresenceQueryTimeout
	}

	return client, nil
}

// Channel returns a handle for the channel with the given reference. It does
// no network work; each call returns a new channel with its own connection.
// It fails with [ErrConfiguration] unless the reference is 1 to 255 ASCII
// letters, digits, hyphens or underscores.
func (client *Client) Channel(reference string) (*Channel, error) {
	if rule := channelReferenceRule(reference); rule != "" {
		return nil, configurationError("channel reference", failure("", rule))
	}

	return newChannel(client, reference), nil
}
