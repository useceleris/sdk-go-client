package celeris

import "time"

// Every fixed value the package uses, in one place. None of these is part of
// the public surface.

const (
	// Outgoing size (LIMIT-01). Size is only ever checked on commands the
	// client sends. Received messages are already fully buffered when they
	// arrive, so checking them bounds no memory and only discards data.
	//
	// The command bound is the server's transport ceiling: above it no plan
	// can accept a command. Each plan's own, smaller payload cap is enforced
	// by the server and surfaces as a MessageSizeLimitError server error.
	maximumCommandBytes = 2 * 1024 * 1024

	// Equal to the command bound, so a maximum-size command always fits an
	// empty writer and anything behind it waits for room.
	maximumBufferedBytes = maximumCommandBytes

	// Bounds the writer and the publishes kept for a rate-limit resend, and
	// is the default bound on the publishes waiting for the writer
	// (PublishQueueSize).
	maximumPendingCommands = 64

	// Outbound recovery (RESEND-01). A rate limit is reported without saying
	// which frame it dropped, so whatever went out recently is resent.
	//
	// The server reports drops at most once a second, and a second more covers
	// the round trip, so a report concerns only commands sent within this
	// window.
	rateLimitSuspectWindow = 2 * time.Second

	// Resending waits at least this long, past the per-second window the
	// dropped frames were counted in.
	rateLimitCooldown = time.Second

	// Bounds the extra load, and the extra usage, a rate limit can cause.
	maximumPublishResends = 1

	// After this many rate limits in a row the limit is treated as a used-up
	// quota (per hour or per month): recent publishes are no longer resent,
	// and recent subscriptions wait for a quota probe.
	maximumConsecutiveRateLimits = 8

	// A used-up quota refuses every frame, so the subscriptions it dropped are
	// re-sent rarely rather than abandoned: first after a minute, then
	// doubling.
	quotaProbeFirstDelay = time.Minute

	quotaProbeMaximumDelay = time.Hour

	// Generated message ids share every receiver's deduplication window with
	// ids from other publishers, so they are random and long enough never to
	// collide.
	messageIDRandomBytes = 16

	// Decoder bounds. These limit parsing work and recursion, not message
	// size; no legitimate server message approaches them.
	maximumFragments = 4096

	maximumDepth = 32

	// "PRES_LIST_RESPONSE", the longest command the server sends.
	maximumCommandNameBytes = 18

	maximumErrorNameBytes = 64

	// Integer64 (`:`) carries timestamps; it is also the range of bulk and
	// array lengths. "-9223372036854775808" is its widest value.
	maximumInteger64LineBytes = 20

	// Integer32 (`;`) carries every other integer. "-2147483648" is its widest
	// value.
	maximumInteger32LineBytes = 11

	// The largest page a presence query may request.
	maximumPresencePerPage = 100

	// Connection defaults. Consumers do not configure where Celeris lives;
	// overriding the base URL is for local stacks and other deployments
	// (ENDPOINT-01).
	defaultBaseURL = "wss://realtime.useceleris.com"

	defaultConnectTimeout = 15 * time.Second

	defaultPresenceQueryTimeout = 10 * time.Second

	// The longest timeout an option accepts (CONFIG-01). Every SDK shares it,
	// and it stays below the largest delay a JavaScript timer can count.
	maximumTimeout = 15 * time.Minute

	defaultSegmentID = "default"

	// The longest channel reference the server accepts.
	maximumChannelReferenceLength = 255

	// The command a presence query error names as its sub type (QUERY-01).
	presenceListCommand = "PRES_LIST"

	// Recovery. The default MaximumReconnectAttempts.
	maximumRetries = 10

	// The largest MaximumReconnectAttempts an option accepts (CONFIG-01), so a
	// channel that cannot reconnect reaches failed in a known time.
	maximumRetriesCeiling = 100

	retryBudgetReset = time.Minute

	retryBaseDelay = 500 * time.Millisecond

	retryDelayCap = 30 * time.Second

	// A rate limit already in hand disproves recovery only if commands flowed
	// unrefused for longer than the longest pause plus the report window; any
	// sooner, it may be a late report of the frames that just went out.
	quotaReturnConfirmation = retryDelayCap + rateLimitSuspectWindow

	replayOverlap = 5 * time.Second

	// The server's largest replay lookback: an unsigned 32-bit millisecond
	// count.
	replayLookbackCap = 4_294_967_295 * time.Millisecond

	closeBudget = 5 * time.Second

	// TCP keepalive on every socket, a backstop to the heartbeat: probes start
	// after 15 seconds of silence and three unanswered probes, five seconds
	// apart, fail it.
	keepAliveIdle = 15 * time.Second

	keepAliveInterval = 5 * time.Second

	keepAliveCount = 3

	// The server pings every 30 seconds and closes a connection from which it
	// has received no ping or pong for 60 seconds; data frames do not count
	// (HEARTBEAT-01). The client answers its pings only while the channel
	// reads, so it pings on its own this often: every 60-second window holds
	// at least two pings, so one can fail to go out.
	heartbeatInterval = 20 * time.Second

	// A ping unanswered for this much reading time means the path is dead
	// (HEARTBEAT-01), so a dead path is noticed within about 35 seconds on any
	// system. Pongs arrive only while the channel reads, so time a listener
	// holds the receiver never counts.
	heartbeatTimeout = 15 * time.Second

	// The default deduplication window (DeduplicationWindowSize).
	deduplicationWindowSize = 1024
)
