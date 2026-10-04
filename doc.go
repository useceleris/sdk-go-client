// Package celeris is the Celeris realtime client.
//
// A [Client] holds configuration and creates channels. A [Channel] is one
// WebSocket connection; every segment of the channel is multiplexed over it.
// A [Segment] handle subscribes, listens, publishes and queries presence.
//
// Credentials come from a [CredentialProvider], called afresh for every
// connection attempt, reconnects included. Fetch them from your own backend,
// which signs them with the server SDK; a client never signs.
//
// Publish returns once the socket accepted the bytes. The protocol has no
// acknowledgements, so that is not receipt or delivery. A dropped connection
// is recovered automatically with fresh credentials, and every recovery
// declares that gaps and duplicates are possible.
//
// Events are listener registrations that return a function removing the
// listener. Listeners run one at a time, in the order events happened, never
// under an SDK lock; a slow listener holds up the next event. A Channel and
// its segments are safe for concurrent use.
package celeris
