package celeris

// MessageMetadata describes a delivery, beside its payload (MSG-01).
type MessageMetadata struct {
	TokenReference string
	SegmentID      string

	// MessageID is always present: the publisher's, or one the server
	// assigned (REV-01).
	MessageID string

	// Timestamp is Unix milliseconds. Convert it with [time.UnixMilli].
	Timestamp int64
} // end struct MessageMetadata

// ServerNotice is a raw notice from the server: greetings, subscription
// acknowledgements and refusals, as human-readable prose. It names no segment
// and no command, so nothing may gate on its content.
type ServerNotice struct {
	Timestamp int64
	Payload   []byte
} // end struct ServerNotice

// PresencePage is one page of a segment's presence, every figure exactly as
// the server sent it. Past the last page From exceeds To and Connections is
// empty; that is not an error.
type PresencePage struct {
	SegmentID   string
	Total       int32
	PerPage     int32
	CurrentPage int32
	From        int32
	To          int32
	Connections []PresenceConnection
} // end struct PresencePage

// PresenceConnection is one connection present in a segment. A token
// reference can hold several connections.
type PresenceConnection struct {
	TokenReference string
	ConnectionID   string
	Timestamp      int64
} // end struct PresenceConnection

// PresenceEvent is one connection joining or leaving one segment (PRES-01).
type PresenceEvent struct {
	SegmentID      string
	TokenReference string
	ConnectionID   string
	Joined         bool // false: the connection left
	Timestamp      int64
} // end struct PresenceEvent

// RecoveryEvent reports a reconnect. Replay is a bounded window, not a durable
// log, so gaps and duplicates are always possible after one.
type RecoveryEvent struct {
	// RetryIndex is the zero-based retry that succeeded.
	RetryIndex int

	// PossibleGaps and PossibleDuplicates are always true.
	PossibleGaps       bool
	PossibleDuplicates bool
} // end struct RecoveryEvent
