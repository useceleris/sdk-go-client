package celeris

// serverMessage is one decoded server message. Empty strings stand for null
// identifiers: an identifier the server sends is never empty.
type serverMessage interface {
	isServerMessage()
}

type deliveryMessage struct {
	tokenReference string
	segmentID      string
	messageID      string // empty when the server sent null; such a delivery is dropped (REV-01)
	timestamp      int64
	payload        []byte
}

type noticeMessage struct {
	timestamp int64
	payload   []byte
}

type presenceListMessage struct {
	segmentID   string
	requestID   string
	total       int32
	perPage     int32
	currentPage int32
	from        int32
	to          int32
	connections []PresenceConnection
}

type presenceNotifyMessage struct {
	segmentID      string
	tokenReference string
	connectionID   string
	joined         bool
	timestamp      int64
}

type errorMessage struct {
	errorType string
	subType   string // empty when the server sent null
	message   []byte
	resource  any
}

type arrayMessage struct {
	messages []serverMessage
}

// ignoredMessage is a command this version does not know. It is skipped,
// never surfaced (DECODE-01).
type ignoredMessage struct{}

func (deliveryMessage) isServerMessage()       {}
func (noticeMessage) isServerMessage()         {}
func (presenceListMessage) isServerMessage()   {}
func (presenceNotifyMessage) isServerMessage() {}
func (errorMessage) isServerMessage()          {}
func (arrayMessage) isServerMessage()          {}
func (ignoredMessage) isServerMessage()        {}
