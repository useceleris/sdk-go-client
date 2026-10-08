package celeris

// serverMessage is one decoded server message. Empty strings stand for null
// identifiers: an identifier the server sends is never empty.
type serverMessage interface {
	isServerMessage()
} // end interface serverMessage

type deliveryMessage struct {
	tokenReference string
	segmentID      string
	messageID      string // empty when the server sent null; such a delivery is dropped (REV-01)
	timestamp      int64
	payload        []byte
} // end struct deliveryMessage

type noticeMessage struct {
	timestamp int64
	payload   []byte
} // end struct noticeMessage

type presenceListMessage struct {
	segmentID   string
	requestID   string
	total       int32
	perPage     int32
	currentPage int32
	from        int32
	to          int32
	connections []PresenceConnection
} // end struct presenceListMessage

type presenceNotifyMessage struct {
	segmentID      string
	tokenReference string
	connectionID   string
	joined         bool
	timestamp      int64
} // end struct presenceNotifyMessage

type errorMessage struct {
	errorType string
	subType   string // empty when the server sent null
	message   []byte
	resource  any
} // end struct errorMessage

type arrayMessage struct {
	messages []serverMessage
} // end struct arrayMessage

// ignoredMessage is a command this version does not know. It is skipped,
// never surfaced (DECODE-01).
type ignoredMessage struct{}

func (deliveryMessage) isServerMessage() {} // end method isServerMessage

func (noticeMessage) isServerMessage() {} // end method isServerMessage

func (presenceListMessage) isServerMessage() {} // end method isServerMessage

func (presenceNotifyMessage) isServerMessage() {} // end method isServerMessage

func (errorMessage) isServerMessage() {} // end method isServerMessage

func (arrayMessage) isServerMessage() {} // end method isServerMessage

func (ignoredMessage) isServerMessage() {} // end method isServerMessage
