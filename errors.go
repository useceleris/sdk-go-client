package celeris

import (
	"strconv"
	"strings"
)

// ErrorCode names the kind of failure the SDK itself detected. The codes are
// a closed set and are also the sentinels callers match with [errors.Is]:
//
//	if errors.Is(err, celeris.ErrBackpressure) { ... }
//
// There is no authentication code: a handshake the server refuses for its
// credentials cannot be told apart from a network failure (DEV-02), so it is
// reported as [ErrTransport].
type ErrorCode string

const (
	// ErrConfiguration reports invalid input: options, identifiers, payload
	// helpers, or credentials a provider returned.
	ErrConfiguration ErrorCode = "Configuration"

	// ErrTimeout reports a connection attempt or presence query that ran past
	// its deadline.
	ErrTimeout ErrorCode = "Timeout"

	// ErrCancelled reports an operation cancelled by its context or by
	// [Channel.Close].
	ErrCancelled ErrorCode = "Cancelled"

	// ErrTransport reports a failed connection: the credential provider
	// failed, the handshake was refused, or the socket broke.
	ErrTransport ErrorCode = "Transport"

	// ErrNotConnected reports an operation that needs a connection the
	// channel does not have.
	ErrNotConnected ErrorCode = "NotConnected"

	// ErrBackpressure reports that the outbound queue is full or sending is
	// paused after a rate limit. Retry once it drains.
	ErrBackpressure ErrorCode = "Backpressure"

	// ErrOperationInProgress reports a second connect or presence query while
	// the first is still running.
	ErrOperationInProgress ErrorCode = "OperationInProgress"

	// ErrDeliveryUnknown reports a publish that may or may not have left the
	// socket. It is never resent automatically.
	ErrDeliveryUnknown ErrorCode = "DeliveryUnknown"

	// ErrProtocol reports a server message that could not be decoded. The
	// message is dropped and the connection stays up (DECODE-01).
	ErrProtocol ErrorCode = "ProtocolError"
)

// Error returns the code itself, so a bare code can be returned and matched
// like any sentinel error.
func (code ErrorCode) Error() string {
	return string(code)
} // end method Error

// Error is a failure the SDK detected. Its message names what failed and the
// rule or limit it broke; it never carries input values, credentials, or bytes
// the server sent.
type Error struct {
	Code    ErrorCode
	Message string

	// Field and Offset locate a decoding failure: a field name this package
	// chose and a zero-based byte offset into the server message. They are set
	// only when Code is ErrProtocol.
	Field  string
	Offset int
} // end struct Error

func (sdkError *Error) Error() string {
	if sdkError.Code != ErrProtocol {
		return sdkError.Message
	}

	return sdkError.Message + " Field: " + sdkError.Field + ", byte offset " + strconv.Itoa(sdkError.Offset) + "."
} // end method Error

// Is reports whether target is this error's code, so errors.Is(err,
// celeris.ErrTimeout) matches every timeout.
func (sdkError *Error) Is(target error) bool {
	code, ok := target.(ErrorCode)

	return ok && code == sdkError.Code
} // end method Is

// ServerErrorType is the kind of an error frame the server sent. The set stays
// open: a type a newer server adds still reaches the caller unchanged (ERR-01).
type ServerErrorType string

// The error types the server sends, one per server error variant.
const (
	ParserError           ServerErrorType = "ParserError"
	SendError             ServerErrorType = "SendError"
	PermissionDeniedError ServerErrorType = "PermissionDeniedError"
	RateLimitError        ServerErrorType = "RateLimitError"
	MessageSizeLimitError ServerErrorType = "MessageSizeLimitError"
	InternalError         ServerErrorType = "InternalError"
)

// ServerError is an error frame from the server, every field exactly as sent
// (ERR-01). The server keeps the connection open after sending one.
//
// It has no [ErrorCode]: it is shaped differently from the SDK's own errors,
// so match it with [errors.AsType] and inspect Type.
type ServerError struct {
	Type ServerErrorType

	// SubType names the command the error answers, such as "SUB" or
	// "PRES_LIST", or is empty when the server named none.
	SubType string

	// Message is the server's own text, for people to read. Never parse it.
	Message string

	// Resource is whatever Type and SubType define it to carry, such as the
	// segment a denial refers to: nil, a string, an int32, an int64, or a
	// []any of these.
	Resource any
} // end struct ServerError

func (serverError *ServerError) Error() string {
	return string(serverError.Type) + ": " + serverError.Message
} // end method Error

func newError(code ErrorCode, message string) *Error {
	return &Error{Code: code, Message: message}
} // end function newError

func protocolError(message, field string, offset int) *Error {
	return &Error{Code: ErrProtocol, Message: message, Field: field, Offset: offset}
} // end function protocolError

// configurationError names every field that failed and the rule it broke,
// never the value: "Invalid client options. CredentialProvider: Required."
func configurationError(subject string, failures ...string) *Error {
	return newError(ErrConfiguration, "Invalid "+subject+". "+strings.Join(failures, " "))
} // end function configurationError

// failure describes one failed field; an empty path describes the value as a
// whole.
func failure(path, rule string) string {
	if path == "" {
		return rule + "."
	}

	return path + ": " + rule + "."
} // end function failure
