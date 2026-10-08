package celeris

import (
	"bytes"
	"encoding/json"
	"strings"
	"unicode/utf8"
)

// Payloads are opaque bytes on the wire. These helpers cover the two encodings
// applications reach for first; every other format goes through
// [NewPayloadCodec], which keeps serializer libraries out of this package
// (HELP-01).

// TextPayload encodes text as UTF-8. Invalid UTF-8 in text is replaced with
// U+FFFD, so the payload is always valid text.
func TextPayload(text string) []byte {
	return []byte(strings.ToValidUTF8(text, "�"))
} // end function TextPayload

// ReadText decodes a UTF-8 payload. It fails with [ErrConfiguration] when the
// payload is not valid UTF-8.
func ReadText(payload []byte) (string, error) {
	if !utf8.Valid(payload) {
		return "", newError(ErrConfiguration, "Payload is not valid UTF-8, so it cannot be read as text.")
	}

	return string(payload), nil
} // end function ReadText

// JSONPayload encodes value as JSON with encoding/json, without escaping HTML
// characters. It fails with [ErrConfiguration] when value cannot be encoded,
// such as a channel, a function or a cyclic structure.
func JSONPayload(value any) ([]byte, error) {
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)

	if err := encoder.Encode(value); err != nil {
		// The encoder's own error can quote the input, so it is not passed on.
		return nil, newError(ErrConfiguration, "Value is not JSON-serializable: it contains a channel, function, complex number, cyclic structure or unsupported value.")
	}

	return bytes.TrimSuffix(encoded.Bytes(), []byte{'\n'}), nil
} // end function JSONPayload

// ReadJSON decodes a JSON payload into a T. The type is an assertion, not a
// validation: check payloads from peers you do not control against a schema.
func ReadJSON[T any](payload []byte) (T, error) {
	var value T

	if !utf8.Valid(payload) {
		return value, newError(ErrConfiguration, "Payload is not valid UTF-8, so it cannot be read as text.")
	}

	if !json.Valid(payload) {
		return value, newError(ErrConfiguration, "Payload is valid UTF-8 but not valid JSON.")
	}

	if err := json.Unmarshal(payload, &value); err != nil {
		// The decoder's own error can quote the payload, so it is not passed
		// on.
		var zero T

		return zero, newError(ErrConfiguration, "Payload is valid JSON but does not match the requested type.")
	}

	return value, nil
} // end function ReadJSON

// PayloadCodec gives a serializer of your choice, such as protobuf or
// MessagePack, the same shape as the built-in helpers. Create one with
// [NewPayloadCodec].
type PayloadCodec[T any] struct {
	encode func(T) ([]byte, error)
	decode func([]byte) (T, error)
} // end struct PayloadCodec

// NewPayloadCodec wraps an encoder and decoder pair. It fails with
// [ErrConfiguration] when either is nil.
func NewPayloadCodec[T any](encode func(T) ([]byte, error), decode func([]byte) (T, error)) (PayloadCodec[T], error) {
	if encode == nil || decode == nil {
		return PayloadCodec[T]{}, newError(ErrConfiguration, "Codec must provide encode and decode functions.")
	}

	return PayloadCodec[T]{encode: encode, decode: decode}, nil
} // end function NewPayloadCodec

// EncodePayload encodes value with the codec's encoder. The encoder's own
// errors are returned unchanged: they are the caller's, not this package's.
func (codec PayloadCodec[T]) EncodePayload(value T) ([]byte, error) {
	return codec.encode(value)
} // end method EncodePayload

// ReadPayload decodes payload with the codec's decoder. The decoder's own
// errors are returned unchanged.
func (codec PayloadCodec[T]) ReadPayload(payload []byte) (T, error) {
	return codec.decode(payload)
} // end method ReadPayload
