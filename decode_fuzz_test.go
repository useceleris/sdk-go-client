package celeris

import (
	"errors"
	"regexp"
	"testing"
)

// The reasons the decoder gives. Each is fixed text apart from its figures, so
// a failure can never repeat what the server sent.
var decoderReasons = []string{
	"Missing field marker.",
	"Unexpected server message marker.",
	"Trailing data after server message.",
	"Server message has more than N fragments.",
	"Line exceeds its N-byte limit.",
	"Unterminated line.",
	"Invalid UTF-8 text.",
	"Expected decimal digits.",
	"Integer is outside -N to N.",
	"Expected IntegerN marker.",
	"Expected simple or bulk byte marker.",
	"Invalid bulk byte length.",
	"Bulk payload exceeds remaining message bytes.",
	"Missing bulk byte terminator.",
	"Identifier cannot be null.",
	"Identifier must be nonempty and CR/LF-free.",
	"Payload cannot be null.",
	"Arrays are nested deeper than N levels.",
	"Expected array marker.",
	"Array length cannot be negative.",
	"Array length exceeds the N-fragment budget.",
	"Presence connection must contain three fields.",
	"Invalid error header.",
	"Expected simple string marker.",
	"Sub type must be a simple string or null.",
	"Invalid error name.",
	"Unexpected resource marker.",
	"Unknown command inside array has ambiguous boundaries.",
	"Presence event must be 0 or 1.",
}

var figures = regexp.MustCompile(`[0-9]+`)

func isDecoderReason(message string) bool {
	for _, reason := range decoderReasons {
		if figures.ReplaceAllString(reason, "N") == figures.ReplaceAllString(message, "N") {
			return true
		}
	}

	return false
} // end function isDecoderReason

// FuzzDecode checks that no input panics the decoder, and that every failure
// is a located protocol error that repeats nothing it received (WIRE-03,
// WIRE-04, SEC-03).
func FuzzDecode(f *testing.F) {
	for _, vector := range decodingVectors() {
		f.Add(vector.data)
	}

	for _, data := range malformedVectors() {
		f.Add(data)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		_, err := decodeServerMessage(data)

		if err == nil {
			return
		}

		sdkError, ok := errors.AsType[*Error](err)

		if !ok || sdkError.Code != ErrProtocol {
			t.Fatalf("got %v, want a protocol error", err)
		}

		if sdkError.Offset < 0 || sdkError.Offset > len(data) {
			t.Fatalf("offset %d outside a %d-byte message", sdkError.Offset, len(data))
		}

		if !isDecoderReason(sdkError.Message) {
			t.Fatalf("unexpected reason %q", sdkError.Message)
		}
	})
} // end function FuzzDecode
