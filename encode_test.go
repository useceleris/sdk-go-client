package celeris

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestEncodingVectors(t *testing.T) {
	for _, vector := range encodingVectors() {
		t.Run(vector.name, func(t *testing.T) {
			encoded, err := vector.encode()

			if err != nil {
				t.Fatalf("encode failed: %v", err)
			}

			if !bytes.Equal(encoded, vector.expected) {
				t.Fatalf("encoded %q, want %q", encoded, vector.expected)
			}
		})
	}
}

func TestEncodingRejectsInvalidCommands(t *testing.T) {
	cases := map[string]func() ([]byte, error){
		"internal node command": func() ([]byte, error) { return encodeSegmentCommand("NODE_PUB", "s") },
		"future node command":   func() ([]byte, error) { return encodeSegmentCommand("NODE_FUTURE", "s") },
		"publish as segment":    func() ([]byte, error) { return encodeSegmentCommand("PUB", "s") },
		"empty segment":         func() ([]byte, error) { return encodeSegmentCommand("SUB", "") },
		"segment with LF":       func() ([]byte, error) { return encodeSegmentCommand("SUB", "a\n") },
		"segment with CR":       func() ([]byte, error) { return encodeSegmentCommand("SUB", "a\r") },
		"publish empty segment": func() ([]byte, error) { return encodePublish("", "", []byte("x")) },
		"page zero":             func() ([]byte, error) { return encodePresenceList("s", 0, 1, "1") },
		"page negative":         func() ([]byte, error) { return encodePresenceList("s", -1, 1, "1") },
		"per page zero":         func() ([]byte, error) { return encodePresenceList("s", 1, 0, "1") },
		"per page negative":     func() ([]byte, error) { return encodePresenceList("s", 1, -1, "1") },
		"per page above 100":    func() ([]byte, error) { return encodePresenceList("s", 1, 101, "1") },
		"empty request id":      func() ([]byte, error) { return encodePresenceList("s", 1, 1, "") },
	}

	for name, encode := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := encode(); !errors.Is(err, ErrConfiguration) {
				t.Fatalf("got %v, want a configuration error", err)
			}
		})
	}
}

func TestEncodingRejectsIllFormedIdentifiers(t *testing.T) {
	for _, identifier := range invalidIdentifierVectors {
		encoders := []func() ([]byte, error){
			func() ([]byte, error) { return encodeSegmentCommand("SUB", identifier) },
			func() ([]byte, error) { return encodeSegmentCommand("UNSUB", identifier) },
			func() ([]byte, error) { return encodeSegmentCommand("PRES_SUB", identifier) },
			func() ([]byte, error) { return encodeSegmentCommand("PRES_UNSUB", identifier) },
			func() ([]byte, error) { return encodePresenceList(identifier, 1, 1, "1") },
			func() ([]byte, error) { return encodePresenceList("s", 1, 1, identifier) },
			func() ([]byte, error) { return encodePublish(identifier, "", []byte{}) },
			func() ([]byte, error) { return encodePublish("s", identifier, []byte{}) },
		}

		for index, encode := range encoders {
			if _, err := encode(); !errors.Is(err, ErrConfiguration) {
				t.Errorf("identifier %q, encoder %d: got %v, want a configuration error", identifier, index, err)
			}
		}
	}
}

func TestEncodingCountsTheWholeCommandAgainstTheLimit(t *testing.T) {
	// 2 MiB is the whole encoded command, not the payload: "@PUB\n",
	// "$1\ns\n", "$-1\n", "$2097128\n" and the closing LF are 24 bytes.
	encoded, err := encodePublish("s", "", make([]byte, maximumCommandBytes-24))

	if err != nil || len(encoded) != maximumCommandBytes {
		t.Fatalf("got %d bytes and %v, want exactly 2 MiB", len(encoded), err)
	}

	_, err = encodePublish("s", "", make([]byte, maximumCommandBytes-23))

	if !errors.Is(err, ErrConfiguration) || !strings.HasPrefix(err.Error(), "Encoded command exceeds 2 MiB.") {
		t.Fatalf("got %v, want the 2 MiB refusal", err)
	}

	// Characters at the limit, but each one is two UTF-8 bytes.
	if _, err := encodeSegmentCommand("SUB", strings.Repeat("é", maximumCommandBytes/2)); !errors.Is(err, ErrConfiguration) {
		t.Fatalf("got %v, want the 2 MiB refusal", err)
	}

	if encoded, _ := encodeSegmentCommand("SUB", "chat"); string(encoded) != "@SUB\n$4\nchat\n" {
		t.Fatalf("a refused command affected the next: %q", encoded)
	}
}

func TestEncodingCopiesThePayload(t *testing.T) {
	storage := []byte{99, 0, 255, 99}
	encoded, err := encodePublish("s", "", storage[1:3])

	if err != nil {
		t.Fatal(err)
	}

	for index := range storage {
		storage[index] = 42
	}

	if want := join([]byte("@PUB\n$1\ns\n$-1\n$2\n"), []byte{0, 255, 10}); !bytes.Equal(encoded, want) {
		t.Fatalf("encoded %q, want %q", encoded, want)
	}
}

func TestEncodingNamesTheFieldWithoutTheInput(t *testing.T) {
	_, err := encodePublish("synthetic-secret\n", "", nil)

	sdkError, ok := errors.AsType[*Error](err)

	if !ok || sdkError.Code != ErrConfiguration {
		t.Fatalf("got %v, want a configuration error", err)
	}

	if want := "Invalid command. SegmentID: Must not contain CR, LF or invalid UTF-8."; sdkError.Message != want {
		t.Fatalf("message %q, want %q", sdkError.Message, want)
	}

	if strings.Contains(err.Error(), "synthetic-secret") || errors.Unwrap(err) != nil {
		t.Fatalf("error repeats its input or wraps a cause: %v", err)
	}
}
