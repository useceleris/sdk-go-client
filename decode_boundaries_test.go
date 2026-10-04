package celeris

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestDecodingAcceptsMaximumLengthHeaders(t *testing.T) {
	for _, newline := range []string{"\n", "\r\n"} {
		notice, err := decodeServerMessage([]byte("@SERVER_MSG" + newline + ":00000000000000000001" + newline + "$0" + newline + newline))

		if err != nil || notice.(noticeMessage).timestamp != 1 {
			t.Fatalf("%q: got %#v and %v", newline, notice, err)
		}

		frame := "-Err" + newline + "+" + strings.Repeat("A", 64) + newline + "+" + strings.Repeat("B", 64) + newline + "$1" + newline + "m" + newline + "$-1" + newline
		decoded, err := decodeServerMessage([]byte(frame))

		if err != nil {
			t.Fatalf("%q: %v", newline, err)
		}

		if message := decoded.(errorMessage); message.errorType != strings.Repeat("A", 64) || message.subType != strings.Repeat("B", 64) {
			t.Fatalf("%q: decoded %#v", newline, message)
		}
	}
}

func TestDecodingReportsBoundedHeaderFailures(t *testing.T) {
	cases := []struct {
		header string
		reason string
	}{
		{strings.Repeat("0", 20), "Unterminated line."},
		{strings.Repeat("0", 21), "Unterminated line."},
		{strings.Repeat("0", 22), "Line exceeds its 20-byte limit."},
		{strings.Repeat("0", 21) + "\n", "Line exceeds its 20-byte limit."},
		{strings.Repeat("0", 21) + "\r\n", "Line exceeds its 20-byte limit."},
		// A long malformed header fails without searching the rest of the
		// message.
		{strings.Repeat("0", 65536) + "\n", "Line exceeds its 20-byte limit."},
	}

	for _, test := range cases {
		_, err := decodeServerMessage([]byte("@SERVER_MSG\n:" + test.header))
		assertProtocolError(t, err, test.reason, "Timestamp", 12)
	}
}

func TestDecodingCopiesPayloadsContainingMarkerBytes(t *testing.T) {
	payload := make([]byte, 65536)
	pattern := []byte("\n\r@$*:+-")

	for index := range payload {
		payload[index] = pattern[index%len(pattern)]
	}

	data := join([]byte("@SERVER_MSG\n:1\n$65536\n"), payload, []byte{'\n'})
	message, err := decodeServerMessage(data)

	if err != nil {
		t.Fatal(err)
	}

	for index := range data {
		data[index] = 0
	}

	if !bytes.Equal(message.(noticeMessage).payload, payload) {
		t.Fatal("payload changed with its input")
	}
}

func assertProtocolError(t *testing.T, err error, reason, field string, offset int) {
	t.Helper()

	sdkError, ok := errors.AsType[*Error](err)

	if !ok || sdkError.Code != ErrProtocol {
		t.Fatalf("got %v, want a protocol error", err)
	}

	if sdkError.Message != reason || sdkError.Field != field || sdkError.Offset != offset {
		t.Fatalf("got %q at %s byte %d, want %q at %s byte %d", sdkError.Message, sdkError.Field, sdkError.Offset, reason, field, offset)
	}
}
