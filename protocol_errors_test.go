package celeris

import (
	"errors"
	"strings"
	"testing"
)

func TestDecodingLocatesEachFailure(t *testing.T) {
	cases := []struct {
		wire    string
		message string
		field   string
		offset  int
	}{
		{"", "Missing field marker.", "Message", 0},
		{"?", "Unexpected server message marker.", "Message", 0},
		{"*0\n!", "Trailing data after server message.", "Message", 3},
		{"*2\n@UNKNOWN\n@SERVER_MSG\n:1\n$0\n\n", "Unknown command inside array has ambiguous boundaries.", "Command", 3},
		{"@SERVER_MSG\n:abc\n", "Expected decimal digits.", "Timestamp", 12},
		{"@SERVER_MSG\n:+1\n", "Expected decimal digits.", "Timestamp", 12},
		{"@SERVER_MSG\n:9223372036854775808\n", "Integer is outside -9223372036854775808 to 9223372036854775807.", "Timestamp", 12},
		{"@SERVER_MSG\n:000000000000000000000\n", "Line exceeds its 20-byte limit.", "Timestamp", 12},
		{"@SERVER_MSG\n:1", "Unterminated line.", "Timestamp", 12},
		{"@SERVER_MSG\n+1\n", "Expected Integer64 marker.", "Timestamp", 12},
		{"@PRES_LIST_RESPONSE\n+s\n$1\n1\n;2147483648\n", "Integer is outside -2147483648 to 2147483647.", "Total", 28},
		{"@PRES_LIST_RESPONSE\n+s\n$1\n1\n;000000000001\n", "Line exceeds its 11-byte limit.", "Total", 28},
		{"@PRES_LIST_RESPONSE\n+s\n$1\n1\n:1\n", "Expected Integer32 marker.", "Total", 28},
		{"@SERVER_MSG\n:1\n$9\nx\n", "Bulk payload exceeds remaining message bytes.", "Payload", 15},
		{"@SERVER_MSG\n:1\n$1\nx!", "Missing bulk byte terminator.", "Payload", 15},
		{"@SERVER_MSG\n:1\n$-2\n", "Invalid bulk byte length.", "Payload", 15},
		{"@SERVER_MSG\n:1\n$-1\n", "Payload cannot be null.", "Payload", 15},
		{"@SERVER_MSG\n:1\n*0\n", "Expected simple or bulk byte marker.", "Payload", 15},
		{"@MSG\n+u\n+\n", "Identifier must be nonempty and CR/LF-free.", "SegmentID", 8},
		{"@MSG\n$-1\n", "Identifier cannot be null.", "TokenReference", 5},
		{"*-1\n", "Array length cannot be negative.", "Messages", 0},
		{"*4096\n", "Array length exceeds the 4096-fragment budget.", "Messages", 0},
		{"-Bad\n", "Invalid error header.", "Error", 0},
		{"-Err\n+Bad Name\n$-1\n$6\nsecret\n$-1\n", "Invalid error name.", "ErrorType", 5},
		{"-Err\nParserError\nsecret", "Expected simple string marker.", "ErrorType", 5},
		{"-Err\n+ParserError\n$3\nSUB\n$6\nsecret\n$-1\n", "Sub type must be a simple string or null.", "ErrorSubType", 18},
		{"-Err\n+ParserError\n$-1\n$-1\n$-1\n", "Payload cannot be null.", "ErrorMessage", 22},
		{"-Err\n+ParserError\n$-1\n$6\nsecret\n@X\n", "Unexpected resource marker.", "Resource", 32},
		{strings.Repeat("*1\n", 32) + "*0\n", "Arrays are nested deeper than 32 levels.", "Messages", 96},
		{"@PRES_NOTIFY\n+s\n+u\n+c\n;2\n:1\n", "Presence event must be 0 or 1.", "Event", 22},
	}

	for _, test := range cases {
		_, err := decodeServerMessage([]byte(test.wire))
		assertProtocolError(t, err, test.message, test.field, test.offset)

		if want := test.message + " Field: " + test.field + ", byte offset "; !strings.HasPrefix(err.Error(), want) {
			t.Fatalf("%q: error text %q", test.wire, err.Error())
		}
	}
} // end function TestDecodingLocatesEachFailure

func TestDecodingRejectsNonProtocolNumbers(t *testing.T) {
	for _, text := range []string{"", " ", "0x10", "0o10", "0b10", "+1", "1 ", "\t1", "1\r\r", "--1", "1.5", "1e2", "١"} {
		if _, err := decodeServerMessage([]byte("@SERVER_MSG\n:" + text + "\n$0\n\n")); !errors.Is(err, ErrProtocol) {
			t.Fatalf("%q: got %v, want a protocol error", text, err)
		}
	}
} // end function TestDecodingRejectsNonProtocolNumbers

func TestDecodingPreservesAcceptedDecimalSpellings(t *testing.T) {
	for text, want := range map[string]int64{"0001": 1, "-0": 0, "-0001": -1} {
		message, err := decodeServerMessage([]byte("@SERVER_MSG\n:" + text + "\n$0\n\n"))

		if err != nil || message.(noticeMessage).timestamp != want {
			t.Fatalf("%q: got %#v and %v", text, message, err)
		}
	}
} // end function TestDecodingPreservesAcceptedDecimalSpellings

func TestDecodingNeverRepeatsReceivedBytes(t *testing.T) {
	data := join([]byte("@MSG\n+"), []byte{255}, []byte("synthetic-secret\n"))
	_, err := decodeServerMessage(data)
	assertProtocolError(t, err, "Invalid UTF-8 text.", "TokenReference", 5)

	if strings.Contains(err.Error(), "synthetic-secret") || errors.Unwrap(err) != nil {
		t.Fatalf("error repeats received bytes or wraps a cause: %v", err)
	}

	_, err = decodeServerMessage([]byte("synthetic-secret"))
	assertProtocolError(t, err, "Unexpected server message marker.", "Message", 0)

	if strings.Contains(err.Error(), "synthetic-secret") {
		t.Fatalf("error repeats received bytes: %v", err)
	}
} // end function TestDecodingNeverRepeatsReceivedBytes
