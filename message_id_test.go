package celeris

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
)

func TestGeneratedIDsNameTheSDKAndItsVersionThen32HexDigits(t *testing.T) {
	identifier := generateMessageID()
	random, found := strings.CutPrefix(identifier, messageIDPrefix(sdkVersion))

	if !found || !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(random) {
		t.Fatalf("identifier %q", identifier)
	}
} // end function TestGeneratedIDsNameTheSDKAndItsVersionThen32HexDigits

func TestGeneratedIDsHoldOnlyLettersDigitsDotsAndUnderscores(t *testing.T) {
	allowed := regexp.MustCompile(`^[A-Za-z0-9._]+$`)

	for range 100 {
		if identifier := generateMessageID(); !allowed.MatchString(identifier) {
			t.Fatalf("identifier %q", identifier)
		}
	}
} // end function TestGeneratedIDsHoldOnlyLettersDigitsDotsAndUnderscores

func TestGeneratedIDsAreDifferentOnEveryCall(t *testing.T) {
	seen := map[string]bool{}

	for range 1000 {
		seen[generateMessageID()] = true
	}

	if len(seen) != 1000 {
		t.Fatalf("%d distinct ids of 1000", len(seen))
	}
} // end function TestGeneratedIDsAreDifferentOnEveryCall

func TestEveryOtherVersionCharacterBecomesAnUnderscore(t *testing.T) {
	cases := map[string]string{
		"1.2.0-beta.1":  "msg__go_v1.2.0_beta.1__",
		"1.2.0+build/7": "msg__go_v1.2.0_build_7__",
		"1.0.0":         "msg__go_v1.0.0__",
	}

	for version, expected := range cases {
		if prefix := messageIDPrefix(version); prefix != expected {
			t.Fatalf("version %q: got %q, want %q", version, prefix, expected)
		}
	}
} // end function TestEveryOtherVersionCharacterBecomesAnUnderscore

// Client commands write identifiers as bulk strings: "$", the length, LF, the
// bytes, LF. The server reads them without a scan for the end.
func TestAGeneratedIDIsSentAsABulkString(t *testing.T) {
	identifier := generateMessageID()
	frame, err := encodePublish("chat", identifier, []byte("hi"))

	if err != nil {
		t.Fatal(err)
	}

	if expected := "@PUB\n$4\nchat\n$" + strconv.Itoa(len(identifier)) + "\n" + identifier + "\n$2\nhi\n"; string(frame) != expected {
		t.Fatalf("frame %q", frame)
	}
} // end function TestAGeneratedIDIsSentAsABulkString

// Your own id needs at least one character, with no CR or LF. It is sent
// exactly as given, as a bulk string.
func TestYourOwnIDIsSentUnchangedAsABulkString(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		lobby := channel.DefaultSegment()

		publish(t, lobby, "a", "x")
		publish(t, lobby, "order-1042:shipped é", "y")
		assertCommands(t, socket, "@PUB\n$7\ndefault\n$1\na\n$1\nx\n", "@PUB\n$7\ndefault\n$21\norder-1042:shipped é\n$1\ny\n")
		channel.Close()
	})
} // end function TestYourOwnIDIsSentUnchangedAsABulkString

func TestYourOwnIDNeedsAtLeastOneCharacterAndNoLineBreak(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		lobby := channel.DefaultSegment()

		cases := map[string]string{
			"":     "Invalid command. MessageID: Must not be empty.",
			"a\nb": "Invalid command. MessageID: Must not contain CR, LF or invalid UTF-8.",
			"a\rb": "Invalid command. MessageID: Must not contain CR, LF or invalid UTF-8.",
		}

		for messageID, expected := range cases {
			err := lobby.PublishWithMessageID(t.Context(), []byte("x"), messageID)

			if !errors.Is(err, ErrConfiguration) || err.Error() != expected {
				t.Fatalf("id %q: got %v", messageID, err)
			}
		}

		// Nothing was written; the control publish is the first frame.
		publish(t, lobby, "control", "x")
		assertCommands(t, socket, "@PUB\n$7\ndefault\n$7\ncontrol\n$1\nx\n")
		channel.Close()
	})
} // end function TestYourOwnIDNeedsAtLeastOneCharacterAndNoLineBreak
