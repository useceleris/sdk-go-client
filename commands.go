package celeris

import (
	"strings"
	"unicode/utf8"
)

// The commands a client may send. Any other name, including the server's
// internal NODE_* commands, is refused before encoding.
const (
	subscribeCommand           = "SUB"
	unsubscribeCommand         = "UNSUB"
	presenceSubscribeCommand   = "PRES_SUB"
	presenceUnsubscribeCommand = "PRES_UNSUB"
)

// identifierRule returns the rule an identifier that reaches the wire breaks,
// or "" when it is well formed: nonempty, free of CR and LF, and valid UTF-8,
// so the bytes sent are exactly the text the caller passed (D-003).
func identifierRule(identifier string) string {
	if identifier == "" {
		return "Must not be empty"
	}

	if strings.ContainsAny(identifier, "\r\n") || !utf8.ValidString(identifier) {
		return "Must not contain CR, LF or invalid UTF-8"
	}

	return ""
}

// channelReferenceRule returns the rule a channel reference breaks, or "".
func channelReferenceRule(reference string) string {
	if reference == "" {
		return "Must not be empty"
	}

	if len(reference) > maximumChannelReferenceLength {
		return "Must be at most 255 characters"
	}

	for index := range len(reference) {
		character := reference[index]
		letter := character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z'
		digit := character >= '0' && character <= '9'

		if !letter && !digit && character != '-' && character != '_' {
			return "Must contain only ASCII letters, digits, hyphens (-) or underscores (_)"
		}
	}

	return ""
}
