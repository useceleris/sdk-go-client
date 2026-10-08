package celeris

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
)

// generateMessageID gives every publish an id, so a resent copy is
// recognisable and receivers drop it (RESEND-01). A generated id names the SDK
// that made it, then holds 16 random bytes as hex:
// msg__go_v1.0.0__0123456789abcdef0123456789abcdef. The double underscores
// keep it apart from the server's own msg_{node}_ ids.
func generateMessageID() string {
	random := make([]byte, messageIDRandomBytes)
	_, _ = rand.Read(random) // never fails: crypto/rand aborts the program instead

	return messageIDPrefix(sdkLanguage, sdkVersion) + hex.EncodeToString(random)
} // end function generateMessageID

// messageIDPrefix is "msg__", the language, "_v", the version, "__". An id
// holds only letters, digits, dots and underscores, so any other character of
// the version, such as the hyphen of a pre-release, becomes an underscore.
func messageIDPrefix(language, version string) string {
	safeVersion := strings.Map(func(character rune) rune {
		letter := character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z'
		digit := character >= '0' && character <= '9'

		if letter || digit || character == '.' {
			return character
		}

		return '_'
	}, version)

	return "msg__" + language + "_v" + safeVersion + "__"
} // end function messageIDPrefix
