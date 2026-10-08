package celeris

import (
	"crypto/rand"
	"encoding/hex"
)

// generateMessageID gives every publish an id, so a resent copy is
// recognisable and receivers drop it (RESEND-01).
func generateMessageID() string {
	random := make([]byte, messageIDRandomBytes)
	_, _ = rand.Read(random) // never fails: crypto/rand aborts the program instead

	return hex.EncodeToString(random)
} // end function generateMessageID
