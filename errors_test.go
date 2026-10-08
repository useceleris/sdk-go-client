package celeris

import (
	"errors"
	"fmt"
	"testing"
)

func TestErrorsMatchTheirCode(t *testing.T) {
	err := fmt.Errorf("context: %w", newError(ErrTimeout, "Connection attempt timed out after 15s."))

	if !errors.Is(err, ErrTimeout) || errors.Is(err, ErrTransport) {
		t.Fatalf("errors.Is mismatched %v", err)
	}

	sdkError, ok := errors.AsType[*Error](err)

	if !ok || sdkError.Code != ErrTimeout {
		t.Fatalf("errors.AsType mismatched %v", err)
	}

	if ErrTimeout.Error() != "Timeout" {
		t.Fatalf("code text %q", ErrTimeout.Error())
	}
} // end function TestErrorsMatchTheirCode

func TestProtocolErrorsLocateTheirField(t *testing.T) {
	err := protocolError("Invalid UTF-8 text.", "TokenReference", 5)

	if want := "Invalid UTF-8 text. Field: TokenReference, byte offset 5."; err.Error() != want {
		t.Fatalf("text %q, want %q", err.Error(), want)
	}
} // end function TestProtocolErrorsLocateTheirField

func TestServerErrorsCarryNoCode(t *testing.T) {
	var err error = &ServerError{Type: RateLimitError, Message: "Rate limit exceeded"}

	for _, code := range []ErrorCode{ErrConfiguration, ErrTimeout, ErrCancelled, ErrTransport, ErrNotConnected, ErrBackpressure, ErrOperationInProgress, ErrDeliveryUnknown, ErrProtocol} {
		if errors.Is(err, code) {
			t.Fatalf("server error matched %s", code)
		}
	}

	if err.Error() != "RateLimitError: Rate limit exceeded" {
		t.Fatalf("text %q", err.Error())
	}
} // end function TestServerErrorsCarryNoCode
