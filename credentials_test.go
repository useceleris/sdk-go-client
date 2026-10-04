package celeris

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

func TestCredentialsNeverPrint(t *testing.T) {
	credentials := Credentials{Payload: "synthetic-payload", Signature: "synthetic-signature"}
	formats := []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d", "%T %v"}

	for _, format := range formats {
		for _, value := range []any{credentials, &credentials, []Credentials{credentials}} {
			if printed := fmt.Sprintf(format, value); strings.Contains(printed, "synthetic") {
				t.Fatalf("%s printed %q", format, printed)
			}
		}
	}

	if printed := credentials.String(); strings.Contains(printed, "synthetic") {
		t.Fatalf("String() returned %q", printed)
	}

	var logged bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logged, nil))
	logger.Info("attempt", "credentials", credentials)
	slog.New(slog.NewJSONHandler(&logged, nil)).Info("attempt", "credentials", credentials)

	if strings.Contains(logged.String(), "synthetic") {
		t.Fatalf("slog printed %q", logged.String())
	}
}

func TestCredentialsEncodeAsJSONForCredentialEndpoints(t *testing.T) {
	encoded, err := json.Marshal(Credentials{Payload: "p", Signature: "s"})

	if err != nil || string(encoded) != `{"payload":"p","signature":"s"}` {
		t.Fatalf("encoded %s and %v", encoded, err)
	}

	var decoded Credentials

	if err := json.Unmarshal(encoded, &decoded); err != nil || decoded.Payload != "p" || decoded.Signature != "s" {
		t.Fatalf("decoded %v", err)
	}
}

func TestCredentialValidationNamesFieldsWithoutValues(t *testing.T) {
	cases := []struct {
		credentials Credentials
		message     string
	}{
		{Credentials{}, "Invalid credentials. Payload: Must not be empty. Signature: Must not be empty."},
		{Credentials{Payload: "p"}, "Invalid credentials. Signature: Must not be empty."},
		{Credentials{Payload: "secret\xff", Signature: "s"}, "Invalid credentials. Payload: Must not contain invalid UTF-8."},
	}

	for _, test := range cases {
		assertConfigurationError(t, validateCredentials(test.credentials), test.message)
	}

	if err := validateCredentials(Credentials{Payload: "p", Signature: "s"}); err != nil {
		t.Fatalf("valid credentials refused: %v", err)
	}
}

func TestGeneratedMessageIDsAreRandomHex(t *testing.T) {
	seen := map[string]bool{}

	for range 100 {
		identifier := generateMessageID()

		if len(identifier) != 32 || strings.Trim(identifier, "0123456789abcdef") != "" || seen[identifier] {
			t.Fatalf("message id %q", identifier)
		}

		seen[identifier] = true
	}
}
