package celeris

import (
	"net/url"
	"strings"
	"testing"
)

func TestUnsafeBaseURLsAreRefused(t *testing.T) {
	for _, baseURL := range []string{
		"ws://example.test",
		"https://example.test",
		"wss://user:pass@example.test",
		"wss://example.test?",
		"wss://example.test#",
		"not a url",
		"wss:",
		"",
	} {
		if _, err := validateBaseURL(baseURL, true); err == nil || !strings.HasPrefix(err.Error(), "Invalid connection URL.") {
			t.Errorf("%q: got %v", baseURL, err)
		}
	}
}

func TestLoopbackNeedsTheExplicitOptIn(t *testing.T) {
	for _, host := range []string{"localhost", "LOCALHOST", "127.0.0.1", "127.1.2.3", "[::1]"} {
		if _, err := validateBaseURL("ws://"+host+":19002", false); err == nil {
			t.Errorf("%s accepted without the opt-in", host)
		}

		if parsed, err := validateBaseURL("ws://"+host+":19002", true); err != nil || parsed.Scheme != "ws" {
			t.Errorf("%s refused with the opt-in: %v", host, err)
		}
	}

	for _, host := range []string{"localhost.evil.test", "128.0.0.1", "0.0.0.0", "[::]", "[::ffff:127.0.0.1]"} {
		if _, err := validateBaseURL("ws://"+host, true); err == nil {
			t.Errorf("%s accepted as loopback", host)
		}
	}
}

func TestCredentialURLsKeepThePathAndEncodeValuesOnce(t *testing.T) {
	reference := strings.Repeat("a", 255)

	for _, base := range []string{"wss://example.test", "wss://example.test/", "wss://example.test/prefix///"} {
		original, err := validateBaseURL(base, false)

		if err != nil {
			t.Fatal(err)
		}

		result, err := url.Parse(credentialURL(original, reference, Credentials{Payload: "+/%=&識", Signature: "%2B"}))

		if err != nil {
			t.Fatal(err)
		}

		if want := strings.TrimRight(original.Path, "/") + "/channel/" + reference; result.Path != want {
			t.Errorf("%s: path %q, want %q", base, result.Path, want)
		}

		if query := result.Query(); query.Get("payload") != "+/%=&識" || query.Get("signature") != "%2B" {
			t.Errorf("%s: query %q", base, result.RawQuery)
		}

		if original.RawQuery != "" {
			t.Errorf("%s: base URL changed", base)
		}
	}
}

func TestCredentialURLsKeepAnEncodedBasePath(t *testing.T) {
	original, err := validateBaseURL("wss://example.test/a%2Fb/", false)

	if err != nil {
		t.Fatal(err)
	}

	if got := credentialURL(original, "room", Credentials{Payload: "p", Signature: "s"}); !strings.HasPrefix(got, "wss://example.test/a%2Fb/channel/room?") {
		t.Fatalf("url %q", got)
	}
}
