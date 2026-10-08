package celeris

import (
	"net"
	"net/url"
	"strings"
)

// validateBaseURL accepts wss://, or ws:// for a loopback host when the caller
// opted in (ENDPOINT-01, SEC-02).
func validateBaseURL(baseURL string, allowInsecureLoopback bool) (*url.URL, error) {
	parsed, err := url.Parse(baseURL)

	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, newError(ErrConfiguration, "Invalid connection URL. BaseURL is not an absolute URL.")
	}

	if parsed.User != nil {
		return nil, newError(ErrConfiguration, "Invalid connection URL. BaseURL must not contain a username or password.")
	}

	if strings.ContainsAny(baseURL, "?#") {
		return nil, newError(ErrConfiguration, "Invalid connection URL. BaseURL must not contain a query string or fragment.")
	}

	if parsed.Scheme != "wss" && (parsed.Scheme != "ws" || !allowInsecureLoopback || !isLoopback(parsed.Hostname())) {
		return nil, newError(ErrConfiguration, "Invalid connection URL. BaseURL must use wss://, or ws:// for a loopback host when AllowInsecureLoopback is true.")
	}

	return parsed, nil
} // end function validateBaseURL

// isLoopback accepts localhost, ::1 and dotted 127.x.x.x addresses, as the
// reference does; an IPv4-mapped IPv6 address is not loopback there either.
func isLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") || host == "::1" {
		return true
	}

	address := net.ParseIP(host)

	return address != nil && !strings.Contains(host, ":") && address.To4()[0] == 127
} // end function isLoopback

// credentialURL is where one attempt connects. The credentials travel in its
// query, so it must never reach an error, a log or a caller.
func credentialURL(baseURL *url.URL, channelReference string, credentials Credentials) string {
	connection := *baseURL
	connection.Path = strings.TrimRight(baseURL.Path, "/") + "/channel/" + channelReference

	if baseURL.RawPath != "" {
		connection.RawPath = strings.TrimRight(baseURL.RawPath, "/") + "/channel/" + channelReference
	}

	connection.RawQuery = url.Values{
		"payload":   {credentials.Payload},
		"signature": {credentials.Signature},
	}.Encode()

	return connection.String()
} // end function credentialURL
