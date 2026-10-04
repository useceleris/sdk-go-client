package celeris

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestTextPayloadsRoundTrip(t *testing.T) {
	for _, text := range []string{"", "hello", "héllo 안녕 🛰️"} {
		payload := TextPayload(text)

		if !bytes.Equal(payload, []byte(text)) {
			t.Fatalf("%q encoded as %q", text, payload)
		}

		if decoded, err := ReadText(payload); err != nil || decoded != text {
			t.Fatalf("%q decoded as %q and %v", text, decoded, err)
		}
	}
}

func TestTextPayloadReplacesInvalidUTF8(t *testing.T) {
	if payload := TextPayload("a\xffb"); string(payload) != "a�b" {
		t.Fatalf("encoded %q", payload)
	}
}

func TestReadTextRejectsInvalidUTF8(t *testing.T) {
	_, err := ReadText([]byte{0x80})
	assertConfigurationError(t, err, "Payload is not valid UTF-8, so it cannot be read as text.")
}

func TestJSONPayloadsRoundTrip(t *testing.T) {
	type value struct {
		ID     int            `json:"id"`
		Text   string         `json:"text"`
		Nested map[string]any `json:"nested"`
		List   []int          `json:"list"`
	}

	original := value{ID: 7, Text: "héllo <&>", Nested: map[string]any{"ok": true}, List: []int{1, 2}}
	payload, err := JSONPayload(original)

	if err != nil {
		t.Fatal(err)
	}

	if want := `{"id":7,"text":"héllo <&>","nested":{"ok":true},"list":[1,2]}`; string(payload) != want {
		t.Fatalf("encoded %s, want %s", payload, want)
	}

	decoded, err := ReadJSON[value](payload)

	if err != nil || !reflect.DeepEqual(decoded, original) {
		t.Fatalf("decoded %#v and %v", decoded, err)
	}
}

func TestJSONPayloadRejectsUnserializableValues(t *testing.T) {
	type cyclic struct {
		Secret string
		Self   *cyclic
	}

	loop := &cyclic{Secret: "synthetic-marker"}
	loop.Self = loop

	for name, value := range map[string]any{
		"a channel":         make(chan int),
		"a function":        func() {},
		"a complex number":  complex(1, 2),
		"a cyclic value":    loop,
		"an infinite float": float64(1) / zero(),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := JSONPayload(value)
			assertConfigurationError(t, err, "Value is not JSON-serializable: it contains a channel, function, complex number, cyclic structure or unsupported value.")

			if strings.Contains(err.Error(), "synthetic-marker") {
				t.Fatalf("error leaks its input: %v", err)
			}
		})
	}
}

func zero() float64 {
	return 0
}

func TestReadJSONRejectsInvalidPayloads(t *testing.T) {
	_, err := ReadJSON[any]([]byte("{ not json"))
	assertConfigurationError(t, err, "Payload is valid UTF-8 but not valid JSON.")

	_, err = ReadJSON[any]([]byte{0x80})
	assertConfigurationError(t, err, "Payload is not valid UTF-8, so it cannot be read as text.")

	_, err = ReadJSON[struct{ Count int }]([]byte(`{"Count":"synthetic-marker"}`))
	assertConfigurationError(t, err, "Payload is valid JSON but does not match the requested type.")

	if strings.Contains(err.Error(), "synthetic-marker") {
		t.Fatalf("error leaks the payload: %v", err)
	}

	_, err = ReadJSON[any]([]byte(strings.Repeat("[", 100_000) + strings.Repeat("]", 100_000)))
	assertConfigurationError(t, err, "Payload is valid UTF-8 but not valid JSON.")
}

func TestPayloadCodecsRoundTrip(t *testing.T) {
	type body struct{ Text string }

	codec, err := NewPayloadCodec(
		func(value body) ([]byte, error) { return []byte(value.Text), nil },
		func(payload []byte) (body, error) { return body{Text: string(payload)}, nil },
	)

	if err != nil {
		t.Fatal(err)
	}

	payload, err := codec.EncodePayload(body{Text: "hello"})

	if err != nil || string(payload) != "hello" {
		t.Fatalf("encoded %q and %v", payload, err)
	}

	if decoded, err := codec.ReadPayload(payload); err != nil || decoded.Text != "hello" {
		t.Fatalf("decoded %#v and %v", decoded, err)
	}
}

func TestPayloadCodecsPassTheCallersErrorsThrough(t *testing.T) {
	failure := errors.New("synthetic-decoder-failure")
	codec, err := NewPayloadCodec(
		func(string) ([]byte, error) { return nil, failure },
		func([]byte) (string, error) { return "", failure },
	)

	if err != nil {
		t.Fatal(err)
	}

	if _, err := codec.EncodePayload("x"); !errors.Is(err, failure) {
		t.Fatalf("got %v, want the encoder's own error", err)
	}

	if _, err := codec.ReadPayload(nil); !errors.Is(err, failure) {
		t.Fatalf("got %v, want the decoder's own error", err)
	}
}

func TestPayloadCodecsRequireBothFunctions(t *testing.T) {
	_, err := NewPayloadCodec[string](nil, func([]byte) (string, error) { return "", nil })
	assertConfigurationError(t, err, "Codec must provide encode and decode functions.")

	_, err = NewPayloadCodec[string](func(string) ([]byte, error) { return nil, nil }, nil)
	assertConfigurationError(t, err, "Codec must provide encode and decode functions.")
}

func assertConfigurationError(t *testing.T, err error, message string) {
	t.Helper()

	sdkError, ok := errors.AsType[*Error](err)

	if !ok || sdkError.Code != ErrConfiguration {
		t.Fatalf("got %v, want a configuration error", err)
	}

	if sdkError.Message != message {
		t.Fatalf("message %q, want %q", sdkError.Message, message)
	}

	if errors.Unwrap(err) != nil {
		t.Fatalf("error wraps a cause: %v", err)
	}
}
