package celeris

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestDecodingVectors(t *testing.T) {
	for _, vector := range decodingVectors() {
		t.Run(vector.name, func(t *testing.T) {
			message, err := decodeServerMessage(vector.data)

			if err != nil {
				t.Fatalf("decode failed: %v", err)
			}

			if !reflect.DeepEqual(message, vector.expected) {
				t.Fatalf("decoded %#v, want %#v", message, vector.expected)
			}
		})
	}
}

func TestDecodingRejectsMalformedMessages(t *testing.T) {
	for index, data := range malformedVectors() {
		t.Run(strconv.Itoa(index), func(t *testing.T) {
			if _, err := decodeServerMessage(data); !errors.Is(err, ErrProtocol) {
				t.Fatalf("%q: got %v, want a protocol error", data, err)
			}
		})
	}
}

func TestDecodingRejectsEveryTruncation(t *testing.T) {
	data := decodingVectors()[0].data

	for length := range len(data) {
		if _, err := decodeServerMessage(data[:length]); !errors.Is(err, ErrProtocol) {
			t.Fatalf("length %d: got %v, want a protocol error", length, err)
		}
	}
}

func TestDecodingBoundsNestingAndFragments(t *testing.T) {
	if _, err := decodeServerMessage([]byte(strings.Repeat("*1\n", 31) + "*0\n")); err != nil {
		t.Fatalf("31 levels: %v", err)
	}

	if _, err := decodeServerMessage([]byte(strings.Repeat("*1\n", 32) + "*0\n")); !errors.Is(err, ErrProtocol) {
		t.Fatalf("32 levels: got %v, want a protocol error", err)
	}

	if _, err := decodeServerMessage([]byte("*4095\n" + strings.Repeat("*0\n", 4095))); err != nil {
		t.Fatalf("4096 fragments: %v", err)
	}

	if _, err := decodeServerMessage([]byte("*1366\n" + strings.Repeat("@SERVER_MSG\n:1\n$0\n\n", 1366))); !errors.Is(err, ErrProtocol) {
		t.Fatalf("over 4096 fragments: got %v, want a protocol error", err)
	}
}

// LIMIT-01: received messages are never size-checked.
func TestDecodingAcceptsMessagesOverOneMebibyte(t *testing.T) {
	const payloadLength = 1024 * 1024
	data := join([]byte("@MSG\n+user\n+chat\n+msg_1\n:1\n$"+strconv.Itoa(payloadLength)+"\n"), make([]byte, payloadLength), []byte{'\n'})
	message, err := decodeServerMessage(data)

	if err != nil {
		t.Fatal(err)
	}

	if delivery, ok := message.(deliveryMessage); !ok || delivery.segmentID != "chat" || len(delivery.payload) != payloadLength {
		t.Fatalf("decoded %T, want the full delivery", message)
	}
}

func TestDecodingCopiesPayloads(t *testing.T) {
	data := []byte("*2\n@SERVER_MSG\n:1\n$1\nx\n@SERVER_MSG\n:1\n$1\nx\n")
	message, err := decodeServerMessage(data)

	if err != nil {
		t.Fatal(err)
	}

	for index := range data {
		data[index] = 0
	}

	messages := message.(arrayMessage).messages
	first := messages[0].(noticeMessage)
	second := messages[1].(noticeMessage)

	if string(first.payload) != "x" {
		t.Fatalf("payload shares the input: %q", first.payload)
	}

	first.payload[0] = 0

	if string(second.payload) != "x" {
		t.Fatalf("payloads share memory: %q", second.payload)
	}
}

func TestDecodingSurvivesMutatedInputs(t *testing.T) {
	vectors := decodingVectors()
	seed := uint32(0xce1e)

	for attempt := range 512 {
		data := append([]byte(nil), vectors[attempt%len(vectors)].data...)
		seed = seed*1664525 + 1013904223
		data[int(seed)%len(data)] = byte(seed)

		if _, err := decodeServerMessage(data); err != nil && !errors.Is(err, ErrProtocol) {
			t.Fatalf("got %v, want success or a protocol error", err)
		}
	}
}
