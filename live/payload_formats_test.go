package live

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	celeris "github.com/useceleris/sdk-go-client"
)

// Payloads are opaque bytes to the SDK and the server: whatever a caller
// encodes is what the peer decodes. These vectors are hand-encoded so the
// suite needs no serializer, and each is decoded on arrival so it proves
// itself rather than matching a copy of itself.

func mustHex(text string) []byte {
	decoded, err := hex.DecodeString(strings.ReplaceAll(text, " ", ""))

	if err != nil {
		panic(err)
	}

	return decoded
} // end function mustHex

// protobuf: field 1 varint 150, field 2 "안녕 celeris", field 3 { field 1 = 1 }.
var protobufVector = mustHex("08 96 01 12 0e ec 95 88 eb 85 95 20 63 65 6c 65 72 69 73 1a 02 08 01")

// MessagePack: fixmap(3) { "id": 7, "bin": bin8 <00 ff 10>, "txt": "héllo" }.
var messagePackVector = mustHex("83 a2 69 64 07 a3 62 69 6e c4 03 00 ff 10 a3 74 78 74 a6 68 c3 a9 6c 6c 6f")

func readVarint(data []byte, offset int) (int, int) {
	value, shift := 0, 0

	for {
		character := data[offset]
		offset++
		value |= int(character&0x7f) << shift

		if character&0x80 == 0 {
			return value, offset
		}

		shift += 7
	}
} // end function readVarint

func TestRoundTripsPayloadsByteIdentically(t *testing.T) {
	jsonVector, err := celeris.JSONPayload(map[string]any{"id": 7, "txt": "héllo 안녕", "nested": map[string]bool{"ok": true}})

	if err != nil {
		t.Fatal(err)
	}

	vectors := map[string][]byte{"json": jsonVector, "messagepack": messagePackVector, "protobuf": protobufVector, "binary": {0, 255, 13, 10, 0}}

	for label, vector := range vectors {
		t.Run(label, func(t *testing.T) {
			reference := uniqueChannelReference("fmt-" + label)
			publisher := connectedChannel(t, reference)
			receiver := connectedChannel(t, reference)
			subscribe(t, segment(t, receiver, "formats"))
			time.Sleep(1500 * time.Millisecond)

			if err := segment(t, publisher, "formats").Publish(t.Context(), vector); err != nil {
				t.Fatal(err)
			}

			message := nextMessage(t, segment(t, receiver, "formats"), func(delivery) bool { return true }, "the "+label+" delivery", 15*time.Second)

			if !bytes.Equal(message.payload, vector) {
				t.Fatalf("payload changed in transit: % x", message.payload)
			}

			switch label {
			case "json":
				decoded, err := celeris.ReadJSON[struct {
					ID   int    `json:"id"`
					Text string `json:"txt"`
				}](message.payload)

				if err != nil || decoded.ID != 7 || decoded.Text != "héllo 안녕" {
					t.Fatalf("decoded %+v, %v", decoded, err)
				}
			case "protobuf":
				if field, offset := readVarint(message.payload, 1); message.payload[0] != 0x08 || field != 150 || message.payload[offset] != 0x12 {
					t.Fatal("protobuf vector does not decode")
				}
			case "messagepack":
				if message.payload[0] != 0x83 || !bytes.Contains(message.payload, []byte("héllo")) {
					t.Fatal("MessagePack vector does not decode")
				}
			}
		})
	}
} // end function TestRoundTripsPayloadsByteIdentically
