package celeris

import (
	"strconv"
	"strings"
)

// Vector revision 2: realtime@cfa901fa73b2bd26abcc46c2f5ce47879dd4dc75 plus the
// C14 error frame and presence request ids. Hand-authored from the server's
// Rust layouts and copied from the JavaScript reference's codec-vectors.ts; no
// SDK encoder creates expectations.

type encodingVector struct {
	name     string
	encode   func() ([]byte, error)
	expected []byte
}

func join(parts ...[]byte) []byte {
	var joined []byte

	for _, part := range parts {
		joined = append(joined, part...)
	}

	return joined
}

func encodingVectors() []encodingVector {
	vectors := []encodingVector{
		{
			name:     "publish null ID",
			encode:   func() ([]byte, error) { return encodePublish("default", "", []byte("hello")) },
			expected: []byte("@PUB\n$7\ndefault\n$-1\n$5\nhello\n"),
		},
		{
			name:     "publish binary and Unicode",
			encode:   func() ([]byte, error) { return encodePublish("c:é", "識", []byte{0, 255, 13, 10, 64}) },
			expected: join([]byte("@PUB\n$4\nc:é\n$3\n識\n$5\n"), []byte{0, 255, 13, 10, 64, 10}),
		},
		{
			name:     "publish empty",
			encode:   func() ([]byte, error) { return encodePublish("a", "", []byte{}) },
			expected: []byte("@PUB\n$1\na\n$-1\n$0\n\n"),
		},
		{
			name:     "subscribe",
			encode:   func() ([]byte, error) { return encodeSegmentCommand("SUB", "chat") },
			expected: []byte("@SUB\n$4\nchat\n"),
		},
		{
			name:     "unsubscribe",
			encode:   func() ([]byte, error) { return encodeSegmentCommand("UNSUB", "chat") },
			expected: []byte("@UNSUB\n$4\nchat\n"),
		},
		{
			name:     "presence subscribe",
			encode:   func() ([]byte, error) { return encodeSegmentCommand("PRES_SUB", "chat") },
			expected: []byte("@PRES_SUB\n$4\nchat\n"),
		},
		{
			name:     "presence unsubscribe",
			encode:   func() ([]byte, error) { return encodeSegmentCommand("PRES_UNSUB", "chat") },
			expected: []byte("@PRES_UNSUB\n$4\nchat\n"),
		},
		{
			name:     "presence first page",
			encode:   func() ([]byte, error) { return encodePresenceList("chat", 1, 1, "1") },
			expected: []byte("@PRES_LIST\n$4\nchat\n;1\n;1\n$1\n1\n"),
		},
		{
			name:     "presence last allowed page",
			encode:   func() ([]byte, error) { return encodePresenceList("chat", 2147483647, 100, "識-9") },
			expected: []byte("@PRES_LIST\n$4\nchat\n;2147483647\n;100\n$5\n識-9\n"),
		},
	}

	// Valid Unicode is preserved without normalization.
	for _, preserved := range []struct {
		identifier string
		byteLength int
	}{
		{"😀", 4},
		{"\U00010000", 4},
		{"\U0010FFFF", 4},
		{"�", 3},
		{"é", 3},
		{"é", 2},
		{"a\x00b", 3},
	} {
		identifier := preserved.identifier
		vectors = append(vectors, encodingVector{
			name:     "preserves Unicode identifier " + strconv.Quote(identifier),
			encode:   func() ([]byte, error) { return encodeSegmentCommand("SUB", identifier) },
			expected: []byte("@SUB\n$" + strconv.Itoa(preserved.byteLength) + "\n" + identifier + "\n"),
		})
	}

	return vectors
}

type decodingVector struct {
	name     string
	data     []byte
	expected serverMessage
}

func decodingVectors() []decodingVector {
	vectors := []decodingVector{
		{
			name: "peer null ID and zero timestamp",
			data: []byte("@MSG\n+user\n+chat\n$-1\n:0\n$0\n\n"),
			expected: deliveryMessage{
				tokenReference: "user",
				segmentID:      "chat",
				timestamp:      0,
				payload:        []byte{},
			},
		},
		{
			name: "peer Unicode and max timestamp",
			data: join([]byte("@MSG\n+用戶\n+c:é\n+識\n:9223372036854775807\n$5\n"), []byte{0, 255, 13, 10, 64, 10}),
			expected: deliveryMessage{
				tokenReference: "用戶",
				segmentID:      "c:é",
				messageID:      "識",
				timestamp:      9223372036854775807,
				payload:        []byte{0, 255, 13, 10, 64},
			},
		},
		{
			name:     "CRLF and min timestamp",
			data:     []byte("@SERVER_MSG\r\n:-9223372036854775808\r\n$2\r\nok\r\n"),
			expected: noticeMessage{timestamp: -9223372036854775808, payload: []byte("ok")},
		},
		{
			name:     "raw notice",
			data:     []byte("@SERVER_MSG\n:1\n$6\njoined\n"),
			expected: noticeMessage{timestamp: 1, payload: []byte("joined")},
		},
		{
			name: "simple payload and bulk identifiers",
			data: []byte("@MSG\n$1\nu\n$1\ns\n$1\nm\n:-1\n+data\n"),
			expected: deliveryMessage{
				tokenReference: "u",
				segmentID:      "s",
				messageID:      "m",
				timestamp:      -1,
				payload:        []byte("data"),
			},
		},
		{
			name: "preserve BOM in text",
			data: []byte("@MSG\n+\uFEFFu\n+s\n$-1\n:1\n$0\n\n"),
			expected: deliveryMessage{
				tokenReference: "\uFEFFu",
				segmentID:      "s",
				timestamp:      1,
				payload:        []byte{},
			},
		},
		{
			name:     "empty array",
			data:     []byte("*0\n"),
			expected: arrayMessage{messages: []serverMessage{}},
		},
		{
			name: "nested arrays",
			data: []byte("*2\n*0\n*1\n@SERVER_MSG\n:1\n$0\n\n"),
			expected: arrayMessage{messages: []serverMessage{
				arrayMessage{messages: []serverMessage{}},
				arrayMessage{messages: []serverMessage{noticeMessage{timestamp: 1, payload: []byte{}}}},
			}},
		},
		{
			name: "presence connections",
			data: []byte("@PRES_LIST_RESPONSE\n+chat\n$1\n7\n;1\n;25\n;1\n;1\n;1\n*1\n*3\n+user\n+connection\n:123\n"),
			expected: presenceListMessage{
				segmentID:   "chat",
				requestID:   "7",
				total:       1,
				perPage:     25,
				currentPage: 1,
				from:        1,
				to:          1,
				connections: []PresenceConnection{{TokenReference: "user", ConnectionID: "connection", Timestamp: 123}},
			},
		},
		{
			name: "presence empty",
			data: []byte("@PRES_LIST_RESPONSE\n+chat\n$1\n8\n;0\n;25\n;1\n;0\n;0\n*0\n"),
			expected: presenceListMessage{
				segmentID:   "chat",
				requestID:   "8",
				total:       0,
				perPage:     25,
				currentPage: 1,
				from:        0,
				to:          0,
				connections: []PresenceConnection{},
			},
		},
		{
			name: "presence notification join",
			data: []byte("@PRES_NOTIFY\n+chat\n+user\n+connection\n;1\n:123\n"),
			expected: presenceNotifyMessage{
				segmentID:      "chat",
				tokenReference: "user",
				connectionID:   "connection",
				joined:         true,
				timestamp:      123,
			},
		},
		{
			name: "presence notification leave",
			data: []byte("@PRES_NOTIFY\n+chat\n+user\n+connection\n;0\n:124\n"),
			expected: presenceNotifyMessage{
				segmentID:      "chat",
				tokenReference: "user",
				connectionID:   "connection",
				joined:         false,
				timestamp:      124,
			},
		},
		{
			// DECODE-01: a command this version does not know is skipped, not
			// rejected, so a newer server cannot break a deployed client.
			name:     "unknown command ignored",
			data:     []byte("@FUTURE_COMMAND\n+a\n:1\n"),
			expected: ignoredMessage{},
		},
		{
			name: "unknown command ignored in tail position",
			data: []byte("*2\n@SERVER_MSG\n:1\n$0\n\n@FUTURE_COMMAND\n+a\n"),
			expected: arrayMessage{messages: []serverMessage{
				noticeMessage{timestamp: 1, payload: []byte{}},
				ignoredMessage{},
			}},
		},
		{
			// NODE_* commands are internal between server nodes. The SDK
			// recognises none of them, so one that arrives is skipped like any
			// other unknown command.
			name:     "internal node command ignored",
			data:     []byte("@NODE_PUB\n+node-1\n$4\nbody\n"),
			expected: ignoredMessage{},
		},
		{
			name: "internal node command ignored in tail position",
			data: []byte("*2\n@SERVER_MSG\n:1\n$0\n\n@NODE_PUB\n+node-1\n"),
			expected: arrayMessage{messages: []serverMessage{
				noticeMessage{timestamp: 1, payload: []byte{}},
				ignoredMessage{},
			}},
		},
		{
			name:     "any NODE_ command ignored",
			data:     []byte("@NODE_FUTURE\n+a\n"),
			expected: ignoredMessage{},
		},
		{
			name: "presence past last page",
			data: []byte("@PRES_LIST_RESPONSE\n+chat\n$1\n9\n;1\n;25\n;2\n;26\n;1\n*0\n"),
			expected: presenceListMessage{
				segmentID:   "chat",
				requestID:   "9",
				total:       1,
				perPage:     25,
				currentPage: 2,
				from:        26,
				to:          1,
				connections: []PresenceConnection{},
			},
		},
		{
			name: "error without sub type or resource",
			data: []byte("-Err\n+RateLimitError\n$-1\n$4\nslow\n$-1\n"),
			expected: errorMessage{
				errorType: "RateLimitError",
				message:   []byte("slow"),
			},
		},
		{
			// The message is length-prefixed, so it may contain anything,
			// including text that looks like another error.
			name: "error message containing frame text",
			data: []byte("-Err\n+PermissionDeniedError\n+SUB\n$14\ntext\n-Err\nmore\n$4\nroom\n"),
			expected: errorMessage{
				errorType: "PermissionDeniedError",
				subType:   "SUB",
				message:   []byte("text\n-Err\nmore"),
				resource:  "room",
			},
		},
		{
			name: "presence query error carrying its request id",
			data: []byte("-Err\n+InternalError\n+PRES_LIST\n$27\nError getting presence data\n$1\n3\n"),
			expected: errorMessage{
				errorType: "InternalError",
				subType:   "PRES_LIST",
				message:   []byte("Error getting presence data"),
				resource:  "3",
			},
		},
		{
			name: "error resource of every shape",
			data: []byte("-Err\n+FutureError\n+PUB\n$1\nx\n*5\n+a\n:-5\n;7\n$-1\n*1\n$1\nb\n"),
			expected: errorMessage{
				errorType: "FutureError",
				subType:   "PUB",
				message:   []byte("x"),
				resource:  []any{"a", int64(-5), int32(7), nil, []any{"b"}},
			},
		},
		{
			// Errors are self-delimiting, so they may sit anywhere in a batch,
			// and two batched errors decode as two.
			name: "batched errors before other messages",
			data: []byte("*3\n-Err\n+PermissionDeniedError\n+PRES_SUB\n$2\nno\n$4\nroom\n-Err\n+PermissionDeniedError\n+PRES_LIST\n$2\nno\n$1\n4\n@SERVER_MSG\n:1\n$2\nok\n"),
			expected: arrayMessage{messages: []serverMessage{
				errorMessage{errorType: "PermissionDeniedError", subType: "PRES_SUB", message: []byte("no"), resource: "room"},
				errorMessage{errorType: "PermissionDeniedError", subType: "PRES_LIST", message: []byte("no"), resource: "4"},
				noticeMessage{timestamp: 1, payload: []byte("ok")},
			}},
		},
	}

	// Invalid text encodings remain valid opaque binary payloads.
	for _, invalid := range invalidUTF8Vectors {
		vectors = append(vectors, decodingVector{
			name:     "opaque non-UTF8 payload " + strconv.Quote(string(invalid)),
			data:     join([]byte("@SERVER_MSG\n:1\n$"+strconv.Itoa(len(invalid))+"\n"), invalid, []byte{'\n'}),
			expected: noticeMessage{timestamp: 1, payload: invalid},
		})
	}

	return vectors
}

var invalidUTF8Vectors = [][]byte{
	{0x80},
	{0xc0, 0xaf},
	{0xed, 0xa0, 0x80},
	{0xf0, 0x9f, 0x98},
	{0xf4, 0x90, 0x80, 0x80},
}

func malformedVectors() [][]byte {
	texts := []string{
		"",
		"*0\ntrailing",
		"*1\n",
		"*-1\n",
		"*4096\n",
		"*9223372036854775808\n",
		// An unknown command is skippable only when it runs to the end of the
		// transport message; anywhere else its boundary is unknowable
		// (DECODE-01).
		"*2\n@FUTURE_COMMAND\n+a\n@SERVER_MSG\n:1\n$0\n\n",
		"*2\n*1\n@FUTURE_COMMAND\n+a\n@SERVER_MSG\n:1\n$0\n\n",
		"*2\n@NODE_PUB\n+node-1\n@SERVER_MSG\n:1\n$0\n\n",
		"+hello\n",
		":1\n",
		"$-1\n",
		"@SERVER_MSG\n:+1\n$0\n\n",
		"@SERVER_MSG\n: 1\n$0\n\n",
		"@SERVER_MSG\n:1.0\n$0\n\n",
		"@SERVER_MSG\n:9223372036854775808\n$0\n\n",
		"@SERVER_MSG\n:-9223372036854775809\n$0\n\n",
		"@SERVER_MSG\n:1\n$-2\n",
		"@SERVER_MSG\n:1\n$-1\n",
		"@SERVER_MSG\n:1\n$9999999999999999999\n",
		"@SERVER_MSG\n:1\n$2\nx\n",
		"@SERVER_MSG\n:1\n$1\nx!",
		"@SERVER_MSG\n:1\n$0\n\r!",
		"@MSG\n+\n+s\n$-1\n:1\n$0\n\n",
		"@MSG\n+u\n+s\n+\n:1\n$0\n\n",
		"@MSG\n+u\rX\n+s\n$-1\n:1\n$0\n\n",
		"@MSG\n$3\nu\ns\n+s\n$-1\n:1\n$0\n\n",
		// A line-based layout without field markers.
		"-Err\nParserError\nmessage",
		"-Other\n+ParserError\n$-1\n$1\nm\n$-1\n",
		// Missing fields.
		"-Err\n+ParserError\n$-1\n$1\nm\n",
		"-Err\n+ParserError\n$-1\n",
		// The type must be a simple-string name.
		"-Err\n$11\nParserError\n$-1\n$1\nm\n$-1\n",
		"-Err\n+Bad\rName\n$-1\n$1\nm\n$-1\n",
		"-Err\n+Bad-Name\n$-1\n$1\nm\n$-1\n",
		"-Err\n+ParserErrorParserErrorParserErrorParserErrorParserErrorParserError\n$-1\n$1\nm\n$-1\n",
		// The sub type must be a name or null.
		"-Err\n+ParserError\n+PRES LIST\n$1\nm\n$-1\n",
		"-Err\n+ParserError\n$3\nSUB\n$1\nm\n$-1\n",
		"-Err\n+ParserError\n:1\n$1\nm\n$-1\n",
		// The message cannot be null.
		"-Err\n+ParserError\n$-1\n$-1\n$-1\n",
		// The resource must be a known fragment, within the depth limit.
		"-Err\n+ParserError\n$-1\n$1\nm\n@SERVER_MSG\n",
		"-Err\n+ParserError\n$-1\n$1\nm\n" + strings.Repeat("*1\n", 40) + "$-1\n",
		"@PRES_LIST_RESPONSE\n+s\n$1\n1\n;0\n;1\n;1\n;0\n;0\n*1\n*2\n+u\n+c\n",
		"@PRES_NOTIFY\n+s\n+u\n+c\n:1\n:123\n",
	}

	// Presence figures and the join/leave flag are Integer32: signed 32-bit,
	// decimal digits only, with the `;` marker.
	for _, total := range []string{
		";2147483648\n",
		";-2147483649\n",
		";000000000001\n",
		";+1\n",
		"; 1\n",
		";1.0\n",
		":1\n",
	} {
		texts = append(texts, "@PRES_LIST_RESPONSE\n+s\n$1\n1\n"+total+";1\n;1\n;0\n;0\n*0\n")
	}

	vectors := make([][]byte, 0, len(texts))

	for _, text := range texts {
		vectors = append(vectors, []byte(text))
	}

	vectors = append(vectors, join([]byte("@MSG\n+"), []byte{255}, []byte("\n+s\n$-1\n:1\n$0\n\n")))

	for _, invalid := range invalidUTF8Vectors {
		vectors = append(vectors, join([]byte("@MSG\n$"+strconv.Itoa(len(invalid))+"\n"), invalid, []byte("\n+s\n$-1\n:1\n$0\n\n")))
	}

	return vectors
}

// Ill-formed text must be refused rather than collapse onto the valid
// replacement character. Go strings are bytes, so where the reference tests
// unpaired UTF-16 surrogates these are their UTF-8 counterparts: encoded
// surrogates, plus the other ways a byte string fails to be UTF-8.
var invalidIdentifierVectors = []string{
	"\xed\xa0\x80",
	"\xed\xbf\xbf",
	"room-\xed\xa0\x80",
	"\xed\xb0\x80-room",
	"a\xed\xa0\x80b",
	"\xed\xb0\x80\xed\xa0\x80",
	"\xed\xa0\x80\xed\xa0\x80",
	"😀\xed\xbf\xbf",
	"\xff",
	"\xc0\xaf",
	"\xf0\x9f\x98",
	"\xf4\x90\x80\x80",
}
