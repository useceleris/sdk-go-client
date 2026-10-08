package celeris

import (
	"bytes"
	"math"
	"strconv"
	"unicode/utf8"
)

// decodeServerMessage decodes one transport message. A decoder is built per
// message over that message's own bytes, so nothing spans messages and a
// malformed one cannot desynchronise the next (DECODE-01).
func decodeServerMessage(data []byte) (serverMessage, error) {
	decoder := messageDecoder{data: data}
	message, err := decoder.readMessage(0, true)

	if err != nil {
		return nil, err
	}

	if decoder.offset != len(data) {
		return nil, protocolError("Trailing data after server message.", "Message", decoder.offset)
	}

	return message, nil
} // end function decodeServerMessage

type messageDecoder struct {
	data      []byte
	offset    int
	fragments int
} // end struct messageDecoder

func (decoder *messageDecoder) readMarker(field string) (byte, error) {
	fieldStartOffset := decoder.offset
	decoder.fragments++

	if decoder.fragments > maximumFragments {
		return 0, protocolError("Server message has more than 4096 fragments.", field, fieldStartOffset)
	}

	if decoder.offset >= len(decoder.data) {
		return 0, protocolError("Missing field marker.", field, fieldStartOffset)
	}

	decoder.offset++

	return decoder.data[fieldStartOffset], nil
} // end method readMarker

// readLine reads up to LF, allowing one CR before it, and returns the content
// between. The scan never runs past the content limit, one CR and the LF.
func (decoder *messageDecoder) readLine(field string, fieldStartOffset, maximumLength int) ([]byte, error) {
	lineStart := decoder.offset
	searchEnd := min(len(decoder.data), lineStart+maximumLength+2)
	newline := bytes.IndexByte(decoder.data[lineStart:searchEnd], '\n')

	if newline < 0 {
		if searchEnd-lineStart == maximumLength+2 {
			return nil, protocolError("Line exceeds its "+strconv.Itoa(maximumLength)+"-byte limit.", field, fieldStartOffset)
		}

		return nil, protocolError("Unterminated line.", field, fieldStartOffset)
	}

	contentEnd := lineStart + newline
	decoder.offset = contentEnd + 1

	if contentEnd > lineStart && decoder.data[contentEnd-1] == '\r' {
		contentEnd--
	}

	if contentEnd-lineStart > maximumLength {
		return nil, protocolError("Line exceeds its "+strconv.Itoa(maximumLength)+"-byte limit.", field, fieldStartOffset)
	}

	return decoder.data[lineStart:contentEnd], nil
} // end method readLine

// readUnboundedLine reads a simple string, bounded only by the message.
func (decoder *messageDecoder) readUnboundedLine(field string, fieldStartOffset int) ([]byte, error) {
	return decoder.readLine(field, fieldStartOffset, len(decoder.data))
} // end method readUnboundedLine

func readText(data []byte, field string, fieldStartOffset int) (string, error) {
	if !utf8.Valid(data) {
		return "", protocolError("Invalid UTF-8 text.", field, fieldStartOffset)
	}

	return string(data), nil
} // end function readText

// readDecimal reads the decimal grammar every numeric field shares: an
// optional minus sign, then digits only. The range defaults to signed 64-bit,
// which also bounds bulk and array lengths.
func (decoder *messageDecoder) readDecimal(field string, fieldStartOffset, maximumLineBytes int, minimum, maximum int64) (int64, error) {
	line, err := decoder.readLine(field, fieldStartOffset, maximumLineBytes)

	if err != nil {
		return 0, err
	}

	digits := line

	if len(digits) > 0 && digits[0] == '-' {
		digits = digits[1:]
	}

	if len(digits) == 0 {
		return 0, protocolError("Expected decimal digits.", field, fieldStartOffset)
	}

	for _, character := range digits {
		if character < '0' || character > '9' {
			return 0, protocolError("Expected decimal digits.", field, fieldStartOffset)
		}
	}

	value, err := strconv.ParseInt(string(line), 10, 64)

	if err != nil || value < minimum || value > maximum {
		return 0, protocolError("Integer is outside "+strconv.FormatInt(minimum, 10)+" to "+strconv.FormatInt(maximum, 10)+".", field, fieldStartOffset)
	}

	return value, nil
} // end method readDecimal

func (decoder *messageDecoder) readInteger64Digits(field string, fieldStartOffset int) (int64, error) {
	return decoder.readDecimal(field, fieldStartOffset, maximumInteger64LineBytes, math.MinInt64, math.MaxInt64)
} // end method readInteger64Digits

func (decoder *messageDecoder) readInteger32Digits(field string, fieldStartOffset int) (int32, error) {
	value, err := decoder.readDecimal(field, fieldStartOffset, maximumInteger32LineBytes, math.MinInt32, math.MaxInt32)

	return int32(value), err
} // end method readInteger32Digits

// readTimestamp reads an Integer64, which carries timestamps only.
func (decoder *messageDecoder) readTimestamp() (int64, error) {
	fieldStartOffset := decoder.offset
	marker, err := decoder.readMarker("Timestamp")

	if err != nil {
		return 0, err
	}

	if marker != ':' {
		return 0, protocolError("Expected Integer64 marker.", "Timestamp", fieldStartOffset)
	}

	return decoder.readInteger64Digits("Timestamp", fieldStartOffset)
} // end method readTimestamp

func (decoder *messageDecoder) readInteger32(field string) (int32, error) {
	fieldStartOffset := decoder.offset
	marker, err := decoder.readMarker(field)

	if err != nil {
		return 0, err
	}

	if marker != ';' {
		return 0, protocolError("Expected Integer32 marker.", field, fieldStartOffset)
	}

	return decoder.readInteger32Digits(field, fieldStartOffset)
} // end method readInteger32

// readBytes reads a simple or bulk string; null is reported as nil with ok
// false.
func (decoder *messageDecoder) readBytes(field string) (value []byte, ok bool, err error) {
	fieldStartOffset := decoder.offset
	marker, err := decoder.readMarker(field)

	if err != nil {
		return nil, false, err
	}

	switch marker {
	case '+':
		value, err = decoder.readUnboundedLine(field, fieldStartOffset)

		return value, err == nil, err
	case '$':
		return decoder.readBulkBytes(field, fieldStartOffset)
	default:
		return nil, false, protocolError("Expected simple or bulk byte marker.", field, fieldStartOffset)
	}
} // end method readBytes

func (decoder *messageDecoder) readBulkBytes(field string, fieldStartOffset int) (value []byte, ok bool, err error) {
	length, err := decoder.readInteger64Digits(field, fieldStartOffset)

	if err != nil {
		return nil, false, err
	}

	if length == -1 {
		return nil, false, nil
	}

	if length < 0 {
		return nil, false, protocolError("Invalid bulk byte length.", field, fieldStartOffset)
	}

	if length > int64(len(decoder.data)-decoder.offset) {
		return nil, false, protocolError("Bulk payload exceeds remaining message bytes.", field, fieldStartOffset)
	}

	end := decoder.offset + int(length)
	value = decoder.data[decoder.offset:end]
	decoder.offset = end

	if decoder.offset < len(decoder.data) && decoder.data[decoder.offset] == '\r' {
		decoder.offset++
	}

	if decoder.offset >= len(decoder.data) || decoder.data[decoder.offset] != '\n' {
		return nil, false, protocolError("Missing bulk byte terminator.", field, fieldStartOffset)
	}

	decoder.offset++

	return value, true, nil
} // end method readBulkBytes

func (decoder *messageDecoder) readIdentifier(field string) (string, error) {
	fieldStartOffset := decoder.offset
	identifier, err := decoder.readNullableIdentifier(field)

	if err != nil {
		return "", err
	}

	if identifier == "" {
		return "", protocolError("Identifier cannot be null.", field, fieldStartOffset)
	}

	return identifier, nil
} // end method readIdentifier

// readNullableIdentifier returns "" for null.
func (decoder *messageDecoder) readNullableIdentifier(field string) (string, error) {
	fieldStartOffset := decoder.offset
	value, ok, err := decoder.readBytes(field)

	if err != nil || !ok {
		return "", err
	}

	identifier, err := readText(value, field, fieldStartOffset)

	if err != nil {
		return "", err
	}

	if identifier == "" || bytes.ContainsAny(value, "\r\n") {
		return "", protocolError("Identifier must be nonempty and CR/LF-free.", field, fieldStartOffset)
	}

	return identifier, nil
} // end method readNullableIdentifier

// readPayload copies the payload once, so what the caller receives does not
// share memory with the transport message.
func (decoder *messageDecoder) readPayload(field string) ([]byte, error) {
	fieldStartOffset := decoder.offset
	value, ok, err := decoder.readBytes(field)

	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, protocolError("Payload cannot be null.", field, fieldStartOffset)
	}

	return bytes.Clone(value), nil
} // end method readPayload

func (decoder *messageDecoder) readArrayLength(depth int, field string, markerAlreadyRead bool) (int, error) {
	fieldStartOffset := decoder.offset

	if markerAlreadyRead {
		fieldStartOffset--
	}

	if depth >= maximumDepth {
		return 0, protocolError("Arrays are nested deeper than 32 levels.", field, fieldStartOffset)
	}

	if !markerAlreadyRead {
		marker, err := decoder.readMarker(field)

		if err != nil {
			return 0, err
		}

		if marker != '*' {
			return 0, protocolError("Expected array marker.", field, fieldStartOffset)
		}
	}

	length, err := decoder.readInteger64Digits(field, fieldStartOffset)

	if err != nil {
		return 0, err
	}

	if length < 0 {
		return 0, protocolError("Array length cannot be negative.", field, fieldStartOffset)
	}

	if length > int64(maximumFragments-decoder.fragments) {
		return 0, protocolError("Array length exceeds the 4096-fragment budget.", field, fieldStartOffset)
	}

	return int(length), nil
} // end method readArrayLength

func (decoder *messageDecoder) readConnections(depth int) ([]PresenceConnection, error) {
	length, err := decoder.readArrayLength(depth, "Connections", false)

	if err != nil {
		return nil, err
	}

	connections := make([]PresenceConnection, 0, length)

	for range length {
		fieldStartOffset := decoder.offset
		fields, err := decoder.readArrayLength(depth+1, "Connection", false)

		if err != nil {
			return nil, err
		}

		if fields != 3 {
			return nil, protocolError("Presence connection must contain three fields.", "Connection", fieldStartOffset)
		}

		var connection PresenceConnection

		if connection.TokenReference, err = decoder.readIdentifier("TokenReference"); err != nil {
			return nil, err
		}

		if connection.ConnectionID, err = decoder.readIdentifier("ConnectionID"); err != nil {
			return nil, err
		}

		if connection.Timestamp, err = decoder.readTimestamp(); err != nil {
			return nil, err
		}

		connections = append(connections, connection)
	}

	return connections, nil
} // end method readConnections

func (decoder *messageDecoder) readMessage(depth int, tail bool) (serverMessage, error) {
	fieldStartOffset := decoder.offset
	marker, err := decoder.readMarker("Message")

	if err != nil {
		return nil, err
	}

	switch marker {
	case '*':
		return decoder.readMessageArray(depth, tail)
	case '-':
		return decoder.readErrorMessage(depth)
	case '@':
		return decoder.readCommandMessage(depth, tail)
	default:
		return nil, protocolError("Unexpected server message marker.", "Message", fieldStartOffset)
	}
} // end method readMessage

func (decoder *messageDecoder) readMessageArray(depth int, tail bool) (serverMessage, error) {
	length, err := decoder.readArrayLength(depth, "Messages", true)

	if err != nil {
		return nil, err
	}

	messages := make([]serverMessage, 0, length)

	for index := range length {
		message, err := decoder.readMessage(depth+1, tail && index == length-1)

		if err != nil {
			return nil, err
		}

		messages = append(messages, message)
	}

	return arrayMessage{messages: messages}, nil
} // end method readMessageArray

// readErrorMessage reads an error frame. Every field is self-delimiting, so
// an error may sit anywhere in a batch.
func (decoder *messageDecoder) readErrorMessage(depth int) (serverMessage, error) {
	fieldStartOffset := decoder.offset - 1
	header, err := decoder.readLine("Error", fieldStartOffset, 3)

	if err != nil {
		return nil, err
	}

	if string(header) != "Err" {
		return nil, protocolError("Invalid error header.", "Error", fieldStartOffset)
	}

	var message errorMessage

	if message.errorType, err = decoder.readErrorType(); err != nil {
		return nil, err
	}

	if message.subType, err = decoder.readErrorSubType(); err != nil {
		return nil, err
	}

	if message.message, err = decoder.readPayload("ErrorMessage"); err != nil {
		return nil, err
	}

	if message.resource, err = decoder.readResource(depth); err != nil {
		return nil, err
	}

	return message, nil
} // end method readErrorMessage

func (decoder *messageDecoder) readErrorType() (string, error) {
	fieldStartOffset := decoder.offset
	marker, err := decoder.readMarker("ErrorType")

	if err != nil {
		return "", err
	}

	if marker != '+' {
		return "", protocolError("Expected simple string marker.", "ErrorType", fieldStartOffset)
	}

	return decoder.readErrorName("ErrorType", fieldStartOffset)
} // end method readErrorType

// readErrorSubType returns "" for null.
func (decoder *messageDecoder) readErrorSubType() (string, error) {
	fieldStartOffset := decoder.offset
	marker, err := decoder.readMarker("ErrorSubType")

	if err != nil {
		return "", err
	}

	switch marker {
	case '+':
		return decoder.readErrorName("ErrorSubType", fieldStartOffset)
	case '$':
		length, err := decoder.readInteger64Digits("ErrorSubType", fieldStartOffset)

		if err != nil {
			return "", err
		}

		if length == -1 {
			return "", nil
		}
	}

	return "", protocolError("Sub type must be a simple string or null.", "ErrorSubType", fieldStartOffset)
} // end method readErrorSubType

// readErrorName reads an error type or sub type: a bounded name of letters,
// digits and underscores that starts with a letter, such as
// PermissionDeniedError or PRES_LIST.
func (decoder *messageDecoder) readErrorName(field string, fieldStartOffset int) (string, error) {
	name, err := decoder.readLine(field, fieldStartOffset, maximumErrorNameBytes)

	if err != nil {
		return "", err
	}

	if !validErrorName(name) {
		return "", protocolError("Invalid error name.", field, fieldStartOffset)
	}

	return string(name), nil
} // end method readErrorName

func validErrorName(name []byte) bool {
	if len(name) == 0 {
		return false
	}

	for index, character := range name {
		letter := character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z'
		digit := character >= '0' && character <= '9'

		if !letter && (index == 0 || !digit && character != '_') {
			return false
		}
	}

	return true
} // end function validErrorName

// readResource reads any single fragment an error's type and sub type define:
// null, a string, an Integer64, an Integer32, or an array of these.
func (decoder *messageDecoder) readResource(depth int) (any, error) {
	fieldStartOffset := decoder.offset
	marker, err := decoder.readMarker("Resource")

	if err != nil {
		return nil, err
	}

	switch marker {
	case '+':
		line, err := decoder.readUnboundedLine("Resource", fieldStartOffset)

		if err != nil {
			return nil, err
		}

		return readText(line, "Resource", fieldStartOffset)
	case '$':
		value, ok, err := decoder.readBulkBytes("Resource", fieldStartOffset)

		if err != nil || !ok {
			return nil, err
		}

		return readText(value, "Resource", fieldStartOffset)
	case ':':
		return decoder.readInteger64Digits("Resource", fieldStartOffset)
	case ';':
		return decoder.readInteger32Digits("Resource", fieldStartOffset)
	case '*':
		length, err := decoder.readArrayLength(depth+1, "Resource", true)

		if err != nil {
			return nil, err
		}

		items := make([]any, 0, length)

		for range length {
			item, err := decoder.readResource(depth + 1)

			if err != nil {
				return nil, err
			}

			items = append(items, item)
		}

		return items, nil
	default:
		return nil, protocolError("Unexpected resource marker.", "Resource", fieldStartOffset)
	}
} // end method readResource

func (decoder *messageDecoder) readCommandMessage(depth int, tail bool) (serverMessage, error) {
	fieldStartOffset := decoder.offset - 1
	line, err := decoder.readLine("Command", fieldStartOffset, maximumCommandNameBytes)

	if err != nil {
		return nil, err
	}

	command, err := readText(line, "Command", fieldStartOffset)

	if err != nil {
		return nil, err
	}

	switch command {
	case "MSG":
		return decoder.readDelivery()
	case "SERVER_MSG":
		return decoder.readNotice()
	case "PRES_NOTIFY":
		return decoder.readPresenceNotification()
	case "PRES_LIST_RESPONSE":
		return decoder.readPresenceResponse(depth)
	default:
		return decoder.skipUnknownCommand(depth, tail, fieldStartOffset)
	}
} // end method readCommandMessage

// skipUnknownCommand skips a command this version does not know. It carries an
// unknown number of fields, so its end is knowable only when it runs to the
// end of the transport message. Newer servers may add commands; skipping them
// keeps this client working instead of dropping its connection (DECODE-01).
func (decoder *messageDecoder) skipUnknownCommand(depth int, tail bool, fieldStartOffset int) (serverMessage, error) {
	if depth != 0 && !tail {
		return nil, protocolError("Unknown command inside array has ambiguous boundaries.", "Command", fieldStartOffset)
	}

	decoder.offset = len(decoder.data)

	return ignoredMessage{}, nil
} // end method skipUnknownCommand

func (decoder *messageDecoder) readDelivery() (serverMessage, error) {
	var message deliveryMessage
	var err error

	if message.tokenReference, err = decoder.readIdentifier("TokenReference"); err != nil {
		return nil, err
	}

	if message.segmentID, err = decoder.readIdentifier("SegmentID"); err != nil {
		return nil, err
	}

	if message.messageID, err = decoder.readNullableIdentifier("MessageID"); err != nil {
		return nil, err
	}

	if message.timestamp, err = decoder.readTimestamp(); err != nil {
		return nil, err
	}

	if message.payload, err = decoder.readPayload("Payload"); err != nil {
		return nil, err
	}

	return message, nil
} // end method readDelivery

func (decoder *messageDecoder) readNotice() (serverMessage, error) {
	var message noticeMessage
	var err error

	if message.timestamp, err = decoder.readTimestamp(); err != nil {
		return nil, err
	}

	if message.payload, err = decoder.readPayload("Payload"); err != nil {
		return nil, err
	}

	return message, nil
} // end method readNotice

func (decoder *messageDecoder) readPresenceNotification() (serverMessage, error) {
	var message presenceNotifyMessage
	var err error

	if message.segmentID, err = decoder.readIdentifier("SegmentID"); err != nil {
		return nil, err
	}

	if message.tokenReference, err = decoder.readIdentifier("TokenReference"); err != nil {
		return nil, err
	}

	if message.connectionID, err = decoder.readIdentifier("ConnectionID"); err != nil {
		return nil, err
	}

	eventOffset := decoder.offset
	event, err := decoder.readInteger32("Event")

	if err != nil {
		return nil, err
	}

	// A join/leave flag, not metadata: narrowed here rather than passed
	// through raw, and any other value is not a flag this client knows.
	if event != 0 && event != 1 {
		return nil, protocolError("Presence event must be 0 or 1.", "Event", eventOffset)
	}

	message.joined = event == 1

	if message.timestamp, err = decoder.readTimestamp(); err != nil {
		return nil, err
	}

	return message, nil
} // end method readPresenceNotification

func (decoder *messageDecoder) readPresenceResponse(depth int) (serverMessage, error) {
	var message presenceListMessage
	var err error

	if message.segmentID, err = decoder.readIdentifier("SegmentID"); err != nil {
		return nil, err
	}

	if message.requestID, err = decoder.readIdentifier("RequestID"); err != nil {
		return nil, err
	}

	figures := []struct {
		field string
		value *int32
	}{
		{"Total", &message.total},
		{"PerPage", &message.perPage},
		{"CurrentPage", &message.currentPage},
		{"From", &message.from},
		{"To", &message.to},
	}

	for _, figure := range figures {
		if *figure.value, err = decoder.readInteger32(figure.field); err != nil {
			return nil, err
		}
	}

	if message.connections, err = decoder.readConnections(depth); err != nil {
		return nil, err
	}

	return message, nil
} // end method readPresenceResponse
