package celeris

import "strconv"

const commandTooLarge = "Encoded command exceeds 2 MiB. That is the most the server accepts on any plan; send a smaller payload."

// encodePublish encodes PUB. An empty messageID encodes as null; the channel
// always supplies one (RESEND-01), so null is reachable only from tests.
func encodePublish(segmentID, messageID string, payload []byte) ([]byte, error) {
	var failures []string

	if rule := identifierRule(segmentID); rule != "" {
		failures = append(failures, failure("SegmentID", rule))
	}

	if messageID != "" {
		if rule := identifierRule(messageID); rule != "" {
			failures = append(failures, failure("MessageID", rule))
		}
	}

	if failures != nil {
		return nil, configurationError("command", failures...)
	}

	size := len("@PUB\n") + bulkSize(len(segmentID)) + bulkSize(len(payload))

	if messageID == "" {
		size += len("$-1\n")
	} else {
		size += bulkSize(len(messageID))
	}

	if size > maximumCommandBytes {
		return nil, newError(ErrConfiguration, commandTooLarge)
	}

	command := make([]byte, 0, size)
	command = append(command, "@PUB\n"...)
	command = appendBulk(command, segmentID)

	if messageID == "" {
		command = append(command, "$-1\n"...)
	} else {
		command = appendBulk(command, messageID)
	}

	return appendBulk(command, payload), nil
}

// encodeSegmentCommand encodes SUB, UNSUB, PRES_SUB or PRES_UNSUB.
func encodeSegmentCommand(name, segmentID string) ([]byte, error) {
	switch name {
	case subscribeCommand, unsubscribeCommand, presenceSubscribeCommand, presenceUnsubscribeCommand:
	default:
		return nil, newError(ErrConfiguration, "Invalid command. Unsupported command type.")
	}

	if rule := identifierRule(segmentID); rule != "" {
		return nil, configurationError("command", failure("SegmentID", rule))
	}

	size := len(name) + 2 + bulkSize(len(segmentID))

	if size > maximumCommandBytes {
		return nil, newError(ErrConfiguration, commandTooLarge)
	}

	command := make([]byte, 0, size)
	command = append(command, '@')
	command = append(command, name...)
	command = append(command, '\n')

	return appendBulk(command, segmentID), nil
}

// encodePresenceList encodes PRES_LIST with the query's request id
// (QUERY-01).
func encodePresenceList(segmentID string, page, perPage int32, requestID string) ([]byte, error) {
	var failures []string

	if rule := identifierRule(segmentID); rule != "" {
		failures = append(failures, failure("SegmentID", rule))
	}

	if page < 1 {
		failures = append(failures, failure("Page", "Must be at least 1"))
	}

	if perPage < 1 {
		failures = append(failures, failure("PerPage", "Must be at least 1"))
	}

	if perPage > maximumPresencePerPage {
		failures = append(failures, failure("PerPage", "Must be at most 100"))
	}

	if rule := identifierRule(requestID); rule != "" {
		failures = append(failures, failure("RequestID", rule))
	}

	if failures != nil {
		return nil, configurationError("command", failures...)
	}

	figures := len(";\n;\n") + len(strconv.Itoa(int(page))) + len(strconv.Itoa(int(perPage)))
	size := len("@PRES_LIST\n") + bulkSize(len(segmentID)) + figures + bulkSize(len(requestID))

	if size > maximumCommandBytes {
		return nil, newError(ErrConfiguration, commandTooLarge)
	}

	command := make([]byte, 0, size)
	command = append(command, "@PRES_LIST\n"...)
	command = appendBulk(command, segmentID)
	command = append(command, ';')
	command = strconv.AppendInt(command, int64(page), 10)
	command = append(command, "\n;"...)
	command = strconv.AppendInt(command, int64(perPage), 10)
	command = append(command, '\n')

	return appendBulk(command, requestID), nil
}

// bulkSize is the encoded size of a bulk string of the given length.
func bulkSize(length int) int {
	return len("$\n\n") + len(strconv.Itoa(length)) + length
}

func appendBulk[Bytes string | []byte](command []byte, value Bytes) []byte {
	command = append(command, '$')
	command = strconv.AppendInt(command, int64(len(value)), 10)
	command = append(command, '\n')
	command = append(command, value...)

	return append(command, '\n')
}
