package celeris

import (
	"container/list"
	"slices"
	"time"
)

type interestKind int

const (
	messageInterest interestKind = iota
	presenceInterest
)

var interestKinds = [...]interestKind{messageInterest, presenceInterest}

type queuedPublish struct {
	segmentID string
	data      []byte

	// Its place in the order commands were queued in.
	sequence uint64

	resends int

	// Receives the outcome once; settled guards it under the channel's mutex.
	result  chan error
	settled bool

	// The connection it was last handed to, or nil while it is queued.
	connection *connection
}

func (publish *queuedPublish) settle(err error) {
	if publish.settled {
		return
	}

	publish.settled = true
	publish.result <- err
}

type sentInterest struct {
	kind      interestKind
	segmentID string
	sentAt    time.Time
}

type sentPublish struct {
	publish *queuedPublish
	sentAt  time.Time
}

// commandQueue sends subscription changes ahead of publishes, waits for room
// in the writer, and resends recent commands after a rate limit
// (RESEND-01). The server never says which frame a rate limit dropped, so
// everything sent within the suspect window is resent: subscriptions as their
// current state, publishes once and with their original message id, so
// receivers drop any copy that got through.
//
// It belongs to its channel and is guarded by the channel's mutex.
type commandQueue struct {
	channel *Channel

	// Per kind: segment → the sequence of its latest change, in the order the
	// segments were first marked.
	pendingInterests [len(interestKinds)]orderedMap[uint64]

	publishes []*queuedPublish
	sequence  uint64

	recentInterests []sentInterest

	// Holds payloads, so it keeps no more than maximumPendingCommands.
	recentPublishes []sentPublish

	pauseTimer            *time.Timer
	rateLimitStreak       int
	rateLimitStreakEndsAt time.Time

	// Subscriptions dropped while the limit was treated as a used-up quota,
	// re-sent by a probe on a slow, doubling schedule.
	abandonedInterests [len(interestKinds)]orderedMap[struct{}]
	probeTimer         *time.Timer
	probeCount         int

	// Zero until a command is sent after the latest rate limit.
	firstSentSinceRateLimitAt time.Time
}

func (queue *commandQueue) queueInterest(kind interestKind, segmentID string) {
	queue.markInterest(kind, segmentID)
	queue.drain()
}

// publish queues a publish, which settles once written to the socket.
func (queue *commandQueue) publish(segmentID string, data []byte) (*queuedPublish, error) {
	if len(queue.publishes) >= maximumPendingCommands {
		return nil, newError(ErrBackpressure, "64 publishes are already waiting to be sent. Retry once some have gone out.")
	}

	queue.sequence++
	publish := &queuedPublish{segmentID: segmentID, data: data, sequence: queue.sequence, result: make(chan error, 1)}
	queue.publishes = append(queue.publishes, publish)
	queue.drain()

	return publish, nil
}

// withdraw removes a publish that has not been handed to the writer, and
// reports whether it did.
func (queue *commandQueue) withdraw(publish *queuedPublish) bool {
	index := slices.Index(queue.publishes, publish)

	if index < 0 {
		return false
	}

	queue.publishes = slices.Delete(queue.publishes, index, index+1)

	return true
}

// sendNow hands over a command that is never queued or resent, such as a
// presence query.
func (queue *commandQueue) sendNow(connection *connection, data []byte) error {
	if queue.pauseTimer != nil {
		return newError(ErrBackpressure, "Sending is paused after a rate limit; try again in a moment.")
	}

	if !connection.hasRoom(len(data)) {
		return newError(ErrBackpressure, "Command writer is full: 64 commands are waiting to be sent. Retry once the socket has flushed them.")
	}

	queue.handOff(connection, &outboundFrame{data: data})

	return nil
}

func (queue *commandQueue) receiveRateLimit() {
	// A limit arriving while sending is paused, with nothing sent since the
	// last one, reports the same episode through another limit type (the
	// server throttles each type separately). It carries nothing new.
	if queue.pauseTimer != nil && queue.firstSentSinceRateLimitAt.IsZero() {
		return
	}

	now := time.Now()
	queue.endProbingIfQuotaReturned(now, quotaReturnConfirmation)

	// While probing the streak holds, so a probe's own limit cannot start
	// another full run of resends.
	if queue.probeCount == 0 && now.After(queue.rateLimitStreakEndsAt) {
		queue.rateLimitStreak = 0
	}

	// A limit that keeps returning is a used-up quota rather than a burst,
	// and resending into it would never succeed.
	if queue.rateLimitStreak < maximumConsecutiveRateLimits {
		queue.requeueRecent(now)
	} else {
		queue.abandonRecent()
	}

	queue.recentInterests = nil
	queue.recentPublishes = nil
	queue.firstSentSinceRateLimitAt = time.Time{}

	delay := rateLimitCooldown + retryDelay(queue.rateLimitStreak, queue.channel.client.random)
	queue.rateLimitStreak++
	queue.rateLimitStreakEndsAt = now.Add(delay + rateLimitSuspectWindow)

	stopTimer(&queue.pauseTimer)

	var timer *time.Timer

	timer = time.AfterFunc(delay, func() {
		queue.channel.mutex.Lock()
		defer queue.channel.mutex.Unlock()

		if queue.pauseTimer != timer {
			return
		}

		queue.pauseTimer = nil
		queue.drain()
	})

	queue.pauseTimer = timer
}

// reset fails waiting publishes and forgets everything tied to the socket:
// the next one re-syncs every subscription itself. The rate-limit streak and
// the probe schedule stay, since a reconnect does not refill a quota.
func (queue *commandQueue) reset(err error) {
	stopTimer(&queue.pauseTimer)
	stopTimer(&queue.probeTimer)

	for _, kind := range interestKinds {
		queue.pendingInterests[kind].clear()
		queue.abandonedInterests[kind].clear()
	}

	queue.recentInterests = nil
	queue.recentPublishes = nil
	queue.firstSentSinceRateLimitAt = time.Time{}

	publishes := queue.publishes
	queue.publishes = nil

	for _, publish := range publishes {
		publish.settle(err)
	}
}

// handOff gives a command to the writer and records it as sent, as the
// reference does when it hands a command to the socket: a rate limit resends
// it, and it shows that commands flowed since the last limit.
func (queue *commandQueue) handOff(connection *connection, frame *outboundFrame) {
	connection.handOff(frame)

	if frame.publish != nil {
		queue.recordSentPublish(frame.publish)
	}

	if queue.firstSentSinceRateLimitAt.IsZero() {
		queue.firstSentSinceRateLimitAt = time.Now()
	}
}

// markInterest gives the change a newer sequence, so the sync follows every
// publish queued before it.
func (queue *commandQueue) markInterest(kind interestKind, segmentID string) {
	queue.sequence++
	queue.pendingInterests[kind].set(segmentID, queue.sequence)
}

func (queue *commandQueue) requeueRecent(now time.Time) {
	for _, sent := range queue.recentInterests {
		if now.Sub(sent.sentAt) <= rateLimitSuspectWindow {
			queue.markInterest(sent.kind, sent.segmentID)
		}
	}

	var resent []*queuedPublish

	for _, sent := range queue.recentPublishes {
		if now.Sub(sent.sentAt) <= rateLimitSuspectWindow && sent.publish.resends < maximumPublishResends {
			sent.publish.resends++
			resent = append(resent, sent.publish)
		}
	}

	queue.publishes = append(resent, queue.publishes...)
}

// abandonRecent drops recent publishes, and hands every subscription sent
// since the previous limit to the quota probe, however late the report:
// syncs are idempotent, so over-abandoning costs at most a redundant frame,
// while missing one loses the subscription.
func (queue *commandQueue) abandonRecent() {
	for _, sent := range queue.recentInterests {
		queue.abandonedInterests[sent.kind].set(sent.segmentID, struct{}{})
	}

	abandoned := queue.abandonedInterests[messageInterest].length() > 0 || queue.abandonedInterests[presenceInterest].length() > 0

	if !abandoned || queue.probeTimer != nil {
		return
	}

	delay := quotaProbeMaximumDelay

	if queue.probeCount < 16 {
		delay = min(quotaProbeMaximumDelay, quotaProbeFirstDelay<<queue.probeCount)
	}

	queue.probeCount++

	var timer *time.Timer

	timer = time.AfterFunc(delay, func() {
		queue.channel.mutex.Lock()
		defer queue.channel.mutex.Unlock()

		if queue.probeTimer != timer {
			return
		}

		queue.probeTimer = nil
		queue.restoreAbandoned()
		queue.drain()
	})

	queue.probeTimer = timer
}

func (queue *commandQueue) restoreAbandoned() {
	for _, kind := range interestKinds {
		for _, segmentID := range queue.abandonedInterests[kind].keys() {
			queue.markInterest(kind, segmentID)
		}

		queue.abandonedInterests[kind].clear()
	}
}

// endProbingIfQuotaReturned restores abandoned subscriptions at once when
// commands went quietSpan without a rate limit following them: the quota is
// back.
func (queue *commandQueue) endProbingIfQuotaReturned(now time.Time, quietSpan time.Duration) {
	if queue.probeCount == 0 || queue.firstSentSinceRateLimitAt.IsZero() || now.Sub(queue.firstSentSinceRateLimitAt) <= quietSpan {
		return
	}

	stopTimer(&queue.probeTimer)
	queue.probeCount = 0
	queue.rateLimitStreak = 0
	queue.restoreAbandoned()
}

// drain hands commands to the writer while it has room. The writer drains
// again whenever it finishes a write, so a full writer only delays commands.
func (queue *commandQueue) drain() {
	connection := queue.channel.connection

	if connection == nil || queue.pauseTimer != nil {
		return
	}

	queue.endProbingIfQuotaReturned(time.Now(), rateLimitSuspectWindow)

	for {
		if kind, segmentID, ok := queue.nextReadyInterest(); ok {
			if !queue.handOffInterest(connection, kind, segmentID) {
				return
			}

			continue
		}

		if len(queue.publishes) == 0 || !connection.hasRoom(len(queue.publishes[0].data)) {
			return
		}

		publish := queue.publishes[0]
		queue.publishes = queue.publishes[1:]
		publish.connection = connection
		queue.handOff(connection, &outboundFrame{data: publish.data, publish: publish})
	}
}

// nextReadyInterest finds the first subscription change with no earlier
// publish to its segment still queued: publishing joins the segment, so a
// change has to follow the publishes queued before it for the segment to end
// up as asked.
func (queue *commandQueue) nextReadyInterest() (interestKind, string, bool) {
	for _, kind := range interestKinds {
		ready := ""

		queue.pendingInterests[kind].each(func(segmentID string, sequence uint64) bool {
			blocked := slices.ContainsFunc(queue.publishes, func(publish *queuedPublish) bool {
				return publish.segmentID == segmentID && publish.sequence < sequence
			})

			if !blocked {
				ready = segmentID
			}

			return blocked
		})

		if ready != "" {
			return kind, ready, true
		}
	}

	return 0, "", false
}

// handOffInterest hands the writer the command that brings the server in line
// with the segment's interest as it stands now. It reports false when the
// writer has no room.
func (queue *commandQueue) handOffInterest(connection *connection, kind interestKind, segmentID string) bool {
	if command := queue.channel.interestCommand(kind, segmentID); command != "" {
		// The segment was validated when its handle was made, so encoding
		// cannot fail.
		data, err := encodeSegmentCommand(command, segmentID)

		if err == nil {
			if !connection.hasRoom(len(data)) {
				return false
			}

			queue.handOff(connection, &outboundFrame{data: data})
			queue.recordSentInterest(kind, segmentID)
		}
	}

	queue.pendingInterests[kind].remove(segmentID)

	return true
}

// forget drops a cancelled publish from the resend record.
func (queue *commandQueue) forget(publish *queuedPublish) {
	queue.recentPublishes = slices.DeleteFunc(queue.recentPublishes, func(sent sentPublish) bool {
		return sent.publish == publish
	})
}

func (queue *commandQueue) recordSentInterest(kind interestKind, segmentID string) {
	now := time.Now()
	expired := 0

	for expired < len(queue.recentInterests) && now.Sub(queue.recentInterests[expired].sentAt) > rateLimitSuspectWindow {
		expired++
	}

	queue.recentInterests = append(queue.recentInterests[expired:], sentInterest{kind: kind, segmentID: segmentID, sentAt: now})
}

func (queue *commandQueue) recordSentPublish(publish *queuedPublish) {
	now := time.Now()
	expired := 0

	for expired < len(queue.recentPublishes) && (len(queue.recentPublishes)-expired >= maximumPendingCommands || now.Sub(queue.recentPublishes[expired].sentAt) > rateLimitSuspectWindow) {
		expired++
	}

	queue.recentPublishes = append(queue.recentPublishes[expired:], sentPublish{publish: publish, sentAt: now})
}

// stopTimer stops a timer, if there is one, and forgets it.
func stopTimer(timer **time.Timer) {
	if *timer != nil {
		(*timer).Stop()
		*timer = nil
	}
}

// orderedMap keeps keys in the order they were first set, as the reference's
// Map does: restoration and resends follow registration order. Setting,
// reading and removing a key take constant time.
type orderedMap[Value any] struct {
	order    list.List
	elements map[string]*list.Element
}

type orderedEntry[Value any] struct {
	key   string
	value Value
}

func (entries *orderedMap[Value]) set(key string, value Value) {
	if element, exists := entries.elements[key]; exists {
		element.Value = orderedEntry[Value]{key, value}

		return
	}

	if entries.elements == nil {
		entries.elements = map[string]*list.Element{}
	}

	entries.elements[key] = entries.order.PushBack(orderedEntry[Value]{key, value})
}

func (entries *orderedMap[Value]) get(key string) (Value, bool) {
	element, exists := entries.elements[key]

	if !exists {
		var zero Value

		return zero, false
	}

	return element.Value.(orderedEntry[Value]).value, true
}

func (entries *orderedMap[Value]) remove(key string) {
	if element, exists := entries.elements[key]; exists {
		entries.order.Remove(element)
		delete(entries.elements, key)
	}
}

// each visits the entries in order until visit returns false.
func (entries *orderedMap[Value]) each(visit func(key string, value Value) bool) {
	for element := entries.order.Front(); element != nil; element = element.Next() {
		entry := element.Value.(orderedEntry[Value])

		if !visit(entry.key, entry.value) {
			return
		}
	}
}

func (entries *orderedMap[Value]) keys() []string {
	keys := make([]string, 0, len(entries.elements))
	entries.each(func(key string, _ Value) bool {
		keys = append(keys, key)

		return true
	})

	return keys
}

func (entries *orderedMap[Value]) length() int {
	return len(entries.elements)
}

func (entries *orderedMap[Value]) clear() {
	entries.order.Init()
	entries.elements = nil
}
