package celeris

import (
	"container/list"
	"slices"
	"strconv"
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
} // end struct queuedPublish

func (publish *queuedPublish) settle(err error) {
	if publish.settled {
		return
	}

	publish.settled = true
	publish.result <- err
} // end method settle

type sentInterest struct {
	kind      interestKind
	segmentID string
	sentAt    time.Time
} // end struct sentInterest

type sentPublish struct {
	publish *queuedPublish
	sentAt  time.Time
} // end struct sentPublish

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
} // end struct commandQueue

func (queue *commandQueue) queueInterest(kind interestKind, segmentID string) {
	queue.markInterest(kind, segmentID)
	queue.drain()
} // end method queueInterest

// publish queues a publish, which settles once written to the socket.
func (queue *commandQueue) publish(segmentID string, data []byte) (*queuedPublish, error) {
	limit := queue.channel.client.publishQueueSize

	if len(queue.publishes) >= limit {
		return nil, newError(ErrBackpressure, "The publish queue is full (size "+strconv.Itoa(limit)+"). Retry once some publishes have gone out.")
	}

	queue.sequence++
	publish := &queuedPublish{segmentID: segmentID, data: data, sequence: queue.sequence, result: make(chan error, 1)}
	queue.publishes = append(queue.publishes, publish)
	queue.drain()

	return publish, nil
} // end method publish

// withdraw removes a publish that has not been handed to the writer, and
// reports whether it did.
func (queue *commandQueue) withdraw(publish *queuedPublish) bool {
	index := slices.Index(queue.publishes, publish)

	if index < 0 {
		return false
	}

	queue.publishes = slices.Delete(queue.publishes, index, index+1)

	return true
} // end method withdraw

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
} // end method sendNow

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
} // end method receiveRateLimit

// reset fails every waiting publish with err and empties the queue, for a
// channel that stopped in failed or closed: nothing waits for a later
// connect (QUEUE-01).
func (queue *commandQueue) reset(err error) {
	for _, publish := range queue.publishes {
		publish.settle(err)
	}

	queue.publishes = nil

	for _, kind := range interestKinds {
		queue.pendingInterests[kind].clear()
	}

	queue.dropConnection()
} // end method reset

// dropConnection forgets everything tied to the socket that dropped and keeps
// what still waits to be sent (QUEUE-01): publishes never handed to a socket,
// and subscription changes, which still follow the publishes queued before
// them. A copy queued for a rate-limit resend is dropped: a publish the socket
// never wrote comes back from the writer (connection.takeUnwritten), and one
// it wrote is never sent again. The rate-limit streak and the probe schedule
// stay, since a reconnect does not refill a quota.
func (queue *commandQueue) dropConnection() {
	stopTimer(&queue.pauseTimer)
	stopTimer(&queue.probeTimer)

	for _, kind := range interestKinds {
		queue.abandonedInterests[kind].clear()
	}

	queue.recentInterests = nil
	queue.recentPublishes = nil
	queue.firstSentSinceRateLimitAt = time.Time{}
	queue.publishes = slices.DeleteFunc(queue.publishes, func(publish *queuedPublish) bool {
		return publish.connection != nil
	})
} // end method dropConnection

// putBack returns publishes a dropped socket never started writing to the
// front of the queue, in their order, ahead of everything queued after them.
func (queue *commandQueue) putBack(publishes []*queuedPublish) {
	queue.publishes = append(publishes, queue.publishes...)
} // end method putBack

// restore syncs every held subscription to a new socket: messages, then
// presence, each in registration order. A sequence of zero puts it ahead of
// every queued publish, even one queued before the connection dropped, so the
// connection is a member of its segments again before its publishes join
// theirs (QUEUE-01).
//
// A new socket holds no subscription, so a waiting change that drops one is
// kept only behind a queued publish to its segment, which joins it. A publish
// never subscribes to presence.
func (queue *commandQueue) restore() {
	for _, kind := range interestKinds {
		pending := &queue.pendingInterests[kind]

		for _, segmentID := range pending.keys() {
			published := slices.ContainsFunc(queue.publishes, func(publish *queuedPublish) bool {
				return publish.segmentID == segmentID
			})

			if kind == presenceInterest || !published {
				pending.remove(segmentID)
			}
		}

		for _, segmentID := range queue.channel.interests(kind).keys() {
			pending.remove(segmentID)
			pending.set(segmentID, 0)
		}
	}

	queue.drain()
} // end method restore

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
} // end method handOff

// markInterest gives the change a newer sequence, so the sync follows every
// publish queued before it.
func (queue *commandQueue) markInterest(kind interestKind, segmentID string) {
	queue.sequence++
	queue.pendingInterests[kind].set(segmentID, queue.sequence)
} // end method markInterest

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
} // end method requeueRecent

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
} // end method abandonRecent

func (queue *commandQueue) restoreAbandoned() {
	for _, kind := range interestKinds {
		for _, segmentID := range queue.abandonedInterests[kind].keys() {
			queue.markInterest(kind, segmentID)
		}

		queue.abandonedInterests[kind].clear()
	}
} // end method restoreAbandoned

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
} // end method endProbingIfQuotaReturned

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
} // end method drain

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
} // end method nextReadyInterest

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
} // end method handOffInterest

// forget drops a cancelled publish from the resend record.
func (queue *commandQueue) forget(publish *queuedPublish) {
	queue.recentPublishes = slices.DeleteFunc(queue.recentPublishes, func(sent sentPublish) bool {
		return sent.publish == publish
	})
} // end method forget

func (queue *commandQueue) recordSentInterest(kind interestKind, segmentID string) {
	now := time.Now()
	expired := 0

	for expired < len(queue.recentInterests) && now.Sub(queue.recentInterests[expired].sentAt) > rateLimitSuspectWindow {
		expired++
	}

	queue.recentInterests = append(queue.recentInterests[expired:], sentInterest{kind: kind, segmentID: segmentID, sentAt: now})
} // end method recordSentInterest

func (queue *commandQueue) recordSentPublish(publish *queuedPublish) {
	now := time.Now()
	expired := 0

	for expired < len(queue.recentPublishes) && (len(queue.recentPublishes)-expired >= maximumPendingCommands || now.Sub(queue.recentPublishes[expired].sentAt) > rateLimitSuspectWindow) {
		expired++
	}

	queue.recentPublishes = append(queue.recentPublishes[expired:], sentPublish{publish: publish, sentAt: now})
} // end method recordSentPublish

// stopTimer stops a timer, if there is one, and forgets it.
func stopTimer(timer **time.Timer) {
	if *timer != nil {
		(*timer).Stop()
		*timer = nil
	}
} // end function stopTimer

// orderedMap keeps keys in the order they were first set, as the reference's
// Map does: restoration and resends follow registration order. Setting,
// reading and removing a key take constant time.
type orderedMap[Value any] struct {
	order    list.List
	elements map[string]*list.Element
} // end struct orderedMap

type orderedEntry[Value any] struct {
	key   string
	value Value
} // end struct orderedEntry

func (entries *orderedMap[Value]) set(key string, value Value) {
	if element, exists := entries.elements[key]; exists {
		element.Value = orderedEntry[Value]{key, value}

		return
	}

	if entries.elements == nil {
		entries.elements = map[string]*list.Element{}
	}

	entries.elements[key] = entries.order.PushBack(orderedEntry[Value]{key, value})
} // end method set

func (entries *orderedMap[Value]) get(key string) (Value, bool) {
	element, exists := entries.elements[key]

	if !exists {
		var zero Value

		return zero, false
	}

	return element.Value.(orderedEntry[Value]).value, true
} // end method get

func (entries *orderedMap[Value]) remove(key string) {
	if element, exists := entries.elements[key]; exists {
		entries.order.Remove(element)
		delete(entries.elements, key)
	}
} // end method remove

// each visits the entries in order until visit returns false.
func (entries *orderedMap[Value]) each(visit func(key string, value Value) bool) {
	for element := entries.order.Front(); element != nil; element = element.Next() {
		entry := element.Value.(orderedEntry[Value])

		if !visit(entry.key, entry.value) {
			return
		}
	}
} // end method each

func (entries *orderedMap[Value]) keys() []string {
	keys := make([]string, 0, len(entries.elements))
	entries.each(func(key string, _ Value) bool {
		keys = append(keys, key)

		return true
	})

	return keys
} // end method keys

func (entries *orderedMap[Value]) length() int {
	return len(entries.elements)
} // end method length

func (entries *orderedMap[Value]) clear() {
	entries.order.Init()
	entries.elements = nil
} // end method clear
