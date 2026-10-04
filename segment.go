package celeris

import "context"

// Segment is a handle for one segment of a channel. It holds only its id:
// subscriptions, listeners and the connection belong to the channel, so any
// number of handles for one segment share them. Obtain one with
// [Channel.Segment] or [Channel.DefaultSegment].
type Segment struct {
	channel *Channel
	id      string
}

// ID returns the segment's id.
func (segment *Segment) ID() string {
	return segment.id
}

// Subscribe registers interest in the segment's messages. The first interest
// in a segment subscribes it, and it stays subscribed until every interest
// is cancelled; subscriptions are restored after every reconnect. The default
// segment is joined automatically and never subscribed or unsubscribed.
//
// No acknowledgement exists: a denial arrives later through
// [ChannelEventHandler.OnError]. It fails with [ErrNotConnected] once the
// channel is closing or closed.
func (segment *Segment) Subscribe() (*Subscription, error) {
	return segment.channel.addInterest(messageInterest, segment.id)
}

// SubscribePresence registers interest in the segment's presence joins and
// leaves. It also keeps the segment joined for messages; cancelling it does
// not leave the segment.
func (segment *Segment) SubscribePresence() (*Subscription, error) {
	return segment.channel.addInterest(presenceInterest, segment.id)
}

// OnMessage registers a listener for the segment's deliveries. The payload is
// the SDK's own copy, shared by every listener of that delivery: treat it as
// read-only. Each message id is delivered once within a bounded window
// (REV-01).
func (segment *Segment) OnMessage(listener func(payload []byte, metadata MessageMetadata)) (remove func()) {
	requireListener(listener == nil)

	channel := segment.channel
	channel.mutex.Lock()
	defer channel.mutex.Unlock()

	return channel.addListenerLocked(listenersOf(channel.messageListeners, segment.id), listener)
}

// OnPresence registers a listener for joins and leaves. Events arrive only
// while a presence subscription is held: the server sends them to presence
// subscribers alone (PRES-01).
func (segment *Segment) OnPresence(listener func(PresenceEvent)) (remove func()) {
	requireListener(listener == nil)

	channel := segment.channel
	channel.mutex.Lock()
	defer channel.mutex.Unlock()

	return channel.addListenerLocked(listenersOf(channel.presenceListeners, segment.id), listener)
}

// Publish sends payload to the segment with a generated message id. It
// returns once the socket accepted the bytes. That is not receipt, delivery
// or durability: the protocol has no acknowledgements.
//
// It fails with [ErrNotConnected] while not connected (there is no offline
// queue), with [ErrBackpressure] when 64 publishes are already waiting, and
// with [ErrDeliveryUnknown] when the socket failed mid-write; such a publish
// is never resent. A publish over the plan's payload cap still succeeds here
// and is refused afterwards with a MessageSizeLimitError through
// [ChannelEventHandler.OnError].
func (segment *Segment) Publish(ctx context.Context, payload []byte) error {
	return segment.channel.publish(ctx, segment.id, payload, generateMessageID())
}

// PublishWithMessageID is [Segment.Publish] with your own message id.
// Receivers drop a repeated id within their deduplication window, so reuse an
// id only to resend the same message.
func (segment *Segment) PublishWithMessageID(ctx context.Context, payload []byte, messageID string) error {
	return segment.channel.publish(ctx, segment.id, payload, messageID)
}

// PresenceList returns one page of the segment's presence. One query may be
// in flight per channel; it fails with [ErrOperationInProgress] while another
// is pending, and with [ErrTimeout] after the presence query timeout.
//
// The reply is routed by the goroutine that runs listeners, so a query made
// inside a listener always times out. Call it from your own goroutine.
func (segment *Segment) PresenceList(ctx context.Context, page, perPage int32) (PresencePage, error) {
	return segment.channel.queryPresence(ctx, segment.id, page, perPage)
}

// Subscription is one registered interest. Cancel it when done.
type Subscription struct {
	channel   *Channel
	kind      interestKind
	segmentID string
	cancelled bool
}

// Cancel releases the interest at once. The segment is unsubscribed when its
// last message and presence interests are both released; cancelling presence
// never leaves the segment. Calling Cancel again does nothing.
func (subscription *Subscription) Cancel() {
	channel := subscription.channel
	channel.mutex.Lock()
	defer channel.mutex.Unlock()

	if subscription.cancelled {
		return
	}

	subscription.cancelled = true
	channel.releaseInterest(subscription.kind, subscription.segmentID)
}

// listenersOf returns a segment's listener set, creating it on first use.
func listenersOf[Listener any](sets map[string]*listenerSet[Listener], segmentID string) *listenerSet[Listener] {
	listeners := sets[segmentID]

	if listeners == nil {
		listeners = &listenerSet[Listener]{}
		sets[segmentID] = listeners
	}

	return listeners
}
