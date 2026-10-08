package celeris

import "context"

// Segment is a handle for one segment of a channel. It holds only its id:
// subscriptions, listeners and the connection belong to the channel, so any
// number of handles for one segment share them. Obtain one with
// [Channel.Segment] or [Channel.DefaultSegment].
type Segment struct {
	channel *Channel
	id      string
} // end struct Segment

// ID returns the segment's id.
func (segment *Segment) ID() string {
	return segment.id
} // end method ID

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
} // end method Subscribe

// SubscribePresence registers interest in the segment's presence joins and
// leaves. Watching presence is not membership: it delivers no messages, and
// the watcher is not itself announced or listed.
func (segment *Segment) SubscribePresence() (*Subscription, error) {
	return segment.channel.addInterest(presenceInterest, segment.id)
} // end method SubscribePresence

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
} // end method OnMessage

// OnPresence registers a listener for joins and leaves. Events arrive only
// while a presence subscription is held: the server sends them to presence
// subscribers alone (PRES-01).
func (segment *Segment) OnPresence(listener func(PresenceEvent)) (remove func()) {
	requireListener(listener == nil)

	channel := segment.channel
	channel.mutex.Lock()
	defer channel.mutex.Unlock()

	return channel.addListenerLocked(listenersOf(channel.presenceListeners, segment.id), listener)
} // end method OnPresence

// Publish sends payload to the segment with a generated message id. It
// returns once the socket accepted the bytes. That is not receipt, delivery
// or durability: the protocol has no acknowledgements.
//
// While the channel reconnects, the publish waits in the queue and is sent
// after the reconnect, once the restored subscriptions have gone out. It then
// fails with the error [ChannelEventHandler.OnError] reports if recovery stops
// in [StateFailed], and with [ErrCancelled] if the channel closes.
//
// It fails with [ErrNotConnected] before the first connect, while connecting,
// and once failed or closed (there is no offline queue), with
// [ErrBackpressure] when the publish queue ([ClientOptions.PublishQueueSize],
// 64 by default) is already full, and with [ErrDeliveryUnknown] when the
// socket failed mid-write; such a publish is never resent. A publish over the plan's payload cap still succeeds here
// and is refused afterwards with a MessageSizeLimitError through
// [ChannelEventHandler.OnError].
func (segment *Segment) Publish(ctx context.Context, payload []byte) error {
	return segment.channel.publish(ctx, segment.id, payload, generateMessageID())
} // end method Publish

// PublishWithMessageID is [Segment.Publish] with your own message id.
// Receivers drop a repeated id within their deduplication window, so reuse an
// id only to resend the same message.
func (segment *Segment) PublishWithMessageID(ctx context.Context, payload []byte, messageID string) error {
	return segment.channel.publish(ctx, segment.id, payload, messageID)
} // end method PublishWithMessageID

// PresenceList returns one page of the segment's presence. One query may be
// in flight per channel; it fails with [ErrOperationInProgress] while another
// is pending, and with [ErrTimeout] after the presence query timeout.
//
// The reply is routed by the goroutine that runs listeners, so a query made
// inside a listener always times out. Call it from your own goroutine.
func (segment *Segment) PresenceList(ctx context.Context, page, perPage int32) (PresencePage, error) {
	return segment.channel.queryPresence(ctx, segment.id, page, perPage)
} // end method PresenceList

// Subscription is one registered interest. Cancel it when done.
type Subscription struct {
	channel   *Channel
	kind      interestKind
	segmentID string
	cancelled bool
} // end struct Subscription

// Cancel releases the interest at once. Cancelling the last message interest
// unsubscribes the segment, and cancelling the last presence interest stops
// its presence events; neither affects the other. Calling Cancel again does
// nothing.
func (subscription *Subscription) Cancel() {
	channel := subscription.channel
	channel.mutex.Lock()
	defer channel.mutex.Unlock()

	if subscription.cancelled {
		return
	}

	subscription.cancelled = true
	channel.releaseInterest(subscription.kind, subscription.segmentID)
} // end method Cancel

// listenersOf returns a segment's listener set, creating it on first use.
func listenersOf[Listener any](sets map[string]*listenerSet[Listener], segmentID string) *listenerSet[Listener] {
	listeners := sets[segmentID]

	if listeners == nil {
		listeners = &listenerSet[Listener]{}
		sets[segmentID] = listeners
	}

	return listeners
} // end function listenersOf
