package celeris

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"
)

// ChannelState is where a channel is in its lifecycle.
type ChannelState string

const (
	// StateIdle means created and never connected.
	StateIdle ChannelState = "idle"

	// StateConnecting means the first connection attempt is running.
	StateConnecting ChannelState = "connecting"

	// StateConnected means a WebSocket is established. Nothing more is
	// implied.
	StateConnected ChannelState = "connected"

	// StateReconnecting means the connection dropped and is being recovered.
	StateReconnecting ChannelState = "reconnecting"

	// StateFailed means connecting or recovery failed. Connect may be called
	// again.
	StateFailed ChannelState = "failed"

	// StateClosing means Close is shutting the connection down.
	StateClosing ChannelState = "closing"

	// StateClosed is terminal. Create a new channel to connect again.
	StateClosed ChannelState = "closed"
)

var (
	errAttemptTimedOut = errors.New("connection attempt timed out")
	errChannelClosed   = errors.New("channel closed")
)

// Channel is one WebSocket connection. Every segment of the channel is
// multiplexed over it; another Channel, even for the same reference, is
// another connection (SEG-01). Obtain one with [Client.Channel].
//
// A Channel is safe for concurrent use. Close it when done.
type Channel struct {
	client    *Client
	reference string

	mutex sync.Mutex

	state      ChannelState
	generation uint64
	connection *connection

	retriesUsed int
	retryTimer  *time.Timer
	connectedAt time.Time

	// When the connection dropped, or zero when there is no outage. Its
	// monotonic reading measures the outage.
	disconnectedAt time.Time

	// Cancels the attempt in flight; Close cancels it with errChannelClosed.
	attemptCancel context.CancelCauseFunc

	closeStarted bool
	closed       chan struct{}

	queue         commandQueue
	deduplication deduplicationWindow

	messageInterests  orderedMap[int]
	presenceInterests orderedMap[int]

	// Issues presence query request ids. Kept per channel rather than per
	// socket, so an id is never reused across reconnects (QUERY-01).
	presenceRequestCount uint64
	pendingPresence      *presenceQuery

	messageListeners        map[string]*listenerSet[func([]byte, MessageMetadata)]
	presenceListeners       map[string]*listenerSet[func(PresenceEvent)]
	channelMessageListeners listenerSet[func([]byte, MessageMetadata)]
	stateListeners          listenerSet[func(ChannelState)]
	recoveryListeners       listenerSet[func(RecoveryEvent)]
	noticeListeners         listenerSet[func(ServerNotice)]
	errorListeners          listenerSet[func(error)]

	events   []func() func()
	draining bool
	idle     *sync.Cond
} // end struct Channel

type presenceQuery struct {
	requestID string
	result    chan presenceResult
	timer     *time.Timer
} // end struct presenceQuery

type presenceResult struct {
	page PresencePage
	err  error
} // end struct presenceResult

func newChannel(client *Client, reference string) *Channel {
	channel := &Channel{
		client:            client,
		reference:         reference,
		state:             StateIdle,
		closed:            make(chan struct{}),
		messageListeners:  map[string]*listenerSet[func([]byte, MessageMetadata)]{},
		presenceListeners: map[string]*listenerSet[func(PresenceEvent)]{},
	}

	channel.queue.channel = channel
	channel.idle = sync.NewCond(&channel.mutex)

	return channel
} // end function newChannel

// State returns the current state. It may change as soon as it is read.
func (channel *Channel) State() ChannelState {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()

	return channel.state
} // end method State

// Events returns the handler for channel-wide listeners.
func (channel *Channel) Events() ChannelEventHandler {
	return ChannelEventHandler{channel: channel}
} // end method Events

// Segment returns a handle for the segment with the given id. It does no
// network work, and any number of handles for one segment share its
// subscriptions and listeners. It fails with [ErrConfiguration] for an id that
// is empty, contains CR or LF, or is not valid UTF-8.
func (channel *Channel) Segment(segmentID string) (*Segment, error) {
	if rule := identifierRule(segmentID); rule != "" {
		return nil, configurationError("segment ID", failure("", rule))
	}

	return &Segment{channel: channel, id: segmentID}, nil
} // end method Segment

// DefaultSegment returns a handle for the segment every connection joins
// automatically (SEG-01).
func (channel *Channel) DefaultSegment() *Segment {
	return &Segment{channel: channel, id: defaultSegmentID}
} // end method DefaultSegment

// Connect opens the connection. It returns once the WebSocket is established
// and held subscriptions are queued to be restored ahead of any publish, or
// fails without retrying: only a connection that was up recovers
// automatically.
//
// It fails with [ErrOperationInProgress] while connecting, connected or
// reconnecting, and with [ErrNotConnected] once the channel is closing or
// closed. A failed attempt leaves the channel [StateFailed], from which
// Connect may be called again. Cancelling ctx abandons the attempt.
func (channel *Channel) Connect(ctx context.Context) error {
	channel.mutex.Lock()

	switch channel.state {
	case StateConnecting, StateConnected, StateReconnecting:
		state := channel.state
		channel.mutex.Unlock()

		return newError(ErrOperationInProgress, "Connect was already called; the channel is "+string(state)+".")
	case StateClosing, StateClosed:
		channel.mutex.Unlock()

		return newError(ErrNotConnected, "Channel is closed; create a new one with Client.Channel.")
	}

	channel.generation++
	generation := channel.generation
	channel.retriesUsed = 0
	channel.disconnectedAt = time.Time{}
	channel.deduplication.clear()
	channel.queueStateChange(StateConnecting)
	channel.mutex.Unlock()
	channel.dispatchEvents()

	err := channel.establishConnection(ctx, generation, nil, 0)

	if err != nil {
		channel.mutex.Lock()

		if generation == channel.generation {
			channel.generation++
			channel.queueStateChange(StateFailed)
		}

		channel.mutex.Unlock()
	}

	channel.dispatchEvents()

	return err
} // end method Connect

// Close closes the connection and every segment handle with it. It is
// terminal and idempotent, and returns within about five seconds even when
// the server does not answer the closing handshake. Publishes and presence
// queries still waiting fail with [ErrCancelled].
//
// Listeners may still receive events queued before Close returned, including
// the closing and closed state changes.
func (channel *Channel) Close() {
	channel.mutex.Lock()

	if channel.closeStarted {
		channel.mutex.Unlock()
		<-channel.closed

		return
	}

	channel.closeStarted = true
	channel.rejectPendingPresence(newError(ErrCancelled, "Channel closed while the presence query was pending."))
	channel.generation++
	stopTimer(&channel.retryTimer)

	if channel.attemptCancel != nil {
		channel.attemptCancel(errChannelClosed)
		channel.attemptCancel = nil
	}

	cancelled := errClosedBeforeSent()
	channel.queue.reset(cancelled)

	connection := channel.connection
	channel.connection = nil

	if connection != nil {
		connection.closing = true
		connection.ready.Broadcast()
	}

	channel.queueStateChange(StateClosing)
	channel.mutex.Unlock()

	// Events are delivered only once the channel is closed, so a listener that
	// calls Close again returns at once instead of waiting on this call.
	if connection != nil {
		budget := time.NewTimer(closeBudget)

		select {
		case <-connection.writerDone:
		case <-budget.C:
			channel.mutex.Lock()
			connection.failUnwritten(cancelled)
			connection.abandon()
			channel.mutex.Unlock()
		}

		budget.Stop()
	}

	channel.mutex.Lock()
	channel.queueStateChange(StateClosed)
	close(channel.closed)
	channel.mutex.Unlock()
	channel.dispatchEvents()
} // end method Close

// establishConnection runs one attempt: fresh credentials, the handshake,
// then installing the socket. On success the channel is connected and, for a
// reconnect, the recovery event is queued, all before any received message is
// routed.
func (channel *Channel) establishConnection(parent context.Context, generation uint64, reconnect *CredentialRequest, retryIndex int) error {
	timeout := channel.client.connectTimeout

	if reconnect != nil {
		timeout = channel.client.reconnectTimeout
	}

	// Deriving from the caller's context runs its code, so it happens outside
	// the mutex.
	attemptContext, cancelAttempt := context.WithCancelCause(parent)
	attemptContext, cancelTimeout := context.WithTimeoutCause(attemptContext, timeout, errAttemptTimedOut)

	defer cancelTimeout()
	defer cancelAttempt(nil)

	channel.mutex.Lock()

	if generation != channel.generation {
		channel.mutex.Unlock()

		return newError(ErrCancelled, "Connection attempt cancelled: the channel was closed or a newer attempt started.")
	}

	channel.attemptCancel = cancelAttempt
	channel.mutex.Unlock()

	request := CredentialRequest{ChannelReference: channel.reference}

	if reconnect != nil {
		request = *reconnect
	}

	socket, err := channel.openSocket(attemptContext, request, timeout)

	channel.mutex.Lock()
	defer channel.mutex.Unlock()

	channel.attemptCancel = nil

	if err != nil {
		return err
	}

	if generation != channel.generation {
		go func() { _ = socket.closeNow() }()

		return newError(ErrCancelled, "Connection attempt cancelled: the channel was closed or a newer attempt started.")
	}

	connection := newConnection(channel, socket)
	channel.connection = connection
	channel.connectedAt = time.Now()
	channel.disconnectedAt = time.Time{}

	// Restoration goes through the queue, so it waits for writer room and
	// reaches the server before the publishes that waited for this socket.
	channel.queue.restore()
	channel.queueStateChange(StateConnected)

	if reconnect != nil {
		recovery := RecoveryEvent{RetryIndex: retryIndex, PossibleGaps: true, PossibleDuplicates: true}
		channel.queueEvent(&channel.recoveryListeners, func(listener func(RecoveryEvent)) { listener(recovery) }, true)
	}

	go connection.writeLoop()
	go channel.receive(connection)
	go connection.heartbeat()

	return nil
} // end method establishConnection

// openSocket acquires credentials and performs the handshake. Every failure
// maps to fixed text: a provider's error and the dialer's error, which quotes
// the credential URL, are never passed on.
func (channel *Channel) openSocket(ctx context.Context, request CredentialRequest, timeout time.Duration) (socket, error) {
	type providerResult struct {
		credentials Credentials
		err         error
	}

	// A context already done never reaches the provider.
	if ctx.Err() != nil {
		return nil, attemptError(ctx, timeout)
	}

	results := make(chan providerResult, 1)

	go func() {
		defer func() {
			if recover() != nil {
				results <- providerResult{err: errProviderPanicked}
			}
		}()

		credentials, err := channel.client.credentialProvider(ctx, request)
		results <- providerResult{credentials: credentials, err: err}
	}()

	var result providerResult

	// A provider that ignores ctx still cannot hold the attempt past its
	// deadline, and what it returns afterwards is discarded.
	select {
	case <-ctx.Done():
		return nil, attemptError(ctx, timeout)
	case result = <-results:
	}

	if ctx.Err() != nil {
		return nil, attemptError(ctx, timeout)
	}

	if result.err != nil {
		return nil, newError(ErrTransport, "Credential acquisition failed: the credential provider returned an error or panicked.")
	}

	if err := validateCredentials(result.credentials); err != nil {
		return nil, err
	}

	connectionSocket, err := channel.client.dial(ctx, credentialURL(channel.client.baseURL, channel.reference, result.credentials))

	if err != nil {
		if ctx.Err() != nil {
			return nil, attemptError(ctx, timeout)
		}

		return nil, newError(ErrTransport, "WebSocket handshake failed: the server refused the connection or could not be reached. Check the base URL, the credentials and the channel reference.")
	}

	return connectionSocket, nil
} // end method openSocket

var errProviderPanicked = errors.New("credential provider panicked")

func attemptError(ctx context.Context, timeout time.Duration) error {
	cause := context.Cause(ctx)

	switch {
	case errors.Is(cause, errAttemptTimedOut):
		return newError(ErrTimeout, "Connection attempt timed out after "+timeout.String()+".")
	case errors.Is(cause, errChannelClosed):
		return newError(ErrCancelled, "Connection attempt cancelled: the channel was closed or a newer attempt started.")
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return newError(ErrTimeout, "Connection attempt ran past its context's deadline.")
	default:
		return newError(ErrCancelled, "Connection attempt cancelled by its context.")
	}
} // end function attemptError

// receive routes what arrives on one socket until it fails or is detached.
func (channel *Channel) receive(connection *connection) {
	finished := false

	// A listener that ends this goroutine, as t.FailNow does, leaves nothing
	// reading the socket, so the socket is replaced. A panic is raised again
	// first: one inside routing may hold the mutex, and taking it here would
	// hang the channel instead of crashing.
	defer func() {
		if failure := recover(); failure != nil {
			panic(failure)
		}

		if !finished {
			channel.mutex.Lock()
			queued := channel.receiveSocketFailure(connection)
			channel.mutex.Unlock()

			if queued {
				go channel.dispatchEvents()
			}
		}
	}()

	for {
		channel.mutex.Lock()
		connection.startReading()
		channel.mutex.Unlock()

		binary, data, err := connection.socket.read(connection.context)

		channel.mutex.Lock()
		connection.stopReading(err == nil)

		if err != nil {
			queued := channel.receiveSocketFailure(connection)
			channel.mutex.Unlock()
			finished = true

			// A socket closed by Close queues nothing here; its events are
			// Close's to deliver.
			if queued {
				channel.dispatchEvents()
			}

			return
		}

		channel.mutex.Unlock()

		// A message that cannot be decoded costs exactly that message:
		// decoding never spans messages, so the next one is unaffected and the
		// connection stays up (DECODE-01).
		var message serverMessage

		if !binary {
			err = protocolError("Expected a binary WebSocket message.", "Message", 0)
		} else {
			message, err = decodeServerMessage(data)
		}

		if err != nil {
			message = decodeFailure{err: err}
		}

		if !channel.route(connection, message) {
			finished = true

			return
		}
	}
} // end method receive

// decodeFailure carries a decoding failure through routing, so it is reported
// in order with everything else.
type decodeFailure struct {
	err error
} // end struct decodeFailure

func (decodeFailure) isServerMessage() {} // end method isServerMessage

// route delivers one decoded message, entry by entry for a batch, waiting
// before each until no listener is running. It reports false once the
// connection is no longer the channel's.
func (channel *Channel) route(connection *connection, message serverMessage) bool {
	switch message := message.(type) {
	case arrayMessage:
		for _, entry := range message.messages {
			if !channel.route(connection, entry) {
				return false
			}
		}

		return true
	case ignoredMessage:
		return true
	}

	channel.mutex.Lock()
	channel.awaitQuiescence()

	if channel.connection != connection {
		channel.mutex.Unlock()

		return false
	}

	channel.routeEntry(message)
	channel.mutex.Unlock()
	channel.dispatchEvents()

	return true
} // end method route

func (channel *Channel) routeEntry(message serverMessage) {
	switch message := message.(type) {
	case deliveryMessage:
		channel.deliver(message)
	case noticeMessage:
		notice := ServerNotice{Timestamp: message.timestamp, Payload: message.payload}
		channel.queueEvent(&channel.noticeListeners, func(listener func(ServerNotice)) { listener(notice) }, true)
	case presenceNotifyMessage:
		channel.deliverPresence(message)
	case presenceListMessage:
		channel.receivePresenceResponse(message)
	case errorMessage:
		channel.receiveServerError(message)
	case decodeFailure:
		channel.queueError(message.err)
	}
} // end method routeEntry

func (channel *Channel) deliver(message deliveryMessage) {
	// Every delivery carries an id, the publisher's or one the server assigns
	// (REV-01). One without leaves the message undeliverable, since it cannot
	// be deduplicated, so it is dropped and reported without taking the
	// connection down (DECODE-01).
	if message.messageID == "" {
		channel.queueError(protocolError("Server message is missing its identifier.", "MessageID", 0))

		return
	}

	// Ids are recorded before fanout, even with no listeners.
	if !channel.deduplication.recordIfNew(message.messageID, channel.client.deduplicationWindowSize) {
		return
	}

	metadata := MessageMetadata{
		TokenReference: message.tokenReference,
		SegmentID:      message.segmentID,
		MessageID:      message.messageID,
		Timestamp:      message.timestamp,
	}

	invoke := func(listener func([]byte, MessageMetadata)) { listener(message.payload, metadata) }

	// The segment's listeners first, then the channel's (MSG-02).
	if listeners := channel.messageListeners[message.segmentID]; listeners != nil {
		channel.queueEvent(listeners, invoke, true)
	}

	// Queued even with no channel listeners: a listener set is snapshotted
	// when its event's turn comes, so a channel listener a segment listener
	// registers mid-delivery still receives this message, as in the reference.
	channel.queueEvent(&channel.channelMessageListeners, invoke, true)
} // end method deliver

func (channel *Channel) deliverPresence(message presenceNotifyMessage) {
	listeners := channel.presenceListeners[message.segmentID]

	if listeners == nil {
		return
	}

	event := PresenceEvent{
		SegmentID:      message.segmentID,
		TokenReference: message.tokenReference,
		ConnectionID:   message.connectionID,
		Joined:         message.joined,
		Timestamp:      message.timestamp,
	}

	channel.queueEvent(listeners, func(listener func(PresenceEvent)) { listener(event) }, true)
} // end method deliverPresence

// receiveServerError reports an error frame once, every field as sent, and
// leaves the connection up (ERR-01).
func (channel *Channel) receiveServerError(message errorMessage) {
	serverError := &ServerError{
		Type:     ServerErrorType(message.errorType),
		SubType:  message.subType,
		Message:  string(message.message),
		Resource: message.resource,
	}

	// A presence query error names its query by request id and answers that
	// query alone. A stale id belongs to a query that was already rejected
	// and reported, so it is dropped (QUERY-01).
	if message.subType == presenceListCommand {
		if pending := channel.pendingPresence; pending != nil && message.resource == pending.requestID {
			channel.rejectPendingPresence(serverError)
		}

		return
	}

	if serverError.Type == RateLimitError {
		channel.queue.receiveRateLimit()
	}

	channel.queueError(serverError)
} // end method receiveServerError

func (channel *Channel) receivePresenceResponse(message presenceListMessage) {
	pending := channel.pendingPresence

	// A response carrying any other request id answers a query that already
	// failed, so it is dropped; a pending query keeps waiting for its own.
	if pending == nil || message.requestID != pending.requestID {
		return
	}

	channel.takePendingPresence()
	pending.result <- presenceResult{page: PresencePage{
		SegmentID:   message.segmentID,
		Total:       message.total,
		PerPage:     message.perPage,
		CurrentPage: message.currentPage,
		From:        message.from,
		To:          message.to,
		Connections: message.connections,
	}}
} // end method receivePresenceResponse

func (channel *Channel) takePendingPresence() *presenceQuery {
	pending := channel.pendingPresence

	if pending != nil {
		channel.pendingPresence = nil
		pending.timer.Stop()
	}

	return pending
} // end method takePendingPresence

func (channel *Channel) rejectPendingPresence(err error) {
	if pending := channel.takePendingPresence(); pending != nil {
		pending.result <- presenceResult{err: err}
	}
} // end method rejectPendingPresence

func errClosedBeforeSent() error {
	return newError(ErrCancelled, "Channel closed before the publish was sent.")
} // end function errClosedBeforeSent

// receiveSocketFailure starts recovery when the channel's own socket closed
// or failed, and reports whether that queued events. The caller holds the
// mutex.
func (channel *Channel) receiveSocketFailure(connection *connection) bool {
	if channel.connection != connection {
		return false
	}

	channel.enterReconnecting(time.Now())

	return true
} // end method receiveSocketFailure

// heartbeatFailure starts recovery when the heartbeat found the channel's own
// socket dead, and reports whether that queued events. The outage began when
// the server was last heard from, so the replay lookback covers what was
// published during the silence. The caller holds the mutex.
func (channel *Channel) heartbeatFailure(connection *connection) bool {
	if channel.connection != connection {
		return false
	}

	channel.enterReconnecting(connection.lastHeard)

	return true
} // end method heartbeatFailure

// enterReconnecting starts recovery from an outage that began at outageStart.
func (channel *Channel) enterReconnecting(outageStart time.Time) {
	channel.rejectPendingPresence(newError(ErrTransport, "Connection lost during the presence query; query again once the channel reconnects."))
	channel.queue.dropConnection()

	now := time.Now()

	if now.Sub(channel.connectedAt) >= retryBudgetReset {
		channel.retriesUsed = 0
	}

	// The monotonic reading is the outage's start; the wall clock moves back
	// by the same span.
	channel.disconnectedAt = now.Add(-now.Sub(outageStart))
	channel.queue.putBack(channel.connection.takeUnwritten())
	channel.connection.abandon()
	channel.connection = nil
	channel.queueStateChange(StateReconnecting)
	channel.scheduleRetry()
} // end method enterReconnecting

func (channel *Channel) scheduleRetry() {
	generation := channel.generation
	delay := retryDelay(channel.retriesUsed, channel.client.random)

	var timer *time.Timer

	timer = time.AfterFunc(delay, func() {
		channel.mutex.Lock()

		if channel.retryTimer != timer || generation != channel.generation {
			channel.mutex.Unlock()

			return
		}

		channel.retryTimer = nil
		channel.mutex.Unlock()
		channel.runReconnectAttempt(generation)
	})

	channel.retryTimer = timer
} // end method scheduleRetry

func (channel *Channel) runReconnectAttempt(generation uint64) {
	channel.mutex.Lock()

	if generation != channel.generation || channel.disconnectedAt.IsZero() {
		channel.mutex.Unlock()

		return
	}

	retryIndex := channel.retriesUsed
	request := CredentialRequest{
		ChannelReference: channel.reference,
		Reconnect:        true,
		DisconnectedAt:   channel.disconnectedAt.Round(0),
		ReplayLookback:   replayLookback(time.Since(channel.disconnectedAt)),
	}

	channel.mutex.Unlock()

	err := channel.establishConnection(context.Background(), generation, &request, retryIndex)

	if err != nil {
		channel.mutex.Lock()

		if generation == channel.generation {
			if errors.Is(err, ErrTransport) || errors.Is(err, ErrTimeout) {
				channel.retriesUsed++

				if channel.retriesUsed >= channel.client.maximumReconnectAttempts {
					channel.failTerminal(err)
				} else {
					channel.scheduleRetry()
				}
			} else {
				channel.failTerminal(err)
			}
		}

		channel.mutex.Unlock()
	}

	channel.dispatchEvents()
} // end method runReconnectAttempt

func (channel *Channel) failTerminal(err error) {
	channel.rejectPendingPresence(newError(ErrTransport, "Connection lost during the presence query; query again once the channel reconnects."))
	channel.queue.reset(err)
	channel.generation++
	stopTimer(&channel.retryTimer)
	channel.queueError(err)
	channel.queueStateChange(StateFailed)
} // end method failTerminal

// addInterest counts one interest; the first registration and the last
// cancellation queue a sync of the segment's subscription.
func (channel *Channel) addInterest(kind interestKind, segmentID string) (*Subscription, error) {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()

	if channel.state == StateClosing || channel.state == StateClosed {
		return nil, newError(ErrNotConnected, "Channel is closed; create a new one with Client.Channel.")
	}

	interests := channel.interests(kind)
	count, _ := interests.get(segmentID)
	interests.set(segmentID, count+1)

	if count == 0 {
		channel.queueInterestSync(kind, segmentID)
	}

	return &Subscription{channel: channel, kind: kind, segmentID: segmentID}, nil
} // end method addInterest

func (channel *Channel) releaseInterest(kind interestKind, segmentID string) {
	interests := channel.interests(kind)
	count, _ := interests.get(segmentID)

	if count > 1 {
		interests.set(segmentID, count-1)

		return
	}

	interests.remove(segmentID)
	channel.queueInterestSync(kind, segmentID)
} // end method releaseInterest

func (channel *Channel) interests(kind interestKind) *orderedMap[int] {
	if kind == messageInterest {
		return &channel.messageInterests
	}

	return &channel.presenceInterests
} // end method interests

// queueInterestSync does nothing before the first socket: installing one
// syncs every held interest. While reconnecting the change waits in the
// queue, behind the publishes queued before it.
func (channel *Channel) queueInterestSync(kind interestKind, segmentID string) {
	if channel.connection != nil || channel.state == StateReconnecting {
		channel.queue.queueInterest(kind, segmentID)
	}
} // end method queueInterestSync

// interestCommand names the command that brings the server in line with the
// segment's interest as it stands now, or "" when none is needed.
// Subscriptions are synced as state, so a resend is always safe.
func (channel *Channel) interestCommand(kind interestKind, segmentID string) string {
	// Presence applies to every segment, the default one included: the
	// server's connect-time auto-join grants message membership only.
	if kind == presenceInterest {
		if _, held := channel.presenceInterests.get(segmentID); held {
			return presenceSubscribeCommand
		}

		return presenceUnsubscribeCommand
	}

	// The server joins the default segment on connect and never leaves it.
	if segmentID == defaultSegmentID {
		return ""
	}

	// Watching presence is not membership, so it never holds the segment.
	if _, held := channel.messageInterests.get(segmentID); held {
		return subscribeCommand
	}

	return unsubscribeCommand
} // end method interestCommand

func (channel *Channel) publish(ctx context.Context, segmentID string, payload []byte, messageID string) error {
	// The context is the caller's code, and encoding copies up to 2 MiB: both
	// happen outside the mutex. Their failures are still reported in the
	// reference's order, after the connection check.
	contextDone := ctx.Err() != nil

	var data []byte
	var encodeErr error

	if messageID != "" {
		data, encodeErr = encodePublish(segmentID, messageID, payload)
	}

	channel.mutex.Lock()

	// While reconnecting the publish waits in the queue for the next socket
	// (QUEUE-01).
	queueable := channel.state == StateConnected && channel.connection != nil || channel.state == StateReconnecting

	if !queueable {
		state := channel.state
		channel.mutex.Unlock()

		return newError(ErrNotConnected, "Channel is not connected; it is "+string(state)+".")
	}

	if contextDone {
		channel.mutex.Unlock()

		return contextError(ctx, "Publish cancelled: its context was already done.", "Publish not sent: its context's deadline had already passed.")
	}

	// An empty id would encode as null; every publish carries one (RESEND-01).
	if messageID == "" {
		channel.mutex.Unlock()

		return configurationError("command", failure("MessageID", "Must not be empty"))
	}

	if encodeErr != nil {
		channel.mutex.Unlock()

		return encodeErr
	}

	publish, err := channel.queue.publish(segmentID, data)
	channel.mutex.Unlock()

	if err != nil {
		return err
	}

	select {
	case err := <-publish.result:
		return err
	case <-ctx.Done():
	}

	cancelled := contextError(ctx, "Publish cancelled by its context before it was sent.", "Publish ran past its context's deadline before it was sent.")

	channel.mutex.Lock()
	channel.cancelPublish(publish, cancelled)
	channel.mutex.Unlock()

	return <-publish.result
} // end method publish

// cancelPublish settles a publish whose context ended. A rate limit can
// requeue a publish still in the writer, so it may have a copy queued and
// copies in the writer at once: every copy not yet written is taken back, and
// the publish is cancelled unless the socket is writing one, which makes it
// DeliveryUnknown. Either way no later rate limit resends it. The caller
// holds the mutex.
func (channel *Channel) cancelPublish(publish *queuedPublish, cancelled error) {
	if publish.settled {
		return
	}

	connection := publish.connection
	withdrawn := channel.queue.withdraw(publish)
	removed := connection != nil && connection.remove(publish)
	writing := connection != nil && connection.writing != nil && connection.writing.publish == publish

	switch {
	case writing:
		publish.settle(newError(ErrDeliveryUnknown, "Publish's context ended while the socket was writing it, so it may or may not have been sent."))
	case withdrawn || removed:
		publish.settle(cancelled)
	default:
		return
	}

	channel.queue.forget(publish)

	// The room taken back from the writer may let a waiting command through.
	if removed && connection == channel.connection {
		channel.queue.drain()
	}
} // end method cancelPublish

func (channel *Channel) queryPresence(ctx context.Context, segmentID string, page, perPage int32) (PresencePage, error) {
	contextDone := ctx.Err() != nil

	channel.mutex.Lock()

	connection := channel.connection

	if channel.state != StateConnected || connection == nil {
		state := channel.state
		channel.mutex.Unlock()

		return PresencePage{}, newError(ErrNotConnected, "Channel is not connected; it is "+string(state)+".")
	}

	if channel.pendingPresence != nil {
		channel.mutex.Unlock()

		return PresencePage{}, newError(ErrOperationInProgress, "A presence query is already in flight; wait for it to settle before starting another.")
	}

	if contextDone {
		channel.mutex.Unlock()

		return PresencePage{}, contextError(ctx, "Presence query cancelled: its context was already done.", "Presence query not sent: its context's deadline had already passed.")
	}

	channel.presenceRequestCount++
	requestID := strconv.FormatUint(channel.presenceRequestCount, 10)
	data, err := encodePresenceList(segmentID, page, perPage, requestID)

	if err == nil {
		// A query the writer refuses fails without taking the query slot.
		err = channel.queue.sendNow(connection, data)
	}

	if err != nil {
		channel.mutex.Unlock()

		return PresencePage{}, err
	}

	// A timed-out or cancelled query frees its slot and leaves the connection
	// alone: a late reply carries the old request id and is dropped.
	pending := &presenceQuery{requestID: requestID, result: make(chan presenceResult, 1)}
	pending.timer = time.AfterFunc(channel.client.presenceQueryTimeout, func() {
		channel.mutex.Lock()
		defer channel.mutex.Unlock()

		if channel.pendingPresence == pending {
			channel.rejectPendingPresence(newError(ErrTimeout, "Presence query timed out after "+channel.client.presenceQueryTimeout.String()+"."))
		}
	})

	channel.pendingPresence = pending
	channel.mutex.Unlock()

	select {
	case result := <-pending.result:
		return result.page, result.err
	case <-ctx.Done():
	}

	cancelled := contextError(ctx, "Presence query cancelled by its context.", "Presence query ran past its context's deadline.")

	channel.mutex.Lock()

	if channel.pendingPresence == pending {
		channel.rejectPendingPresence(cancelled)
	}

	channel.mutex.Unlock()

	result := <-pending.result

	return result.page, result.err
} // end method queryPresence

// contextError reports an operation whose context ended before it completed:
// a deadline as a timeout, anything else as a cancellation.
func contextError(ctx context.Context, cancelled, timedOut string) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return newError(ErrTimeout, timedOut)
	}

	return newError(ErrCancelled, cancelled)
} // end function contextError

// deduplicationWindow remembers the most recently delivered message ids,
// DeduplicationWindowSize of them. It survives reconnects, absorbing replay
// duplicates, and clears on Connect (REV-01).
type deduplicationWindow struct {
	identifiers map[string]struct{}
	order       []string
} // end struct deduplicationWindow

// recordIfNew returns false for an id already in the window. A new one is
// recorded, evicting the oldest once the window holds limit ids.
func (window *deduplicationWindow) recordIfNew(identifier string, limit int) bool {
	if _, seen := window.identifiers[identifier]; seen {
		return false
	}

	if window.identifiers == nil {
		window.identifiers = map[string]struct{}{}
	}

	window.identifiers[identifier] = struct{}{}
	window.order = append(window.order, identifier)

	if len(window.order) > limit {
		delete(window.identifiers, window.order[0])
		window.order = window.order[1:]
	}

	return true
} // end method recordIfNew

func (window *deduplicationWindow) clear() {
	window.identifiers = nil
	window.order = nil
} // end method clear
