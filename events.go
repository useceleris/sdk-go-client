package celeris

import (
	"slices"
	"sync/atomic"
)

// Events are handler registrations that return a function removing the
// handler (DEV-01). Delivery is synchronous and ordered, with no queue on the
// consumer's side: a slow listener holds up the next event rather than
// letting events pile up.
//
// Every event is queued under the channel's mutex at the moment of the change
// that causes it, and one goroutine at a time delivers the queue, releasing
// the mutex around each listener. An event reaches the listeners registered
// when its turn comes. Listeners therefore observe events in the order state
// changed, never run under an SDK lock, and may call back into the channel. A
// call made inside a listener never waits for its own events: they are
// delivered after the listener returns.

// ChannelEventHandler registers channel-wide listeners. Obtain one with
// [Channel.Events]. Each registration returns a function that removes exactly
// that listener; calling it again does nothing.
type ChannelEventHandler struct {
	channel *Channel
}

// OnStateChange registers a listener for every state transition.
func (handler ChannelEventHandler) OnStateChange(listener func(ChannelState)) (remove func()) {
	requireListener(listener == nil)

	return handler.channel.addListener(&handler.channel.stateListeners, listener)
}

// OnRecovery registers a listener for every successful reconnect.
func (handler ChannelEventHandler) OnRecovery(listener func(RecoveryEvent)) (remove func()) {
	requireListener(listener == nil)

	return handler.channel.addListener(&handler.channel.recoveryListeners, listener)
}

// OnNotice registers a listener for the server's raw notices.
func (handler ChannelEventHandler) OnNotice(listener func(ServerNotice)) (remove func()) {
	requireListener(listener == nil)

	return handler.channel.addListener(&handler.channel.noticeListeners, listener)
}

// OnError registers a listener for failures no caller is waiting for: a
// *[ServerError] the server sent, or an *[Error] such as an undecodable
// message, a failed reconnect, or a listener that panicked.
func (handler ChannelEventHandler) OnError(listener func(error)) (remove func()) {
	requireListener(listener == nil)

	return handler.channel.addListener(&handler.channel.errorListeners, listener)
}

// requireListener panics on a nil listener: registering one is a programming
// error, and it would otherwise fail only when an event arrives.
func requireListener(missing bool) {
	if missing {
		panic("celeris: nil listener")
	}
}

type listenerEntry[Listener any] struct {
	listener Listener

	// Read without the channel's mutex while events are delivered, so an
	// entry removed before its turn is skipped.
	removed atomic.Bool
}

type listenerSet[Listener any] struct {
	entries []*listenerEntry[Listener]
}

func (set *listenerSet[Listener]) remove(entry *listenerEntry[Listener]) {
	entry.removed.Store(true)
	set.entries = slices.DeleteFunc(set.entries, func(candidate *listenerEntry[Listener]) bool {
		return candidate == entry
	})
}

func (channel *Channel) addListener[Listener any](set *listenerSet[Listener], listener Listener) (remove func()) {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()

	return channel.addListenerLocked(set, listener)
}

func (channel *Channel) addListenerLocked[Listener any](set *listenerSet[Listener], listener Listener) (remove func()) {
	entry := &listenerEntry[Listener]{listener: listener}
	set.entries = append(set.entries, entry)

	return func() {
		channel.mutex.Lock()
		defer channel.mutex.Unlock()

		set.remove(entry)
	}
}

// queueEvent queues delivery of one event to the listeners registered when
// its turn comes, as the reference dispatches to the listeners registered at
// dispatch. The caller holds the mutex. A listener that panics is reported
// through the error listeners; an error listener that panics is not, since
// that would report into the dispatch that failed.
func (channel *Channel) queueEvent[Listener any](set *listenerSet[Listener], invoke func(Listener), reportPanics bool) {
	channel.events = append(channel.events, channel.eventFor(set, invoke, reportPanics))
}

// eventFor returns an event, which the drainer prepares under the mutex: it
// takes the listener snapshot and returns the delivery to run without it.
func (channel *Channel) eventFor[Listener any](set *listenerSet[Listener], invoke func(Listener), reportPanics bool) func() func() {
	return func() func() {
		entries := slices.Clone(set.entries)

		return func() { channel.deliverEntries(entries, invoke, reportPanics) }
	}
}

// deliverEntries calls each listener in turn. If one ends its goroutine, as
// t.FailNow does, the listeners after it are handed on as the next event.
func (channel *Channel) deliverEntries[Listener any](entries []*listenerEntry[Listener], invoke func(Listener), reportPanics bool) {
	next := 0

	defer func() {
		if next < len(entries) {
			rest := entries[next:]

			channel.mutex.Lock()
			channel.events = append([]func() func(){func() func() {
				return func() { channel.deliverEntries(rest, invoke, reportPanics) }
			}}, channel.events...)
			channel.mutex.Unlock()
		}
	}()

	for next < len(entries) {
		entry := entries[next]
		next++

		if !entry.removed.Load() {
			channel.invokeListener(func() { invoke(entry.listener) }, reportPanics)
		}
	}
}

func (channel *Channel) queueStateChange(state ChannelState) {
	channel.state = state
	channel.queueEvent(&channel.stateListeners, func(listener func(ChannelState)) { listener(state) }, true)
}

func (channel *Channel) queueError(err error) {
	channel.queueEvent(&channel.errorListeners, func(listener func(error)) { listener(err) }, false)
}

func (channel *Channel) invokeListener(call func(), reportPanics bool) {
	defer func() {
		if recover() != nil && reportPanics {
			// Reported next, ahead of events already queued, as the reference
			// reports it before dispatch moves on.
			failure := newError(ErrTransport, "A listener callback panicked; the channel recovered and kept running.")
			report := channel.eventFor(&channel.errorListeners, func(listener func(error)) { listener(failure) }, false)

			channel.mutex.Lock()
			channel.events = append([]func() func(){report}, channel.events...)
			channel.mutex.Unlock()
		}
	}()

	call()
}

// dispatchEvents delivers queued events in order until none remain. Only one
// goroutine delivers at a time; any other returns at once, and the events it
// queued are delivered by the one already delivering.
func (channel *Channel) dispatchEvents() {
	channel.mutex.Lock()

	if channel.draining {
		channel.mutex.Unlock()

		return
	}

	channel.draining = true
	locked := true

	// Deferred so that a listener ending its goroutine, as t.FailNow does,
	// cannot leave the channel marked as delivering.
	defer func() {
		if !locked {
			channel.mutex.Lock()
		}

		channel.draining = false
		channel.idle.Broadcast()
		remaining := len(channel.events) > 0
		channel.mutex.Unlock()

		if remaining {
			go channel.dispatchEvents()
		}
	}()

	for len(channel.events) > 0 {
		deliver := channel.events[0]()
		channel.events[0] = nil
		channel.events = channel.events[1:]

		locked = false
		channel.mutex.Unlock()
		deliver()
		channel.mutex.Lock()
		locked = true
	}
}

// awaitQuiescence waits until no events are queued or being delivered. The
// receive goroutine calls it, holding the mutex, before routing each message,
// so no message is processed while a listener runs: the read-ahead is one
// transport message, and there is no inbound queue (DEV-01).
func (channel *Channel) awaitQuiescence() {
	for channel.draining || len(channel.events) > 0 {
		if !channel.draining {
			channel.mutex.Unlock()
			channel.dispatchEvents()
			channel.mutex.Lock()

			continue
		}

		channel.idle.Wait()
	}
}
