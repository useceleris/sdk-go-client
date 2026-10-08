package celeris

import (
	"slices"
	"testing"
	"testing/synctest"
	"time"
)

// HEARTBEAT-01: every connection pings every 20 seconds, so the server's
// 60-second client timeout never fires, and a ping unanswered for 15 seconds
// of reading time fails the connection. The fake answers pings at once unless
// its pongs are withheld.

// assertPings checks when socket was pinged, as offsets from start.
func assertPings(t *testing.T, socket *fakeSocket, start time.Time, want ...time.Duration) {
	t.Helper()

	var got []time.Duration

	for _, pingTime := range socket.pingTimes() {
		got = append(got, pingTime.Sub(start))
	}

	if !slices.Equal(got, want) {
		t.Fatalf("pings at %v, want %v", got, want)
	}
} // end function assertPings

// sleepUntil advances the bubble's clock to offset after start.
func sleepUntil(start time.Time, offset time.Duration) {
	synctest.Sleep(time.Until(start.Add(offset)))
} // end function sleepUntil

func TestConnectedChannelPingsEveryTwentySecondsWhileIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, _, socket := connectTestChannel(t)
		connectedAt := time.Now()
		synctest.Sleep(heartbeatInterval - time.Millisecond)
		assertPings(t, socket, connectedAt)

		synctest.Sleep(time.Millisecond + 2*heartbeatInterval)
		assertPings(t, socket, connectedAt, 20*time.Second, 40*time.Second, 60*time.Second)
	})
} // end function TestConnectedChannelPingsEveryTwentySecondsWhileIdle

func TestUnansweredPingFailsAnIdleConnectionAfterFifteenSecondsOfReading(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, dead := connectTestChannel(t)
		connectedAt := time.Now()
		states := recordStates(channel)
		dead.withholdPongs()
		sleepUntil(connectedAt, heartbeatInterval+heartbeatTimeout-time.Millisecond)
		assertStates(t, states)

		sleepUntil(connectedAt, heartbeatInterval+heartbeatTimeout)
		assertStates(t, states, StateReconnecting, StateConnected)

		// The replacement answers its pings; the dead socket's heartbeat ended.
		sleepUntil(connectedAt, 2*time.Minute)

		if !dead.isClosed() || server.socketCount() != 2 || channel.State() != StateConnected {
			t.Fatalf("closed %v, %d sockets, state %s", dead.isClosed(), server.socketCount(), channel.State())
		}

		assertPings(t, dead, connectedAt, 20*time.Second)
	})
} // end function TestUnansweredPingFailsAnIdleConnectionAfterFifteenSecondsOfReading

// The outage began when the server was last heard from, here the pong at 20
// seconds, so the replay lookback covers the 35 silent seconds before the
// heartbeat noticed, plus the usual 5.
func TestAHeartbeatOutageStartsWhenTheServerWasLastHeard(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, server, socket := connectTestChannel(t)
		connectedAt := time.Now()
		sleepUntil(connectedAt, 30*time.Second)
		socket.withholdPongs()
		sleepUntil(connectedAt, 55*time.Second)

		requests := server.credentialRequests()

		if len(requests) != 2 {
			t.Fatalf("%d credential requests", len(requests))
		}

		reconnect := requests[1]

		if !reconnect.Reconnect || !reconnect.DisconnectedAt.Equal(connectedAt.Add(20*time.Second)) || reconnect.ReplayLookback != 40*time.Second {
			t.Fatalf("reconnect request %+v", reconnect)
		}
	})
} // end function TestAHeartbeatOutageStartsWhenTheServerWasLastHeard

// Pongs are read only while the receiver reads, so a listener holding it for
// 90 seconds never counts against a ping, and the pings go on meanwhile.
func TestAListenerHoldingTheReceiverNeverCountsAgainstAPing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, socket := connectTestChannel(t)
		connectedAt := time.Now()
		states := recordStates(channel)
		socket.withholdPongs()
		segment(t, channel, "chat").OnMessage(func([]byte, MessageMetadata) { time.Sleep(90 * time.Second) })
		socket.receive(messageFrame("chat", "id-1", "x"))
		sleepUntil(connectedAt, 90*time.Second)
		assertPings(t, socket, connectedAt, 20*time.Second, 40*time.Second, 60*time.Second, 80*time.Second)

		// Each ping stops waiting when the next is due.
		if channel.State() != StateConnected || len(states.all()) != 0 || server.socketCount() != 1 || socket.waitingPings() != 1 {
			t.Fatalf("state %s, states %v, %d sockets, %d pings waiting", channel.State(), states.all(), server.socketCount(), socket.waitingPings())
		}

		// Reading resumes, and the ping sent at 20 seconds has all 15 left.
		sleepUntil(connectedAt, 90*time.Second+heartbeatTimeout-time.Millisecond)
		assertStates(t, states)

		sleepUntil(connectedAt, 90*time.Second+heartbeatTimeout)
		assertStates(t, states, StateReconnecting, StateConnected)
	})
} // end function TestAListenerHoldingTheReceiverNeverCountsAgainstAPing

func TestAPongClearsTheUnansweredPing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		connectedAt := time.Now()
		states := recordStates(channel)
		answer := socket.withholdPongs()
		sleepUntil(connectedAt, 34*time.Second)
		answer()
		socket.withholdPongs()

		// The ping at 40 seconds starts the count afresh.
		sleepUntil(connectedAt, 55*time.Second-time.Millisecond)
		assertStates(t, states)

		sleepUntil(connectedAt, 55*time.Second)
		assertStates(t, states, StateReconnecting, StateConnected)
	})
} // end function TestAPongClearsTheUnansweredPing

func TestAReceivedMessageClearsTheUnansweredPing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, _, socket := connectTestChannel(t)
		connectedAt := time.Now()
		states := recordStates(channel)
		socket.withholdPongs()
		sleepUntil(connectedAt, 30*time.Second)
		socket.receive(messageFrame("chat", "id-1", "x"))
		sleepUntil(connectedAt, 50*time.Second)
		socket.receive(messageFrame("chat", "id-2", "x"))

		// Nothing answers the ping at 60 seconds.
		sleepUntil(connectedAt, 75*time.Second-time.Millisecond)
		assertStates(t, states)

		sleepUntil(connectedAt, 75*time.Second)
		assertStates(t, states, StateReconnecting, StateConnected)
	})
} // end function TestAReceivedMessageClearsTheUnansweredPing

// The bubble's exit proves every heartbeat goroutine ended; the pings prove
// each stopped with its socket.
func TestNoHeartbeatOutlivesItsSocket(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel, server, dropped := connectTestChannel(t)
		connectedAt := time.Now()
		synctest.Sleep(heartbeatInterval)
		dropped.drop()
		synctest.Wait()

		replacement := server.socket(-1)
		reconnectedAt := time.Now()
		synctest.Sleep(2 * heartbeatInterval)
		channel.Close()
		synctest.Sleep(3 * heartbeatInterval)

		assertPings(t, dropped, connectedAt, 20*time.Second)
		assertPings(t, replacement, reconnectedAt, 20*time.Second, 40*time.Second)
	})
} // end function TestNoHeartbeatOutlivesItsSocket
