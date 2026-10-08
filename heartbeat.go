package celeris

import (
	"context"
	"time"
)

// heartbeat pings the server every heartbeatInterval until the connection
// ends, so the server's client timeout never fires while a listener holds the
// receiver, and fails the connection once a ping has gone unanswered for
// heartbeatTimeout of reading time (HEARTBEAT-01). The library reads pongs
// only while the receiver reads, so time a listener holds it never counts.
func (connection *connection) heartbeat() {
	channel := connection.channel
	pings := time.NewTicker(heartbeatInterval)
	defer pings.Stop()

	// Fires when the oldest unanswered ping may have used up its reading
	// time; with none unanswered, a wake does nothing.
	timeout := time.NewTimer(heartbeatTimeout)
	defer timeout.Stop()

	for {
		select {
		case <-connection.context.Done():
			return
		case <-pings.C:
			channel.mutex.Lock()

			if !connection.pingUnanswered {
				connection.pingUnanswered = true
				connection.unansweredSince = connection.totalReadingTime()
				timeout.Reset(heartbeatTimeout)
			}

			channel.mutex.Unlock()

			go connection.ping()
		case <-timeout.C:
			channel.mutex.Lock()

			if !connection.pingUnanswered {
				channel.mutex.Unlock()

				continue
			}

			if left := heartbeatTimeout - (connection.totalReadingTime() - connection.unansweredSince); left > 0 {
				timeout.Reset(left)
				channel.mutex.Unlock()

				continue
			}

			queued := channel.heartbeatFailure(connection)
			channel.mutex.Unlock()

			if queued {
				channel.dispatchEvents()
			}

			return
		}
	}
} // end method heartbeat

// ping sends one ping and waits for its pong until the next ping is due. A
// pong clears the unanswered ping; a failed ping stays unanswered.
func (connection *connection) ping() {
	pingContext, cancel := context.WithTimeout(connection.context, heartbeatInterval)
	defer cancel()

	if connection.socket.ping(pingContext) != nil {
		return
	}

	connection.channel.mutex.Lock()
	connection.heard()
	connection.channel.mutex.Unlock()
} // end method ping

// heard records a pong or a received message, which clears the unanswered
// ping. The caller holds the mutex.
func (connection *connection) heard() {
	connection.lastHeard = time.Now()
	connection.pingUnanswered = false
} // end method heard

// startReading and stopReading bracket every read. The caller holds the mutex.
func (connection *connection) startReading() {
	connection.reading = true
	connection.readingSince = time.Now()
} // end method startReading

func (connection *connection) stopReading(received bool) {
	connection.readingTime += time.Since(connection.readingSince)
	connection.reading = false

	if received {
		connection.heard()
	}
} // end method stopReading

// totalReadingTime includes the read in progress. The caller holds the mutex.
func (connection *connection) totalReadingTime() time.Duration {
	if connection.reading {
		return connection.readingTime + time.Since(connection.readingSince)
	}

	return connection.readingTime
} // end method totalReadingTime
