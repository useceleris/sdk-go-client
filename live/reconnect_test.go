package live

import (
	"context"
	"io"
	"net"
	"net/url"
	"slices"
	"sync"
	"testing"
	"time"

	celeris "github.com/useceleris/sdk-go-client"
)

// droppingProxy forwards TCP connections to the realtime service. A test can
// break it as a network would:
//   - setRefusing: new connections close at once
//   - dropAll: every open connection closes
//   - blackholeAll: the open connections silently stop carrying anything
//   - setBlackhole: new connections stay open but carry nothing
//   - stallUpstream: the proxy stops reading what clients send, so their
//     socket buffers fill
type droppingProxy struct {
	listener net.Listener
	target   string

	mutex      sync.Mutex
	unstalled  *sync.Cond
	refusing   bool
	blackhole  bool
	stalled    bool
	links      []net.Conn
	blackholed map[net.Conn]bool
} // end struct droppingProxy

func newDroppingProxy(t *testing.T) *droppingProxy {
	t.Helper()

	target, err := url.Parse(websocketURL())

	if err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")

	if err != nil {
		t.Fatal(err)
	}

	proxy := &droppingProxy{listener: listener, target: target.Host}
	proxy.unstalled = sync.NewCond(&proxy.mutex)
	t.Cleanup(func() {
		_ = listener.Close()
		proxy.dropAll()
		proxy.stallUpstream(false)
	})

	go proxy.accept()

	return proxy
} // end function newDroppingProxy

func (proxy *droppingProxy) url() string {
	return "ws://" + proxy.listener.Addr().String()
} // end method url

func (proxy *droppingProxy) accept() {
	for {
		client, err := proxy.listener.Accept()

		if err != nil {
			return
		}

		go proxy.link(client)
	}
} // end method accept

func (proxy *droppingProxy) link(client net.Conn) {
	proxy.mutex.Lock()
	refusing, blackhole := proxy.refusing, proxy.blackhole

	if blackhole && !refusing {
		proxy.links = append(proxy.links, client)
	}

	proxy.mutex.Unlock()

	if refusing {
		_ = client.Close()

		return
	}

	if blackhole {
		_, _ = io.Copy(io.Discard, client)
		_ = client.Close()

		return
	}

	upstream, err := net.Dial("tcp", proxy.target)

	if err != nil {
		_ = client.Close()

		return
	}

	proxy.mutex.Lock()
	proxy.links = append(proxy.links, client, upstream)
	proxy.mutex.Unlock()

	go func() {
		proxy.forward(upstream, client, true)
		_ = upstream.Close()
	}()

	proxy.forward(client, upstream, false)
	_ = client.Close()
} // end method link

// forward copies source to destination until either fails. Once its link is
// blackholed, it reads and discards, so the far side hears nothing and neither
// side's socket closes. While upstream is stalled, it reads nothing from a
// client.
func (proxy *droppingProxy) forward(destination, source net.Conn, fromClient bool) {
	buffer := make([]byte, 32*1024)

	for {
		proxy.mutex.Lock()

		for fromClient && proxy.stalled {
			proxy.unstalled.Wait()
		}

		proxy.mutex.Unlock()
		count, err := source.Read(buffer)

		proxy.mutex.Lock()
		blackholed := proxy.blackholed[source]
		proxy.mutex.Unlock()

		if count > 0 && !blackholed {
			if _, writeErr := destination.Write(buffer[:count]); writeErr != nil {
				return
			}
		}

		if err != nil {
			return
		}
	}
} // end method forward

// blackholeAll silently stops carrying every current connection; new ones are
// carried as usual.
func (proxy *droppingProxy) blackholeAll() {
	proxy.mutex.Lock()
	defer proxy.mutex.Unlock()

	proxy.blackholed = map[net.Conn]bool{}

	for _, link := range proxy.links {
		proxy.blackholed[link] = true
	}
} // end method blackholeAll

func (proxy *droppingProxy) setRefusing(refusing bool) {
	proxy.mutex.Lock()
	proxy.refusing = refusing
	proxy.mutex.Unlock()
} // end method setRefusing

func (proxy *droppingProxy) setBlackhole(blackhole bool) {
	proxy.mutex.Lock()
	proxy.blackhole = blackhole
	proxy.mutex.Unlock()
} // end method setBlackhole

func (proxy *droppingProxy) stallUpstream(stalled bool) {
	proxy.mutex.Lock()
	proxy.stalled = stalled
	proxy.unstalled.Broadcast()
	proxy.mutex.Unlock()
} // end method stallUpstream

func (proxy *droppingProxy) dropAll() {
	proxy.mutex.Lock()
	links := proxy.links
	proxy.links = nil
	proxy.mutex.Unlock()

	for _, link := range links {
		_ = link.Close()
	}
} // end method dropAll

// reconnectSetup is a receiver behind the dropping proxy and a publisher that
// connects directly, so only the receiver has the outage.
type reconnectSetup struct {
	proxy     *droppingProxy
	receiver  *celeris.Channel
	publisher *celeris.Channel
	recovered chan struct{}

	mutex      sync.Mutex
	requests   []celeris.CredentialRequest
	recoveries []celeris.RecoveryEvent
} // end struct reconnectSetup

// newReconnectSetup leaves the receiver unconnected. With replayOnReconnect,
// the credential provider signs the lookback the SDK asks for into the token:
// the canonical mapping.
func newReconnectSetup(t *testing.T, label string, replayOnReconnect bool) *reconnectSetup {
	t.Helper()

	setup := &reconnectSetup{proxy: newDroppingProxy(t), recovered: make(chan struct{})}
	reference := uniqueChannelReference(label)

	client := clientWith(t, setup.proxy.url(), func(_ context.Context, request celeris.CredentialRequest) (celeris.Credentials, error) {
		setup.mutex.Lock()
		setup.requests = append(setup.requests, request)
		setup.mutex.Unlock()

		if replayOnReconnect && request.Reconnect {
			return signCredentials(clientID(), signingSecret(), claim{"replay", request.ReplayLookback.Milliseconds()}), nil
		}

		return signCredentials(clientID(), signingSecret()), nil
	})

	setup.receiver = newChannel(t, client, reference)
	var once sync.Once

	setup.receiver.Events().OnRecovery(func(event celeris.RecoveryEvent) {
		setup.mutex.Lock()
		setup.recoveries = append(setup.recoveries, event)
		setup.mutex.Unlock()
		once.Do(func() { close(setup.recovered) })
	})

	setup.publisher = connectedChannel(t, reference)

	return setup
} // end function newReconnectSetup

// startOutage cuts every connection through the proxy and refuses new ones,
// so a message published now can reach the receiver only by replay.
func (setup *reconnectSetup) startOutage(t *testing.T) {
	t.Helper()

	setup.proxy.setRefusing(true)
	setup.proxy.dropAll()
	time.Sleep(500 * time.Millisecond)

	if state := setup.receiver.State(); state != celeris.StateReconnecting {
		t.Fatalf("state %s during the outage", state)
	}
} // end method startOutage

// endOutage stops refusing connections and waits for the recovery event.
func (setup *reconnectSetup) endOutage(t *testing.T) {
	t.Helper()

	setup.proxy.setRefusing(false)

	select {
	case <-setup.recovered:
	case <-time.After(45 * time.Second):
		t.Fatal("timed out waiting for the recovery event")
	}
} // end method endOutage

func (setup *reconnectSetup) connect(t *testing.T) {
	t.Helper()

	if err := setup.receiver.Connect(t.Context()); err != nil {
		t.Fatal(err)
	}
} // end method connect

func (setup *reconnectSetup) credentialRequests() []celeris.CredentialRequest {
	setup.mutex.Lock()
	defer setup.mutex.Unlock()

	return slices.Clone(setup.requests)
} // end method credentialRequests

func (setup *reconnectSetup) firstRecovery() celeris.RecoveryEvent {
	setup.mutex.Lock()
	defer setup.mutex.Unlock()

	return setup.recoveries[0]
} // end method firstRecovery

func reconnectRequests(requests []celeris.CredentialRequest) []celeris.CredentialRequest {
	return slices.DeleteFunc(requests, func(request celeris.CredentialRequest) bool { return !request.Reconnect })
} // end function reconnectRequests

// SUB-01, REC-02: an outage recovers with fresh credentials, a replay of what
// it missed, and the subscription restored; replayed duplicates are dropped by
// the deduplication window.
func TestRecoversAfterAnOutageWithReplayAndRestoredSubscriptions(t *testing.T) {
	setup := newReconnectSetup(t, "reconnect", true)
	chat := segment(t, setup.receiver, "chat")
	delivered := collect(chat)
	subscribe(t, chat)
	setup.connect(t)
	settle()
	publishAndAwait(t, setup.publisher, setup.receiver, "chat", "before")

	setup.startOutage(t)
	publishText(t, setup.publisher, "chat", "during")
	time.Sleep(2 * time.Second)
	during := awaitBody(t, chat.OnMessage, "during")
	setup.endOutage(t)
	during()

	if event := setup.firstRecovery(); !event.PossibleGaps || !event.PossibleDuplicates {
		t.Fatalf("recovery %+v", event)
	}

	for _, request := range setup.credentialRequests()[1:] {
		if !request.Reconnect || request.DisconnectedAt.IsZero() || request.ReplayLookback < 5*time.Second {
			t.Fatalf("reconnect request %+v", request)
		}
	}

	// The subscription was restored on the new connection.
	publishAndAwait(t, setup.publisher, setup.receiver, "chat", "after")
	settle()

	// Replay sent "before" again; the deduplication window dropped it.
	assertPayloads(t, delivered, "before", "during", "after")
	identifiers := map[string]bool{}

	for _, message := range delivered.all() {
		identifiers[message.metadata.MessageID] = true
	}

	if len(identifiers) != 3 {
		t.Fatalf("%d unique message ids, want 3", len(identifiers))
	}
} // end function TestRecoversAfterAnOutageWithReplayAndRestoredSubscriptions

func TestRecoversEveryMissedMessageOnSeveralSegmentsInOrderAndOneTime(t *testing.T) {
	setup := newReconnectSetup(t, "reconnect-segments", true)
	alphaSegment := segment(t, setup.receiver, "alpha")
	betaSegment := segment(t, setup.receiver, "beta")
	defaultSegment := segment(t, setup.receiver, "default")
	alpha := collect(alphaSegment)
	beta := collect(betaSegment)
	lobby := collect(defaultSegment)
	channelWide := &collector{}
	setup.receiver.Events().OnMessage(channelWide.record)
	subscribe(t, alphaSegment)
	subscribe(t, betaSegment)
	setup.connect(t)
	settle()
	publishAndAwait(t, setup.publisher, setup.receiver, "alpha", "a0")

	setup.startOutage(t)
	publishAll(t, setup.publisher, "alpha", "a1", "a2", "a3")
	publishAll(t, setup.publisher, "beta", "b1", "b2")
	publishText(t, setup.publisher, "default", "d1")
	time.Sleep(2 * time.Second)
	lastAlpha := awaitBody(t, alphaSegment.OnMessage, "a3")
	lastBeta := awaitBody(t, betaSegment.OnMessage, "b2")
	lastLobby := awaitBody(t, defaultSegment.OnMessage, "d1")
	setup.endOutage(t)
	lastAlpha()
	lastBeta()
	lastLobby()
	settle()

	assertPayloads(t, alpha, "a0", "a1", "a2", "a3")
	assertPayloads(t, beta, "b1", "b2")
	assertPayloads(t, lobby, "d1")
	var entries []string

	for _, message := range channelWide.all() {
		entries = append(entries, message.metadata.SegmentID+":"+string(message.payload))
	}

	slices.Sort(entries)
	want := []string{"alpha:a0", "alpha:a1", "alpha:a2", "alpha:a3", "beta:b1", "beta:b2", "default:d1"}

	if !slices.Equal(entries, want) {
		t.Fatalf("channel listener received %q, want %q", entries, want)
	}
} // end function TestRecoversEveryMissedMessageOnSeveralSegmentsInOrderAndOneTime

func TestLosesMissedMessagesWithoutAReplayClaimButRestoresTheSubscription(t *testing.T) {
	setup := newReconnectSetup(t, "reconnect-no-replay", false)
	chat := segment(t, setup.receiver, "chat")
	delivered := collect(chat)
	subscribe(t, chat)
	setup.connect(t)
	settle()
	publishAndAwait(t, setup.publisher, setup.receiver, "chat", "before")

	setup.startOutage(t)
	publishText(t, setup.publisher, "chat", "missed")
	time.Sleep(2 * time.Second)
	setup.endOutage(t)
	settle()

	publishAndAwait(t, setup.publisher, setup.receiver, "chat", "after")
	settle()

	// The recovery event declares the gap that this test makes.
	if event := setup.firstRecovery(); !event.PossibleGaps {
		t.Fatalf("recovery %+v", event)
	}

	assertPayloads(t, delivered, "before", "after")
} // end function TestLosesMissedMessagesWithoutAReplayClaimButRestoresTheSubscription

func TestRecoversEveryMissedMessageAfterALongerOutageWithFailedAttempts(t *testing.T) {
	setup := newReconnectSetup(t, "reconnect-long", true)
	chat := segment(t, setup.receiver, "chat")
	delivered := collect(chat)
	subscribe(t, chat)
	setup.connect(t)
	settle()

	setup.startOutage(t)
	publishText(t, setup.publisher, "chat", "m1")
	time.Sleep(3 * time.Second)
	publishText(t, setup.publisher, "chat", "m2")
	time.Sleep(3 * time.Second)
	publishText(t, setup.publisher, "chat", "m3")
	time.Sleep(500 * time.Millisecond)
	last := awaitBody(t, chat.OnMessage, "m3")
	setup.endOutage(t)
	last()
	settle()

	// Retries in the first 6.5 s fail (their delays are at most 0.5, 1 and
	// 2 s), each with a fresh credential request and a longer lookback.
	reconnects := reconnectRequests(setup.credentialRequests())

	if len(reconnects) < 4 {
		t.Fatalf("%d reconnect credential requests, want at least 4", len(reconnects))
	}

	var lookbacks []time.Duration

	for _, request := range reconnects {
		lookbacks = append(lookbacks, request.ReplayLookback)

		if !request.DisconnectedAt.Equal(reconnects[0].DisconnectedAt) {
			t.Fatalf("reconnect requests disagree on DisconnectedAt: %v and %v", reconnects[0].DisconnectedAt, request.DisconnectedAt)
		}
	}

	if !slices.IsSorted(lookbacks) || lookbacks[len(lookbacks)-1] < 11*time.Second {
		t.Fatalf("lookbacks %v", lookbacks)
	}

	assertPayloads(t, delivered, "m1", "m2", "m3")
} // end function TestRecoversEveryMissedMessageAfterALongerOutageWithFailedAttempts

func TestDoesNotRejoinASegmentThatTheConnectionJoinedOnlyByPublishing(t *testing.T) {
	setup := newReconnectSetup(t, "reconnect-publish-join", true)
	team := collect(segment(t, setup.receiver, "team"))
	setup.connect(t)
	publishText(t, setup.receiver, "team", "joining")
	settle()
	publishAndAwait(t, setup.publisher, setup.receiver, "team", "before")

	setup.startOutage(t)
	setup.endOutage(t)
	settle()

	publishText(t, setup.publisher, "team", "after")
	confirmQuiet(t, setup.publisher, setup.receiver)

	assertPayloads(t, team, "before")
} // end function TestDoesNotRejoinASegmentThatTheConnectionJoinedOnlyByPublishing

func TestAnnouncesTheNewConnectionAndRestoresItsPresenceSubscriptionAfterAReconnect(t *testing.T) {
	setup := newReconnectSetup(t, "reconnect-presence", true)
	room := segment(t, setup.receiver, "room")
	watched := segment(t, setup.publisher, "room")
	subscribe(t, room)
	subscribePresence(t, room)
	subscribePresence(t, watched)
	firstJoin := awaitPresence(t, watched, func(event celeris.PresenceEvent) bool { return event.Joined }, "the first join", 20*time.Second)
	setup.connect(t)
	before := firstJoin()

	left := awaitPresence(t, watched, func(event celeris.PresenceEvent) bool {
		return !event.Joined && event.ConnectionID == before.ConnectionID
	}, "the leave of the old connection", 20*time.Second)

	setup.startOutage(t)
	left()
	rejoined := awaitPresence(t, watched, func(event celeris.PresenceEvent) bool {
		return event.Joined && event.ConnectionID != before.ConnectionID
	}, "the join of the new connection", 45*time.Second)

	setup.endOutage(t)
	rejoined()

	// The receiver's presence subscription came back with the reconnect.
	actorJoin := awaitPresence(t, room, func(event celeris.PresenceEvent) bool {
		return event.Joined && event.TokenReference == "actor"
	}, "the actor's join at the restored watcher", 20*time.Second)

	actor := connectedChannel(t, setup.credentialRequests()[0].ChannelReference, claim{"reference", "actor"})
	subscribe(t, segment(t, actor, "room"))
	actorJoin()
} // end function TestAnnouncesTheNewConnectionAndRestoresItsPresenceSubscriptionAfterAReconnect

// QUEUE-01: publishes made while the channel reconnects wait for the new
// connection and go out in call order. Here the channel behind the proxy
// publishes and the direct one receives.
func TestDeliversPublishesMadeDuringAnOutageAfterTheReconnectInCallOrder(t *testing.T) {
	setup := newReconnectSetup(t, "reconnect-queued", false)
	received := collect(segment(t, setup.publisher, "queue"))
	subscribe(t, segment(t, setup.publisher, "queue"))
	setup.connect(t)
	settle()
	setup.startOutage(t)

	queue := segment(t, setup.receiver, "queue")

	var results []chan error

	for _, body := range []string{"q1", "q2", "q3"} {
		result := make(chan error, 1)

		go func() { result <- queue.Publish(t.Context(), []byte(body)) }()

		results = append(results, result)

		// Each publish is queued before the next call starts, so the calls
		// keep their order.
		time.Sleep(100 * time.Millisecond)
	}

	setup.endOutage(t)

	for index, result := range results {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("publish %d: %v", index+1, err)
			}
		case <-time.After(20 * time.Second):
			t.Fatalf("publish %d still waiting after the reconnect", index+1)
		}
	}

	eventually(t, func() bool { return len(received.all()) >= 3 }, "the three queued publishes", 20*time.Second)
	confirmQuiet(t, setup.receiver, setup.publisher)
	assertPayloads(t, received, "q1", "q2", "q3")
} // end function TestDeliversPublishesMadeDuringAnOutageAfterTheReconnectInCallOrder

// QUEUE-01, SEG-01: the new connection is a member of exactly the segments the
// old one held, and its presence subscription comes back.
func TestKeepsTheSameSegmentsAfterAReconnect(t *testing.T) {
	setup := newReconnectSetup(t, "reconnect-same-segments", false)
	observer := setup.publisher
	alpha := segment(t, setup.receiver, "alpha")
	alphaMessages := collect(alpha)
	betaMessages := collect(segment(t, setup.receiver, "beta"))
	gammaMessages := collect(segment(t, setup.receiver, "gamma"))
	subscribe(t, alpha)
	subscribe(t, segment(t, setup.receiver, "beta"))
	subscribePresence(t, alpha)
	gamma := subscribe(t, segment(t, setup.receiver, "gamma"))

	observedAlpha := segment(t, observer, "alpha")
	subscribePresence(t, observedAlpha)
	firstJoin := awaitPresence(t, observedAlpha, func(event celeris.PresenceEvent) bool { return event.Joined }, "the receiver's join", 20*time.Second)
	setup.connect(t)
	before := firstJoin()
	gamma.Cancel()
	settle()

	rejoined := awaitPresence(t, observedAlpha, func(event celeris.PresenceEvent) bool {
		return event.Joined && event.ConnectionID != before.ConnectionID
	}, "the join of the new connection", 45*time.Second)

	setup.startOutage(t)
	setup.endOutage(t)
	after := rejoined()
	settle()

	// The new connection is listed in alpha and beta, and not in gamma.
	for _, segmentID := range []string{"alpha", "beta", "gamma"} {
		page, err := segment(t, observer, segmentID).PresenceList(t.Context(), 1, 100)

		if err != nil {
			t.Fatalf("%s: %v", segmentID, err)
		}

		listed := slices.ContainsFunc(page.Connections, func(connection celeris.PresenceConnection) bool {
			return connection.ConnectionID == after.ConnectionID
		})

		if want := segmentID != "gamma"; listed != want {
			t.Fatalf("%s lists the new connection: %v, want %v (%+v)", segmentID, listed, want, page.Connections)
		}
	}

	publishText(t, observer, "alpha", "to-alpha")
	publishText(t, observer, "beta", "to-beta")
	publishText(t, observer, "gamma", "to-gamma")
	confirmQuiet(t, observer, setup.receiver)
	assertPayloads(t, alphaMessages, "to-alpha")
	assertPayloads(t, betaMessages, "to-beta")
	assertPayloads(t, gammaMessages)

	// The restored presence subscription on alpha sees a new actor join.
	actorJoin := awaitPresence(t, alpha, func(event celeris.PresenceEvent) bool {
		return event.Joined && event.TokenReference == "actor"
	}, "the actor's join at the restored watcher", 20*time.Second)

	actor := connectedChannel(t, setup.credentialRequests()[0].ChannelReference, claim{"reference", "actor"})
	subscribe(t, segment(t, actor, "alpha"))
	actorJoin()
} // end function TestKeepsTheSameSegmentsAfterAReconnect
