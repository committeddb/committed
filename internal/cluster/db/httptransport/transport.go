package httptransport

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/db"
)

// This is committed's own raft peer transport. etcd's raft library is transport
// agnostic by design — it hands us Ready().Messages to ship and we Step() what
// arrives; the wire is ours. We previously borrowed etcd's internal
// `etcdserver/api/rafthttp`, which dragged the entire `etcd/server/v3` module
// (mvcc, lease, auth, the v3 API, walpb, …) into the build and gated every raft
// upgrade on a matching etcd *server* release. This is the same job — marshal a
// raftpb.Message, POST it to the target peer, Step incoming ones — with no etcd
// server dependency.
//
// Shape: one bounded send queue + worker goroutine per peer. Send() enqueues
// non-blocking and DROPS when a peer's queue is full — raft retransmits, and the
// raft loop must never block on a slow peer. A failed POST reports the peer
// unreachable so raft backs off probing it. Snapshots flow inline as ordinary
// messages (committed never configured out-of-band snapshot streaming, and its
// snapshot is a bounded bbolt metadata dump), so there is no streaming
// machinery here.
//
// Performance vs the old rafthttp stream transport (measured: 3-node loopback,
// in-memory storage so it's transport-isolated): throughput is comparable, but a
// synchronous commit pays ~2–3× the per-message latency (~+175µs) because each
// message is its own POST round trip rather than a write into a persistent
// stream. That cost is a near-constant on top of the peer RTT and the WAL fsync
// that dominate a real commit, so it is a single-digit-percent bump in
// production, not a structural one — and it is why this is a fine first version
// for committed's intra-region, throughput-oriented, fsync-bound workload. The
// one regime it would genuinely hurt is high-RTT/WAN links, where one in-flight
// POST per peer can't keep a fat pipe full (raft wants up to MaxInflightMsgs in
// flight to a catching-up follower). If that ever shows up in production
// metrics, the bounded fix is to let the per-peer worker keep a few POSTs in
// flight at once — not a stream rewrite. See transport_perf_test.go to re-measure.

const (
	// raftMessagePath is the endpoint a peer POSTs a marshaled raftpb.Message to.
	// The configured peer URL is a base (scheme://host:port); both sides agree on
	// this path appended to it.
	raftMessagePath = "/raft/message"

	// clusterIDHeader / clusterID is a wrong-cluster sanity guard (parity with
	// rafthttp's fixed ClusterID): it stops two committed clusters that
	// accidentally share a network from cross-wiring. It is NOT auth — mTLS (when
	// tlsInfo is set) and the optional shared token below are.
	clusterIDHeader = "X-Committed-Cluster-Id"
	clusterID       = "committed-raft"

	// protocolHeader / protocolVersion lets a future wire change reject an
	// incompatible peer instead of mis-decoding its bytes.
	protocolHeader  = "X-Committed-Raft-Protocol"
	protocolVersion = "1"

	// requestTimeout is the outer bound on a single peer POST. The faster
	// dial/response-header timeouts below are what actually detect a dead peer;
	// this just caps a live peer's whole round trip (snapshots flow inline but
	// are bounded metadata, so they finish well within it).
	requestTimeout = 5 * time.Second

	// peerDialTimeout / peerResponseTimeout detect a dead or restarted peer
	// fast. The serialized per-peer worker must not stall on one slow POST: a
	// killed peer's pooled keep-alive connection accepts the request write but
	// never answers (no RST), so without a response-header bound a POST would
	// hang until requestTimeout — long enough that a peer restarted within a
	// few seconds stays unreachable and the cluster can't re-converge. A 2s
	// bound marks it unreachable promptly (raft retransmits) and the next POST
	// dials the restarted listener fresh.
	peerDialTimeout     = 2 * time.Second
	peerResponseTimeout = 2 * time.Second

	// peerQueueDepth is how many messages may queue per peer before Send drops.
	// Small enough to bound memory, large enough to absorb a Ready burst.
	peerQueueDepth = 64

	// maxMessageBytes caps an incoming message body — a sanity bound on inline
	// snapshots (committed's metadata snapshot is KBs–low MBs); far above it
	// signals the need for out-of-band streaming, which is out of scope.
	maxMessageBytes = 128 << 20
)

type HttpTransport struct {
	id     uint64
	seeds  []raft.Peer // initial peers from New, applied in Start (mirrors the old shape)
	logger *zap.Logger
	raft   db.TransportRaft
	// tlsInfo, if non-nil, is the mTLS configuration used for both the dialing
	// side (the http.Client's TLS config) and the listening side (wrapped around
	// stoppableListener via tls.NewListener in Start). nil means plaintext peer
	// transport — today's default.
	tlsInfo *transport.TLSInfo
	client  *http.Client
	// token, when non-empty (COMMITTED_API_TOKEN set), is required as a bearer on
	// the receive handler and sent on every POST — reusing the API-token posture
	// so a sender without the shared secret can't inject raft messages.
	token string

	errorC chan error

	// baseCtx is canceled by Stop so in-flight POSTs abort promptly.
	baseCtx context.Context
	cancel  context.CancelFunc

	mu    sync.RWMutex
	peers map[uint64]*peer
}

// peer is a single remote node: a bounded queue of messages to send it, drained
// by one worker goroutine that POSTs them in order.
type peer struct {
	id    uint64
	url   string
	msgc  chan *raftpb.Message
	stopc chan struct{}
	done  chan struct{}
}

// Factory returns New as a db.TransportFactory — the form the composition root
// hands to db via db.WithTransportFactory. It is the single adapter that wires
// this concrete transport into db (used by cmd in production and by tests), so
// db itself never imports this package.
func Factory() db.TransportFactory {
	return func(id uint64, peers []raft.Peer, logger *zap.Logger, r db.TransportRaft, tlsInfo *transport.TLSInfo, token string) db.Transport {
		return New(id, peers, logger, r, tlsInfo, token)
	}
}

// New constructs an HttpTransport. The peer registry and dial client are ready
// immediately so callers can Send/Stop right after New; the listener and the
// seed peers are wired in Start (mirroring the prior lifecycle). tlsInfo controls
// mTLS for peer transport — nil means plaintext. token is the cluster bearer
// token sent on peer requests — empty means unauthenticated. Both are injected
// by the composition root (cmd/node.go via db's TransportFactory) rather than
// read from the environment here.
func New(id uint64, ps []raft.Peer, l *zap.Logger, r db.TransportRaft, tlsInfo *transport.TLSInfo, token string) *HttpTransport {
	rt, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		log.Fatalf("httptransport: unexpected http.DefaultTransport type %T", http.DefaultTransport)
	}
	tr := rt.Clone()
	// A handful of idle keep-alive conns per peer so the POST-per-message model
	// reuses connections rather than dialing each time.
	tr.MaxIdleConnsPerHost = 4
	// Detect a dead/restarted peer fast (see peerDialTimeout): a stale pooled
	// connection to a killed peer would otherwise hang the worker until the
	// outer requestTimeout.
	tr.ResponseHeaderTimeout = peerResponseTimeout
	tr.DialContext = (&net.Dialer{Timeout: peerDialTimeout, KeepAlive: 30 * time.Second}).DialContext
	if tlsInfo != nil && !tlsInfo.Empty() {
		cfg, err := tlsInfo.ClientConfig()
		if err != nil {
			// Fatal: bad certs mean this node can't dial peers — the operator must
			// fix the config before it can join (matches the prior fail-fast).
			log.Fatalf("httptransport: peer TLS client config: %v", err)
		}
		tr.TLSClientConfig = cfg
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &HttpTransport{
		id:      id,
		seeds:   ps,
		logger:  l,
		raft:    r,
		tlsInfo: tlsInfo,
		client:  &http.Client{Transport: tr}, // per-request context bounds duration, not Client.Timeout
		token:   token,
		errorC:  make(chan error),
		baseCtx: ctx,
		cancel:  cancel,
		peers:   make(map[uint64]*peer),
	}
}

// GetErrorC returns the fatal-transport-error channel the raft loop selects on.
// Like rafthttp's, it stays dormant in normal operation: a fatal serve error
// surfaces through Start's return (serveRaft log.Fatals on it), and a slow or
// dead peer is a ReportUnreachable, not a transport-fatal.
func (t *HttpTransport) GetErrorC() chan error {
	return t.errorC
}

// Start applies the seed peers, binds the listener on this node's own URL, and
// serves the receive handler until stopC closes. An empty local-peer URL means
// "do not bind a listener" — the right shape for single-node tests: there are no
// peers to accept from, so it just blocks until stopC so serveRaft can tell
// "asked to stop" from "the listener exploded".
func (t *HttpTransport) Start(stopC <-chan struct{}) error {
	rawURL := ""
	for _, p := range t.seeds {
		if p.ID == t.id {
			rawURL = string(p.Context)
			continue
		}
		if err := t.AddPeer(p); err != nil {
			t.logger.Warn("skipping invalid seed peer",
				zap.Uint64("id", p.ID), zap.Error(err))
		}
	}

	if rawURL == "" {
		<-stopC
		return nil
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		log.Fatalf("committed: Failed parsing URL (%v)", err)
	}

	var ln net.Listener
	ln, err = newStoppableListener(u.Host, stopC)
	if err != nil {
		log.Fatalf("committed: Failed to listen for raft peers (%v)", err)
	}

	// Terminate TLS here, in front of the handler. ServerConfig sets ClientAuth
	// to RequireAndVerifyClientCert whenever TrustedCAFile is populated — the
	// mTLS invariant: a peer without a CA-signed client cert can't complete the
	// handshake and never reaches the raft handler.
	if t.tlsInfo != nil {
		tlsCfg, err := t.tlsInfo.ServerConfig()
		if err != nil {
			log.Fatalf("raft peer TLS server config: %v", err)
		}
		ln = tls.NewListener(ln, tlsCfg)
	}

	// ReadHeaderTimeout prevents Slowloris. Other timeouts are left default: a
	// large inline snapshot POST can legitimately take a while to read.
	srv := &http.Server{
		Handler:           t.handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	// On shutdown, Close the server so it drops the listener AND every active
	// peer connection. Closing active connections is load-bearing: a bare
	// Serve-returns-on-listener-close leaves the old server goroutine answering
	// a peer's pooled keep-alive connection after this node is torn down. On a
	// restart at the same address the peer keeps reusing that stale connection
	// (the dead node answers 500, which keep-alive treats as a valid response,
	// so the connection is never discarded) and never dials the fresh listener —
	// the restarted node stays unreachable and the cluster can't re-converge.
	go func() {
		<-stopC
		_ = srv.Close()
	}()

	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// handler is the receive side: validate the cluster/protocol/token, decode the
// message, drop it if its sender was removed, else Step it into raft.
func (t *HttpTransport) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(raftMessagePath, t.handleMessage)
	return mux
}

func (t *HttpTransport) handleMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.Header.Get(clusterIDHeader) != clusterID || r.Header.Get(protocolHeader) != protocolVersion {
		http.Error(w, "wrong cluster or protocol", http.StatusPreconditionFailed)
		return
	}
	if t.token != "" && r.Header.Get("Authorization") != "Bearer "+t.token {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxMessageBytes))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	m, err := unmarshalMessage(body)
	if err != nil {
		http.Error(w, "bad message", http.StatusBadRequest)
		return
	}

	if t.raft.IsIDRemoved(m.GetFrom()) {
		// A removed node must not be able to inject messages.
		http.Error(w, "sender removed from cluster", http.StatusForbidden)
		return
	}
	if err := t.raft.Process(r.Context(), m); err != nil {
		http.Error(w, "process error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// AddPeer registers a peer and starts its send worker. Idempotent: re-adding an
// existing id is a no-op (membership replay can re-announce a peer).
func (t *HttpTransport) AddPeer(p raft.Peer) error {
	if _, err := url.Parse(string(p.Context)); err != nil {
		return err
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.peers[p.ID]; exists {
		return nil
	}
	pr := &peer{
		id:    p.ID,
		url:   string(p.Context),
		msgc:  make(chan *raftpb.Message, peerQueueDepth),
		stopc: make(chan struct{}),
		done:  make(chan struct{}),
	}
	t.peers[p.ID] = pr
	go t.runPeer(pr)
	return nil
}

// RemovePeer stops a peer's worker and drops it from the registry. It does not
// block on the worker draining its current POST (up to requestTimeout); the
// worker exits on its own after, and no new messages route to a dropped peer.
func (t *HttpTransport) RemovePeer(id uint64) {
	t.mu.Lock()
	pr, ok := t.peers[id]
	if ok {
		delete(t.peers, id)
	}
	t.mu.Unlock()
	if ok {
		close(pr.stopc)
	}
}

// Send routes each message to its target peer's queue, non-blocking. Messages to
// self, to id 0, or to an unknown peer are dropped, as are messages for a peer
// whose queue is full — raft retransmits, and the raft loop must never block.
func (t *HttpTransport) Send(msgs []*raftpb.Message) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, m := range msgs {
		if m.GetTo() == 0 || m.GetTo() == t.id {
			continue
		}
		pr, ok := t.peers[m.GetTo()]
		if !ok {
			continue
		}
		select {
		case pr.msgc <- m:
		default:
			// Queue full: drop. raft will retransmit. Reporting unreachable here
			// would be too aggressive (a transient burst, not a dead peer).
		}
	}
}

// Stop cancels in-flight POSTs and stops every peer worker. The listener is shut
// down separately, via the stopC passed to Start.
func (t *HttpTransport) Stop() {
	t.mu.Lock()
	peers := t.peers
	t.peers = make(map[uint64]*peer)
	t.mu.Unlock()

	for _, pr := range peers {
		close(pr.stopc)
	}
	t.cancel()
}

// runPeer drains a peer's queue, delivering messages in order until stopped.
func (t *HttpTransport) runPeer(pr *peer) {
	defer close(pr.done)
	for {
		select {
		case <-pr.stopc:
			return
		case m := <-pr.msgc:
			t.deliver(pr, m)
		}
	}
}

// deliver POSTs one message and feeds the result back to raft: a failure marks
// the peer unreachable (and fails any snapshot); a success confirms a snapshot.
func (t *HttpTransport) deliver(pr *peer, m *raftpb.Message) {
	if err := t.post(pr, m); err != nil {
		t.logger.Debug("raft peer send failed",
			zap.Uint64("to", m.GetTo()), zap.Error(err))
		t.raft.ReportUnreachable(m.GetTo())
		if m.GetType() == raftpb.MsgSnap {
			t.raft.ReportSnapshot(m.GetTo(), raft.SnapshotFailure)
		}
		return
	}
	if m.GetType() == raftpb.MsgSnap {
		t.raft.ReportSnapshot(m.GetTo(), raft.SnapshotFinish)
	}
}

func (t *HttpTransport) post(pr *peer, m *raftpb.Message) error {
	data, err := marshalMessage(m)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	ctx, cancel := context.WithTimeout(t.baseCtx, requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, pr.url+raftMessagePath, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set(clusterIDHeader, clusterID)
	req.Header.Set(protocolHeader, protocolVersion)
	if t.token != "" {
		req.Header.Set("Authorization", "Bearer "+t.token)
	}

	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body) // drain so the keep-alive conn is reusable

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("peer %d returned %s", m.GetTo(), resp.Status)
	}
	return nil
}

// marshalMessage / unmarshalMessage isolate the raftpb wire calls. raft 3.7
// moved raftpb to google.golang.org/protobuf, so the gogo Marshal/Unmarshal
// methods are gone — these go through proto. The wire bytes are unchanged
// (same field numbers), so a message marshaled by either generation decodes
// with the other.
func marshalMessage(m *raftpb.Message) ([]byte, error) {
	return proto.Marshal(m)
}

func unmarshalMessage(b []byte) (*raftpb.Message, error) {
	m := &raftpb.Message{}
	err := proto.Unmarshal(b, m)
	return m, err
}
