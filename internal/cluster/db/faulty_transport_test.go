package db_test

import (
	"math/rand"
	"sync"
	"time"

	"github.com/philborlin/committed/internal/cluster/db"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// FaultyTransport wraps a per-node db.Transport and implements db.Transport.
// Phase 1 is a group-level partition wrapper that delegates to the inner
// transport's RemovePeer/AddPeer. Phase 2 adds Send-path fault injection
// (directional drops, latency, probabilistic drops) driven by per-transport
// state updated via the shared FaultyCluster's DirectionalDrop / AddLatency /
// DropRate APIs.
//
// Group-level fault orchestration (Partition, Heal, DirectionalDrop,
// AddLatency, DropRate) is coordinated through a shared *FaultyCluster
// pointer rather than per-transport state. Every FaultyTransport in a
// single test cluster points at the same FaultyCluster, so Partition on
// any one transport takes effect cluster-wide.
//
// The underlying mechanism for Partition / Heal is the inner transport's
// RemovePeer / AddPeer pair — the same primitive PartitionPeerForTest and
// UnpartitionPeerForTest already use. FaultyCluster just books-keeps which
// directional drops are currently in effect so Heal can restore them.
//
// Send-path faults (DirectionalDrop / AddLatency / DropRate) are INDEPENDENT
// of Partition. They filter / delay messages inside Send() without touching
// AddPeer/RemovePeer. This lets tests compose "partition A↔B AND latency
// between C and D AND drop-rate on E→F" without the fault types interacting.
type FaultyTransport struct {
	inner db.Transport
	id    uint64

	// peers is this node's view of the cluster (includes itself). Stored
	// so Heal can re-register peers by the original raft.Peer Context
	// (URL) the cluster was started with.
	peers []raft.Peer

	// cluster is shared across every FaultyTransport in the same test
	// cluster. nil if the transport was constructed standalone (no
	// group-level faults available).
	cluster *FaultyCluster

	// sendMu guards the Send-path fault maps. Updated by
	// FaultyCluster.DirectionalDrop / AddLatency / DropRate (called from
	// test goroutines); read on every Send call (called from the raft
	// Ready loop goroutine). A single mutex across all three maps keeps
	// Send's fault-lookup path a single lock cycle.
	sendMu    sync.Mutex
	dirDrops  map[uint64]bool
	latencies map[uint64]time.Duration
	dropRates map[uint64]float64

	// rng is used by Send for DropRate probability rolls. Seeded by the
	// FaultyCluster so per-test seeds propagate to every transport.
	// Guarded by sendMu along with dropRates — the two are read together.
	rng *rand.Rand

	// workers is one goroutine per destination that delivers latency-
	// affected messages serially. Created lazily on the first latency-
	// affected Send to a destination; once created, ALL subsequent
	// messages to that destination pass through the worker even when
	// the current latency is zero. That guarantee is what preserves
	// FIFO-per-peer ordering: raft assumes messages on a single peer
	// connection arrive in order, and splitting traffic to the same
	// destination between "direct Send" and "delayed Send" would break
	// that invariant if latency were added mid-flight.
	//
	// Guarded by workersMu (NOT sendMu) because Send holds sendMu while
	// deciding whether to enqueue, and the enqueue itself must not block
	// on sendMu — channel sends under a heavily-contested lock deadlock
	// surprisingly easily.
	workersMu sync.Mutex
	workers   map[uint64]chan pendingMsg
	stopped   bool
	workersWG sync.WaitGroup
}

// pendingMsg is one message queued for delivery to a specific destination by
// the per-destination worker goroutine. deliverAt is baked in at enqueue
// time (now + current latency for that destination) so a later AddLatency
// call does not change the effective delay of messages already in flight.
//
// Using an absolute deliver time (rather than a raw delay the worker sleeps
// for serially) is what lets the latency model a real network: a 500ms
// one-way delay should not serialize the queue into 500ms × N — ten
// simultaneously-sent messages should all arrive ~500ms later, not 5s.
// The worker sleeps until deliverAt; if later-queued messages have earlier
// or equal deliverAt, they just follow the head immediately once it's
// delivered, which preserves FIFO while amortizing the delay.
type pendingMsg struct {
	msg       raftpb.Message
	deliverAt time.Time
}

// --- db.Transport passthroughs ---

func (t *FaultyTransport) GetErrorC() chan error             { return t.inner.GetErrorC() }
func (t *FaultyTransport) Start(stopC <-chan struct{}) error { return t.inner.Start(stopC) }
func (t *FaultyTransport) AddPeer(p raft.Peer) error         { return t.inner.AddPeer(p) }
func (t *FaultyTransport) RemovePeer(id uint64)              { t.inner.RemovePeer(id) }

// Send filters and/or delays each outbound raft message per the per-
// destination fault state. Composition rules (all three can be active
// simultaneously without interaction bugs):
//
//  1. Directional drop wins first. If the (id → msg.To) edge is dropped,
//     the message is discarded — drop-rate and latency never run.
//  2. Drop rate runs second. If the dice roll lands in the drop fraction,
//     the message is discarded even though the edge is otherwise healthy.
//  3. Latency runs last. If the (id → msg.To) edge has a non-zero latency
//     (or has ever had one — the worker is sticky), the message is enqueued
//     on the per-destination worker; the worker sleeps for the baked-in
//     delay and then calls inner.Send([]msg{msg}) with exactly that message.
//
// Messages not matched by any fault (no drop, no latency worker) are sent
// directly via inner.Send. This keeps the no-fault-injection fast path
// one function call deep, matching phase 1's passthrough behaviour.
func (t *FaultyTransport) Send(msgs []raftpb.Message) {
	// Snapshot the per-destination state under sendMu, then release before
	// doing any actual work. Holding sendMu across inner.Send or across a
	// channel enqueue risks contention with DirectionalDrop/AddLatency calls
	// from test goroutines; since Send is called from one Ready loop
	// goroutine, the snapshot is race-free for the duration of this call.
	var direct []raftpb.Message
	var queued []pendingQueue

	t.sendMu.Lock()
	for _, m := range msgs {
		if t.dirDrops != nil && t.dirDrops[m.To] {
			continue
		}
		if t.dropRates != nil {
			if rate, ok := t.dropRates[m.To]; ok && rate > 0 && t.rng.Float64() < rate {
				continue
			}
		}
		var delay time.Duration
		if t.latencies != nil {
			delay = t.latencies[m.To]
		}
		// If a worker already exists for this destination, every message
		// must flow through it — even if the current delay is zero — so
		// FIFO ordering is preserved across a mid-flight AddLatency(0).
		t.workersMu.Lock()
		_, hasWorker := t.workers[m.To]
		t.workersMu.Unlock()
		if delay > 0 || hasWorker {
			queued = append(queued, pendingQueue{to: m.To, msg: m, deliverAt: time.Now().Add(delay)})
		} else {
			direct = append(direct, m)
		}
	}
	t.sendMu.Unlock()

	if len(direct) > 0 {
		t.inner.Send(direct)
	}
	for _, q := range queued {
		t.enqueueToWorker(q.to, pendingMsg{msg: q.msg, deliverAt: q.deliverAt})
	}
}

// pendingQueue is the intermediate form used between fault-filter evaluation
// (Send) and worker enqueue. Keeping it separate from pendingMsg lets us
// snapshot the destination under sendMu without holding workersMu across
// the lock boundary.
type pendingQueue struct {
	to        uint64
	msg       raftpb.Message
	deliverAt time.Time
}

// enqueueToWorker routes one message through the per-destination worker
// goroutine, creating the worker lazily if absent. The workers map is
// guarded by workersMu independently of sendMu.
//
// If Stop has already run (t.stopped = true), the message is dropped. That
// is acceptable because Stop is only called during teardown, when any
// messages in flight are by definition irrelevant.
func (t *FaultyTransport) enqueueToWorker(to uint64, p pendingMsg) {
	t.workersMu.Lock()
	if t.stopped {
		t.workersMu.Unlock()
		return
	}
	ch, ok := t.workers[to]
	if !ok {
		if t.workers == nil {
			t.workers = make(map[uint64]chan pendingMsg)
		}
		// Large buffer so the Send path never blocks on worker backpressure.
		// Each queued message costs ~a few hundred bytes, so 4096 entries
		// is ~1MB of memory per active worker — a cheap price for keeping
		// the raft Ready loop responsive. With a too-small buffer, a burst
		// of catch-up MsgApps plus new proposals can fill the channel; Send
		// then blocks serveChannels, which blocks Ticks, which triggers
		// election timeouts and spurious re-elections under load.
		ch = make(chan pendingMsg, 4096)
		t.workers[to] = ch
		t.workersWG.Add(1)
		go t.runWorker(ch)
	}
	t.workersMu.Unlock()
	// Send outside the lock. With a generous buffer, this should never
	// block in practice; the unbounded-send alternative would desync raft
	// and is worse than a bounded wait.
	ch <- p
}

// runWorker drains one destination's queue in order, each message held
// until its absolute deliverAt time, then sent via inner.Send. Consecutive
// already-due messages are batched into a single inner.Send call so the
// rafthttp transport sees the same "here's a bunch of messages" shape it
// would get without the wrapper — a per-message inner.Send burns
// proportional setup overhead per message, which showed up as apparent
// "slow catch-up" in the scenario (d) regression pass.
//
// FIFO is preserved: the worker reads from the channel in order, holds
// each message until its deliverAt, and sends them in that order. Messages
// enqueued in quick succession (same Ready cycle at the leader) all have
// deliverAts within a few milliseconds, so after the first message's
// latency elapses the rest of the burst is already due and goes out in
// the same batch.
//
// Exits when the channel is closed by Stop (drains any remaining queued
// messages first so nothing is lost in teardown).
func (t *FaultyTransport) runWorker(ch chan pendingMsg) {
	defer t.workersWG.Done()
	for first := range ch {
		if wait := time.Until(first.deliverAt); wait > 0 {
			time.Sleep(wait)
		}
		batch := []raftpb.Message{first.msg}
		// Drain any messages that are also already due. Use a non-
		// blocking receive so we flush the batch promptly when the queue
		// runs out.
	drain:
		for {
			select {
			case next, ok := <-ch:
				if !ok {
					// Channel closed mid-drain: flush and exit.
					t.inner.Send(batch)
					return
				}
				if wait := time.Until(next.deliverAt); wait > 0 {
					// Next message isn't due yet. Flush what we have,
					// then sleep until this one is due (starting the
					// next batch with it).
					t.inner.Send(batch)
					time.Sleep(wait)
					batch = []raftpb.Message{next.msg}
					continue
				}
				batch = append(batch, next.msg)
			default:
				break drain
			}
		}
		t.inner.Send(batch)
	}
}

// Stop tears down the inner transport and stops any per-destination workers.
// Idempotent: a second call is a no-op (stopped flag guards).
//
// Worker shutdown order:
//  1. Set t.stopped = true so new enqueues become drops.
//  2. Close every worker channel so runWorker loops exit.
//  3. Wait for all workers via WaitGroup.
//  4. Stop the inner transport.
//
// The drain-then-close ordering matters: if we stopped the inner transport
// first, any in-flight worker sleep would wake and try to call inner.Send
// on a stopped transport, racing teardown.
func (t *FaultyTransport) Stop() {
	t.workersMu.Lock()
	if t.stopped {
		t.workersMu.Unlock()
		t.inner.Stop()
		return
	}
	t.stopped = true
	chans := make([]chan pendingMsg, 0, len(t.workers))
	for _, ch := range t.workers {
		chans = append(chans, ch)
	}
	t.workers = nil
	t.workersMu.Unlock()
	for _, ch := range chans {
		close(ch)
	}
	t.workersWG.Wait()
	t.inner.Stop()
}

// Partition is a convenience pass-through to the shared cluster's Partition.
// Phase 1 scenarios can equivalently call the method directly on FaultyCluster;
// exposing it here matches the ticket's named API and keeps the call site
// terser when a test already has a FaultyTransport handle.
func (t *FaultyTransport) Partition(groupA, groupB []uint64) {
	if t.cluster == nil {
		return
	}
	t.cluster.Partition(groupA, groupB)
}

// Heal mirrors FaultyCluster.Heal for the same convenience reason as Partition.
func (t *FaultyTransport) Heal() {
	if t.cluster == nil {
		return
	}
	t.cluster.Heal()
}

// --- FaultyCluster ---

// FaultyCluster is the shared coordinator across all FaultyTransports in a
// test cluster. Every FaultyTransport created via FaultyCluster.Wrap is
// registered here by node id; Partition and Heal then dispatch directional
// RemovePeer / AddPeer calls across the cluster, and DirectionalDrop /
// AddLatency / DropRate dispatch Send-path fault-state updates to the
// targeted transport.
//
// Safe for concurrent use: Partition and Heal hold an internal mutex, and
// the transports registered with Wrap are read-only after cluster creation.
type FaultyCluster struct {
	peers []raft.Peer

	// seed is the RNG seed propagated to every wrapped FaultyTransport.
	// Drop-rate decisions are rng.Float64() < fraction, so reproducibility
	// requires every transport to share the same seed stream or at least
	// a deterministic one. Per-transport rng (rather than cluster-wide)
	// avoids cross-transport ordering dependencies — Send calls from
	// different nodes would race over a shared rng and produce
	// non-deterministic drop patterns. Each transport derives its own
	// rng from seed + id so two tests with the same seed reproduce.
	seed int64

	mu         sync.Mutex
	transports map[uint64]*FaultyTransport
	// partitioned tracks every (fromNode, peerID) pair currently in a
	// dropped state so Heal can reverse them. The peerID is kept
	// separately because the cluster owns the canonical peers slice —
	// indexing by peerID at heal time avoids storing a copy per entry.
	partitioned []partitionEntry
}

type partitionEntry struct {
	fromNode uint64
	peerID   uint64
}

// NewFaultyCluster constructs a FaultyCluster that expects the given peer
// set. Pass the same []raft.Peer used to construct the raft cluster itself
// — FaultyCluster copies the slice so later mutations by the caller do not
// affect stored peer contexts.
//
// Uses seed=1 for drop-rate reproducibility. Tests that want a different
// seed (e.g., to exercise a different drop pattern) should call
// NewFaultyClusterSeeded.
func NewFaultyCluster(peers []raft.Peer) *FaultyCluster {
	return NewFaultyClusterSeeded(peers, 1)
}

// NewFaultyClusterSeeded constructs a FaultyCluster with an explicit RNG
// seed. The seed is propagated to every wrapped FaultyTransport so
// DropRate decisions are reproducible across test runs.
func NewFaultyClusterSeeded(peers []raft.Peer, seed int64) *FaultyCluster {
	cp := make([]raft.Peer, len(peers))
	copy(cp, peers)
	return &FaultyCluster{
		peers:      cp,
		seed:       seed,
		transports: make(map[uint64]*FaultyTransport),
	}
}

// Wrap returns a TransportWrapper function suitable for passing to
// db.WithTransportWrapperForTest. The returned closure constructs and
// registers a FaultyTransport for the given node id on first call; if
// Wrap is invoked twice for the same id (e.g., restart during a flap
// test), the second call replaces the registered transport so Partition
// targets the new inner.
//
// Each transport gets its own rng derived from (seed XOR id) so drop-rate
// rolls are deterministic per test AND per node — two transports in the
// same cluster don't share RNG state (which would race).
func (c *FaultyCluster) Wrap(id uint64) func(db.Transport) db.Transport {
	return func(inner db.Transport) db.Transport {
		ft := &FaultyTransport{
			inner:   inner,
			id:      id,
			peers:   c.peers,
			cluster: c,
			rng:     rand.New(rand.NewSource(c.seed ^ int64(id))),
		}
		c.mu.Lock()
		c.transports[id] = ft
		c.mu.Unlock()
		return ft
	}
}

// Partition drops every message between groupA and groupB, bidirectionally,
// until Heal is called. See the phase-1 comment in this file for invariants.
func (c *FaultyCluster) Partition(groupA, groupB []uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, a := range groupA {
		for _, b := range groupB {
			c.dropLocked(a, b)
			c.dropLocked(b, a)
		}
	}
}

// dropLocked removes peer `to` from node `from`'s transport and records it
// for Heal. Silently no-ops if `from` is not registered.
func (c *FaultyCluster) dropLocked(from, to uint64) {
	ft, ok := c.transports[from]
	if !ok {
		return
	}
	ft.inner.RemovePeer(to)
	c.partitioned = append(c.partitioned, partitionEntry{fromNode: from, peerID: to})
}

// Heal restores every peer connection that Partition has dropped. Iterates
// the partitioned slice in insertion order and re-adds each (fromNode, peerID)
// pair via AddPeer with the peer's original raft.Peer (Context URL).
func (c *FaultyCluster) Heal() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.partitioned {
		ft, ok := c.transports[e.fromNode]
		if !ok {
			continue
		}
		peer, ok := c.peerByID(e.peerID)
		if !ok {
			continue
		}
		_ = ft.inner.AddPeer(peer)
	}
	c.partitioned = nil
}

// DirectionalDrop enables or disables drop-all for messages `from` → `to`.
// Asymmetric by design: DirectionalDrop(1, 2, true) does NOT imply
// DirectionalDrop(2, 1, true). This is the finer-grained cousin of
// Partition — Partition uses RemovePeer/AddPeer at the transport's peer
// registry level (which etcd rafthttp treats as bidirectional-ish:
// incoming messages from a removed peer are also rejected by the raft
// state machine because it doesn't know who sent them). DirectionalDrop
// filters Send() at the outbound side only, leaving the inbound path open.
//
// That difference is the whole point of scenario (b) — a leader that
// can't reach a follower but receives PreVote messages from that follower
// is exactly what PreVote was designed to handle, and Partition cannot
// express it because the other direction is also blocked.
func (c *FaultyCluster) DirectionalDrop(from, to uint64, on bool) {
	c.mu.Lock()
	ft, ok := c.transports[from]
	c.mu.Unlock()
	if !ok {
		return
	}
	ft.sendMu.Lock()
	defer ft.sendMu.Unlock()
	if on {
		if ft.dirDrops == nil {
			ft.dirDrops = make(map[uint64]bool)
		}
		ft.dirDrops[to] = true
	} else if ft.dirDrops != nil {
		delete(ft.dirDrops, to)
	}
}

// AddLatency sets the per-send delay on the `from` → `to` edge. Passing
// dur=0 disables latency for that edge but does NOT tear down the
// destination worker — once a worker exists for a destination, all
// subsequent messages to that destination still flow through it to
// preserve FIFO ordering relative to any in-flight queued messages.
//
// Calling AddLatency while messages are queued does not re-delay them:
// each queued message has its delay baked in at enqueue time, so the
// new latency takes effect only for messages enqueued after the call.
// This guarantees no reordering of in-flight traffic.
func (c *FaultyCluster) AddLatency(from, to uint64, dur time.Duration) {
	c.mu.Lock()
	ft, ok := c.transports[from]
	c.mu.Unlock()
	if !ok {
		return
	}
	ft.sendMu.Lock()
	defer ft.sendMu.Unlock()
	if ft.latencies == nil {
		ft.latencies = make(map[uint64]time.Duration)
	}
	if dur <= 0 {
		delete(ft.latencies, to)
	} else {
		ft.latencies[to] = dur
	}
}

// DropRate sets the probabilistic drop fraction on the `from` → `to` edge.
// fraction is in [0.0, 1.0]; 0 disables, 1.0 drops every message (behaves
// like DirectionalDrop). Values outside the range are clamped: <0 is
// treated as 0, >1 as 1. The seed per transport (NewFaultyClusterSeeded)
// makes the drop pattern reproducible for a given test.
func (c *FaultyCluster) DropRate(from, to uint64, fraction float64) {
	if fraction < 0 {
		fraction = 0
	}
	if fraction > 1 {
		fraction = 1
	}
	c.mu.Lock()
	ft, ok := c.transports[from]
	c.mu.Unlock()
	if !ok {
		return
	}
	ft.sendMu.Lock()
	defer ft.sendMu.Unlock()
	if ft.dropRates == nil {
		ft.dropRates = make(map[uint64]float64)
	}
	if fraction == 0 {
		delete(ft.dropRates, to)
	} else {
		ft.dropRates[to] = fraction
	}
}

// peerByID looks up the raft.Peer with the given id in the cluster's peer
// set. Returns the zero peer and false if not found. Callers already hold
// c.mu; the peers slice is immutable after cluster construction, so no
// additional locking is needed.
func (c *FaultyCluster) peerByID(id uint64) (raft.Peer, bool) {
	for _, p := range c.peers {
		if p.ID == id {
			return p, true
		}
	}
	return raft.Peer{}, false
}
