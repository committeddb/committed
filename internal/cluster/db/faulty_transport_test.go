package db_test

import (
	"sync"

	"github.com/philborlin/committed/internal/cluster/db"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// FaultyTransport wraps a per-node db.Transport and implements db.Transport.
// Phase 1 is a passthrough — every method delegates to the inner transport.
// Phase 2 will grow Send-time fault injection (latency, directional drops,
// drop-rate) into this struct in place, which is why the wrapper exists now
// even though its methods don't modify behaviour yet.
//
// Group-level fault orchestration (Partition, Heal) is coordinated through
// a shared *FaultyCluster pointer rather than per-transport state. Every
// FaultyTransport in a single test cluster points at the same FaultyCluster,
// so Partition on any one transport takes effect cluster-wide.
//
// The underlying mechanism for phase 1 is the inner transport's
// RemovePeer / AddPeer pair — the same primitive PartitionPeerForTest and
// UnpartitionPeerForTest already use. FaultyCluster just books-keeps which
// directional drops are currently in effect so Heal can restore them.
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
}

// --- db.Transport passthroughs ---

func (t *FaultyTransport) GetErrorC() chan error               { return t.inner.GetErrorC() }
func (t *FaultyTransport) Start(stopC <-chan struct{}) error   { return t.inner.Start(stopC) }
func (t *FaultyTransport) AddPeer(p raft.Peer) error           { return t.inner.AddPeer(p) }
func (t *FaultyTransport) RemovePeer(id uint64)                { t.inner.RemovePeer(id) }
func (t *FaultyTransport) Send(msgs []raftpb.Message)          { t.inner.Send(msgs) }
func (t *FaultyTransport) Stop()                               { t.inner.Stop() }

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

// FaultyCluster is the shared coordinator across all FaultyTransports in a
// test cluster. Every FaultyTransport created via FaultyCluster.Wrap is
// registered here by node id; Partition and Heal then dispatch directional
// RemovePeer / AddPeer calls across the cluster.
//
// Safe for concurrent use: Partition and Heal hold an internal mutex, and
// the transports registered with Wrap are read-only after cluster creation.
type FaultyCluster struct {
	// peers is the cluster-wide peer set (every node, with its original
	// Context URL). Needed so Heal can re-register a peer with AddPeer —
	// AddPeer takes a raft.Peer, not just an id, and the Context carries
	// the URL that the rafthttp layer needs to reconnect.
	peers []raft.Peer

	mu          sync.Mutex
	transports  map[uint64]*FaultyTransport
	// partitioned tracks every (fromNode, peerID) pair currently in a
	// dropped state so Heal can reverse them. The peerID is kept
	// separately because the cluster owns the canonical peers slice —
	// indexing by peerID at heal time avoids storing a copy per entry.
	//
	// A map-of-map (fromNode → set of peerIDs) would be slightly more
	// efficient for deduplication, but in practice Partition is called
	// with disjoint groups so duplicates don't arise, and a flat slice
	// keeps the data structure obvious. Heal just iterates.
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
func NewFaultyCluster(peers []raft.Peer) *FaultyCluster {
	cp := make([]raft.Peer, len(peers))
	copy(cp, peers)
	return &FaultyCluster{
		peers:      cp,
		transports: make(map[uint64]*FaultyTransport),
	}
}

// Wrap returns a TransportWrapper function suitable for passing to
// db.WithTransportWrapperForTest. The returned closure constructs and
// registers a FaultyTransport for the given node id on first call; if
// Wrap is invoked twice for the same id (e.g., restart during a flap
// test), the second call replaces the registered transport so Partition
// targets the new inner.
func (c *FaultyCluster) Wrap(id uint64) func(db.Transport) db.Transport {
	return func(inner db.Transport) db.Transport {
		ft := &FaultyTransport{
			inner:   inner,
			id:      id,
			peers:   c.peers,
			cluster: c,
		}
		c.mu.Lock()
		c.transports[id] = ft
		c.mu.Unlock()
		return ft
	}
}

// Partition drops every message between groupA and groupB, bidirectionally,
// until Heal is called. Messages within a group are unaffected; messages to
// or from nodes not present in either group are also unaffected.
//
// Implementation: for each (a, b) ∈ groupA × groupB, call RemovePeer on
// a's transport (dropping a→b) and on b's transport (dropping b→a).
// Records both directions in partitioned so Heal can reverse them via
// AddPeer with the peer's original raft.Peer Context.
//
// A second call to Partition without an intervening Heal is additive —
// new drops stack on top of old ones. Calling Partition with overlapping
// groups (a node in both) is a programmer error; we don't special-case
// it. Heal restores every tracked drop in one sweep regardless.
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
// for Heal. Silently no-ops if `from` is not registered (e.g., cluster set
// up incorrectly) — tests that mix registered and unregistered ids should
// fail via their own assertions, not via a panic here.
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
//
// AddPeer failures are swallowed with a log-less skip: in practice AddPeer
// only fails if the peer's Context URL is malformed, which can't happen
// because we use the exact same raft.Peer the cluster was constructed with.
// A soft skip means a spurious failure doesn't leave the cluster in a
// half-healed state — every other (from, to) pair still gets its chance.
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
		// AddPeer is idempotent for rafthttp — re-adding an already-present
		// peer is a no-op — so we don't need to track "is this currently
		// removed?" state. Attempt every stored drop.
		_ = ft.inner.AddPeer(peer)
	}
	c.partitioned = nil
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

