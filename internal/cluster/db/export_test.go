package db

import "go.etcd.io/etcd/raft/v3"

// PartitionPeerForTest drops the named peer from this Raft's transport so
// outbound messages to it become no-ops. Combined with calling the same
// method on the OTHER side of the partition, this simulates a transport-
// level network partition without touching the etcd raft state machine.
//
// This is exposed as an exported method on a *_test.go file so it lives
// in the production package (giving it access to n.transport) but does
// not ship in non-test builds. Tests in package db_test reach it via
// db.(*Raft).PartitionPeerForTest.
//
// Used by TestPreVote_PartitionedFollowerDoesNotDisruptLeader to verify
// that a partitioned-then-rejoining follower does not disrupt a healthy
// leader when PreVote is enabled.
func (n *Raft) PartitionPeerForTest(id uint64) {
	n.transport.RemovePeer(id)
}

// UnpartitionPeerForTest re-adds a previously-removed peer to this Raft's
// transport, healing the partition. The peer struct must carry the same
// Context (URL) the cluster was started with — the test passes the
// original raft.Peer back through.
func (n *Raft) UnpartitionPeerForTest(p raft.Peer) error {
	return n.transport.AddPeer(p)
}

// CommitIndexForTest returns this Raft's current commit index as reported
// by etcd raft's Status snapshot. Used by adversarial tests to assert that
// a minority-side leader does not advance commit while partitioned (safety
// invariant: no quorum → no new commits).
func (n *Raft) CommitIndexForTest() uint64 {
	if n.node == nil {
		return 0
	}
	return n.node.Status().HardState.Commit
}

// WithTransportWrapperForTest installs a wrapper around the Transport that
// db.Raft's startRaft constructs. The wrapper receives the plain
// HttpTransport and returns a Transport that's used in its place. Applied
// exactly once per Raft, before serveRaft starts driving the transport, so
// Send/AddPeer/RemovePeer/Start/Stop all flow through the wrapper for the
// lifetime of the Raft.
//
// Only used by the adversarial suite (see faulty_transport_test.go). The
// field on options is unexported, so this test-only Option is the single
// entry point.
func WithTransportWrapperForTest(w func(Transport) Transport) Option {
	return func(o *options) { o.transportWrapper = w }
}

// IngestWorkerIDsForTest returns a snapshot of the ingest worker registry
// keys — one entry per actively-running ingest worker. Used by the
// adversarial suite's concurrent-config-change scenario to assert that
// concurrent ProposeIngestable calls don't leave orphaned workers or
// duplicate entries in the registry.
//
// Takes the registry mutex to avoid racing the registry's insertion /
// deletion paths; returns a copy so the caller can sort / inspect
// without holding the lock.
func (db *DB) IngestWorkerIDsForTest() []string {
	db.workersMu.Lock()
	defer db.workersMu.Unlock()
	ids := make([]string, 0, len(db.ingestWorkers))
	for id := range db.ingestWorkers {
		ids = append(ids, id)
	}
	return ids
}
