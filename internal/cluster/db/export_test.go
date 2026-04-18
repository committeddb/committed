package db

import (
	"context"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"go.uber.org/zap"
)

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
	return n.node.Status().Commit
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

// NewRaftForCompactionTest constructs a bare Raft sufficient to drive
// maybeCompact from a test goroutine. It skips startRaft (no etcd
// raft.Node, no transport, no serve loops) — the tests that use it
// only exercise the compaction decision logic, which reads
// storage.AppliedIndex / EventIndex / RaftLogApproxSize and calls
// storage.CreateSnapshot / Compact. Production callers must continue
// to use NewRaft.
func NewRaftForCompactionTest(s Storage, maxSize uint64, maxAge time.Duration, logger *zap.Logger) *Raft {
	return &Raft{
		storage:         s,
		compactMaxSize:  maxSize,
		compactMaxAge:   maxAge,
		lastCompactTime: time.Now(),
		logger:          logger,
	}
}

// MaybeCompactForTest exposes maybeCompact so unit tests can drive the
// decision logic without wiring up a full Ready loop.
func (n *Raft) MaybeCompactForTest() {
	n.maybeCompact()
}

// LastCompactedIndexForTest lets tests observe the bookkeeping
// maybeCompact maintains after a successful compaction.
func (n *Raft) LastCompactedIndexForTest() uint64 {
	return n.lastCompactedIndex.Load()
}

// SetLastCompactTimeForTest back-dates the internal timestamp so the
// age limb of the compaction policy fires without sleeping. A raw
// time setter is the simplest way to drive that path deterministically.
func (n *Raft) SetLastCompactTimeForTest(t time.Time) {
	n.lastCompactTime = t
}

// SetLeaderIDForTest drives a leader-ID observation through this DB's
// LeaderState, as if the raft Ready loop had just seen a transition.
// Used by propose-fail-fast tests to trigger the watcher without
// standing up a full multi-node flap.
func (db *DB) SetLeaderIDForTest(id uint64) {
	db.leaderState.SetLeaderID(id)
}

// ObservedLeaderForTest returns the leader ID most recently recorded
// in LeaderState — NOT the raw raft.Status().Lead. Tests wait on this
// value specifically because it's the baseline the fail-fast watcher
// compares against: calling SetLeaderID(X) when ObservedLeader is
// already X is a no-op (no transition), while calling it when
// ObservedLeader is some earlier Y produces a Y→X transition that
// drives the at-risk sweep. Raft.Leader() can lead LeaderState by
// one Ready iteration, so waiting on it instead races the watcher.
func (db *DB) ObservedLeaderForTest() uint64 {
	return db.leaderState.Leader()
}

// RegisterWaiterForTest installs a waiter record keyed by requestID,
// stamped with stampedLeader. Returns the ack channel so the test can
// block on it and assert what the watcher / notifyApplied path
// eventually sends. Tests use this to exercise watcher logic without
// going through the full Propose → raft → apply cycle, which is
// timing-sensitive at the tick intervals the suite uses.
func (db *DB) RegisterWaiterForTest(requestID uint64, stampedLeader uint64) <-chan error {
	w := &waiter{
		ack:      make(chan error, 1),
		leaderID: stampedLeader,
	}
	db.waitersMu.Lock()
	db.waiters[requestID] = w
	db.waitersMu.Unlock()
	return w.ack
}

// UnregisterWaiterForTest removes a waiter installed via
// RegisterWaiterForTest. Tests that don't go through Propose must
// clean up manually so leftover waiters don't leak into subsequent
// transition sweeps on the same DB.
func (db *DB) UnregisterWaiterForTest(requestID uint64) {
	db.waitersMu.Lock()
	delete(db.waiters, requestID)
	db.waitersMu.Unlock()
}

// SignalWaiterForTest looks up the waiter by requestID and attempts a
// non-blocking send of err on its ack channel, exactly as
// notifyApplied would on a successful apply. Used to simulate the
// apply path winning the race against the watcher's grace timer.
func (db *DB) SignalWaiterForTest(requestID uint64, err error) {
	db.waitersMu.Lock()
	w, ok := db.waiters[requestID]
	db.waitersMu.Unlock()
	if !ok {
		return
	}
	select {
	case w.ack <- err:
	default:
	}
}

// WaitersForTest returns a snapshot of currently registered waiter
// RequestIDs. Useful for verifying that freeze-drain / Propose defer
// cleanup paths haven't leaked waiter map entries after whatever
// scenario the test is exercising.
func (db *DB) WaitersForTest() []uint64 {
	db.waitersMu.Lock()
	defer db.waitersMu.Unlock()
	ids := make([]uint64, 0, len(db.waiters))
	for id := range db.waiters {
		ids = append(ids, id)
	}
	return ids
}

// DrainInflightBumpsForTest exposes inflightBumps.drain so tests can
// exercise the applied / lost / timeout paths without standing up
// a full ingest worker. Callers construct the ack map by registering
// real waiters via RegisterWaiterForTest, optionally signaling them
// via SignalWaiterForTest, and passing the returned ack channels
// here. After drain returns, every waiter in the map is unregistered
// (leak-prevention cleanup), matching the behavior of the worker's
// real freeze-exit path.
func (db *DB) DrainInflightBumpsForTest(acks map[uint64]<-chan error, timeout time.Duration, id string) {
	b := &inflightBumps{acks: acks}
	b.drain(context.TODO(), db, timeout, db.logger, id)
}

// WaitForAnyWaiterForTest polls db.waiters until any waiter is
// registered and returns its RequestID, or 0 on timeout. Used by the
// ingest fail-fast test to synchronize on "the worker has called
// db.Propose and registered its waiter" before injecting an
// ErrProposalUnknown signal — since the test can't easily predict the
// auto-assigned RequestID. The poll uses a short sleep so the typical
// wait is a handful of microseconds on a warm goroutine schedule.
func (db *DB) WaitForAnyWaiterForTest(timeout time.Duration) uint64 {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		db.waitersMu.Lock()
		for id := range db.waiters {
			db.waitersMu.Unlock()
			return id
		}
		db.waitersMu.Unlock()
		time.Sleep(500 * time.Microsecond)
	}
	return 0
}

// RecordFreezeAndNextBackoffForTest exposes the supervisor's internal
// freeze-accounting so unit tests can exercise the backoff + giveup
// decision without standing up the full ingest-worker stack. Each call
// models one freeze observation: the returned backoff is the wait the
// supervisor would apply before its next restart attempt; consecutive
// is the post-increment freeze count for this id; giveup is true once
// the count exceeds the configured max-attempts cap.
func (db *DB) RecordFreezeAndNextBackoffForTest(id string) (backoff time.Duration, consecutive int, giveup bool) {
	return db.recordFreezeAndNextBackoff(id)
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
