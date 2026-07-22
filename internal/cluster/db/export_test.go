package db

import (
	"context"
	"sync"
	"time"

	"go.etcd.io/raft/v3"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// MemberVersionForTest reports the feature level node id announced (and whether
// one is known), reading the same replicated bucket the gate does. Lets a test
// observe that the startup announce landed.
func (db *DB) MemberVersionForTest(id uint64) (uint64, bool) {
	return db.storage.MemberVersion(id)
}

// InjectStaleWorkerBumpOnRebuildResetForTest arms RebuildSyncable's post-reset
// seam to reproduce the checkpoint-bump-vs-reset race deterministically. The
// first time a rebuild resets id's checkpoint, if a sync worker for id is still
// live at that instant (i.e. the rebuild has not yet stopped it), it injects
// exactly one stale checkpoint bump to staleIndex — the bump an in-flight worker
// would land just after the reset. If the rebuild has already stopped the worker,
// the injection is skipped, faithfully mirroring reality: a stopped worker cannot
// bump. Picking staleIndex >= the replay head makes the re-applied worker seed
// past all history, so a defeated reset shows up as "no replay happened".
func (db *DB) InjectStaleWorkerBumpOnRebuildResetForTest(id string, staleIndex uint64) {
	var once sync.Once
	db.afterRebuildCheckpointReset = func() {
		once.Do(func() {
			db.workersMu.Lock()
			_, live := db.syncWorkers[id]
			db.workersMu.Unlock()
			if !live {
				return // worker already stopped — a real worker could not bump here
			}
			_ = db.proposeSyncableIndex(context.Background(), &cluster.SyncableIndex{ID: id, Index: staleIndex})
		})
	}
}

// SetWorkerDrainTimeoutForTest overrides the bound the listener-path worker
// handoffs (Sync/Ingest replace, deleteSync/deleteIngest) wait for a cancelled
// worker before abandoning it, so a wedged-worker test runs fast.
func (db *DB) SetWorkerDrainTimeoutForTest(d time.Duration) { db.workerDrainTimeout = d }

// InjectWedgedSyncWorkerForTest registers a sync worker handle whose done
// channel never closes — modelling a worker stuck in tx.Commit against an
// unreachable destination that ignores its cancelled context. deleteSync/replace
// must abandon it within workerDrainTimeout rather than block the listener (and
// thus the raft apply loop) forever.
func (db *DB) InjectWedgedSyncWorkerForTest(id string) {
	db.workersMu.Lock()
	db.syncWorkers[id] = &workerHandle{cancel: func() {}, done: make(chan struct{})}
	db.workersMu.Unlock()
}

// DeleteSyncForTest drives the apply-path syncable teardown directly
// (keepData=false — the teardown-exercising shape the wedge tests need).
func (db *DB) DeleteSyncForTest(id string) { db.deleteSync(id, false) }

// SetDeadLetterProposeHookForTest installs the dead-letter propose
// failure-injection seam: a non-nil error from the hook stands in for an
// orphaned propose (leader flap) without touching raft.
func (db *DB) SetDeadLetterProposeHookForTest(h func(*cluster.SyncableDeadLetter) error) {
	db.deadLetterProposeHookForTest = h
}

// SetAfterRebuildCheckpointResetForTest installs the rebuild reset seam
// directly, so a test can assert the checkpoint reset did (or did not) run.
func (db *DB) SetAfterRebuildCheckpointResetForTest(f func()) { db.afterRebuildCheckpointReset = f }

// InjectDrainedSyncWorkerForTest registers a sync worker handle whose done
// channel is already closed (a cleanly-drained worker) carrying the given
// syncable — so a delete proceeds past the drain into the Close/Teardown legs.
// Used by the bounded-teardown/close tests, whose syncables block there.
func (db *DB) InjectDrainedSyncWorkerForTest(id string, s cluster.Syncable) {
	done := make(chan struct{})
	close(done)
	db.workersMu.Lock()
	db.syncWorkers[id] = &workerHandle{cancel: func() {}, done: done, syncable: s}
	db.workersMu.Unlock()
}

// IsLeaderForTest reports whether this node currently believes it is leader —
// the gate deleteSync's owner check (isNode) resolves through when the config
// is already gone. Tests wait on it before driving an owner-only teardown.
func (db *DB) IsLeaderForTest() bool { return db.leaderState.IsLeader() }

// NextRequestIDForTest draws the next RequestID exactly as proposeAsync would,
// so a test can observe the per-process seed (randomRequestIDBase) without
// standing up the full propose→raft→apply path.
func (db *DB) NextRequestIDForTest() uint64 {
	return db.nextRequestID.Add(1)
}

// SetTestTransportFactory installs the package-default TransportFactory the db
// test suite uses, so its many db.New / NewRaft call sites don't each have to
// wire a transport. The test harness calls this once from an init with the real
// HTTP transport factory. Production never calls it — cmd passes
// WithTransportFactory explicitly.
func SetTestTransportFactory(f TransportFactory) {
	pkgDefaultTransportFactory = f
}

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

// MembersForTest returns this Raft's current membership as observed locally:
// members is the union of voters (incoming + mid-joint outgoing) and learners,
// and joint reports whether the configuration is still in the joint state.
// Joint-consensus membership tests poll this to watch a change move through the
// joint configuration and settle into its final form; the union keeps existing
// voter-only assertions working unchanged. Use MemberRolesForTest when a test
// needs to distinguish voters from learners.
func (n *Raft) MembersForTest() (members map[uint64]struct{}, joint bool) {
	if n.node == nil {
		return nil, false
	}
	voters, learners, joint := n.memberStatus()
	for id := range learners {
		voters[id] = struct{}{}
	}
	return voters, joint
}

// MemberRolesForTest returns this Raft's voter set, learner set, and joint
// flag separately, so learner tests can assert that a node is a learner (not a
// voter) and that promotion moves it from one set to the other.
func (n *Raft) MemberRolesForTest() (voters, learners map[uint64]struct{}, joint bool) {
	if n.node == nil {
		return nil, nil, false
	}
	return n.memberStatus()
}

// CommitIndexForTest returns this Raft's current commit index as reported
// by etcd raft's Status snapshot. Used by adversarial tests to assert that
// a minority-side leader does not advance commit while partitioned (safety
// invariant: no quorum → no new commits).
func (n *Raft) CommitIndexForTest() uint64 {
	if n.node == nil {
		return 0
	}
	return n.node.Status().GetCommit()
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

// SetCompactionPressureForTest drives the disk-pressure compaction hint so a
// test can assert maybeCompact fires on pressure alone (size/age limbs
// disabled). Mirrors what db.onDiskState does in production.
func (n *Raft) SetCompactionPressureForTest(on bool) {
	n.setCompactionPressure(on)
}

// SetDiskStateForTest forces the DB's disk-pressure state, exactly as the
// disk-usage watcher would on a sample, so propose-gate tests don't need a
// real low-disk filesystem. level is "ok", "warn", "critical", or "full".
// Drives onDiskState so the compaction-pressure hint is exercised too.
func (db *DB) SetDiskStateForTest(level string) {
	var s diskState
	switch level {
	case "warn":
		s = diskWarn
	case "critical":
		s = diskCritical
	case "full":
		s = diskFull
	default:
		s = diskOK
	}
	db.onDiskState(s)
}

// ProposeIngestDataForTest exposes the ingest worker's disk-pressure-aware
// propose helper so a test can assert it pauses (retries) under disk pressure
// and resumes on recovery without standing up a full ingest worker + upstream.
func (db *DB) ProposeIngestDataForTest(ctx context.Context, p *cluster.Proposal) error {
	return db.proposeIngestData(ctx, p)
}

// ProposeSyncableIndexForTest exposes the sync worker's index-bump propose
// (a "checkpoint"-kind proposal) so a test can assert checkpoints still commit
// while the disk gate is at full.
func (db *DB) ProposeSyncableIndexForTest(ctx context.Context, id string, index uint64) error {
	return db.proposeSyncableIndex(ctx, &cluster.SyncableIndex{ID: id, Index: index})
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

// NotifyLostForTest invokes the truncation lost-callback handler
// (db.notifyLost) directly with the given RequestIDs, exactly as
// wal.Storage.appendEntries does after truncating uncommitted entries.
// Lets the propose-truncation tests assert ErrProposalLost delivery
// without standing up a multi-node cluster and forcing a real log
// conflict.
func (db *DB) NotifyLostForTest(requestIDs []uint64) {
	db.notifyLost(requestIDs)
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
func (db *DB) RecordFreezeAndNextBackoffForTest(id string, pos cluster.Position) (backoff time.Duration, consecutive int, giveup bool) {
	return db.recordFreezeAndNextBackoff(id, pos)
}

// SupervisorStateExistsForTest reports whether the supervisor still holds
// give-up bookkeeping for id — used to pin that a config delete prunes it
// (bounding the map and granting a recreated id a fresh budget).
func (db *DB) SupervisorStateExistsForTest(id string) bool {
	db.ingestSupervisorMu.Lock()
	defer db.ingestSupervisorMu.Unlock()
	_, ok := db.ingestSupervisorStates[id]
	return ok
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

// SetLocalDiskStateForTest stores the node-local disk-pressure level WITHOUT
// driving onDiskState — no compaction-pressure nudge, no verdict recompute,
// no coordinator nudge. Cluster-admission tests use it to set up "local says
// X but the cluster verdict says Y" divergence; use SetDiskStateForTest when
// the test wants the full watcher-callback behavior.
func (db *DB) SetLocalDiskStateForTest(level string) {
	s, ok := parseDiskState(level)
	if !ok {
		panic("unknown disk state " + level)
	}
	db.diskState.Store(int32(s))
}

// SetClusterVerdictForTest injects a cached cluster write-admission verdict
// as if the leader had returned it on a report round trip age ago. Lets
// gate tests exercise verdict-dominates-local and staleness-fallback without
// standing up a multi-node cluster.
func (db *DB) SetClusterVerdictForTest(state, reason string, leaderID uint64, age time.Duration) {
	s, ok := parseDiskState(state)
	if !ok {
		panic("unknown disk state " + state)
	}
	db.diskVerdict.Store(&diskVerdictState{
		state:    s,
		reason:   reason,
		leaderID: leaderID,
		at:       time.Now().Add(-age),
	})
}

// RecordDiskReportForTest seeds the leader-side report map directly, as if
// member nodeID had reported state age ago — so verdict and transfer-target
// tests can model member states without HTTP round trips.
func (db *DB) RecordDiskReportForTest(nodeID uint64, state string, age time.Duration) {
	s, ok := parseDiskState(state)
	if !ok {
		panic("unknown disk state " + state)
	}
	db.diskReportsMu.Lock()
	db.diskReports[nodeID] = diskReport{state: s, at: time.Now().Add(-age)}
	db.diskReportsMu.Unlock()
}

// DiskCoordinateForTest runs one disk-coordinator cycle synchronously (the
// same body the background loop runs on each tick), so tests can drive the
// leader recompute / follower report-and-cache paths deterministically.
func (db *DB) DiskCoordinateForTest() {
	db.diskCoordinate(time.Now())
}

// SetDiskTransferHooksForTest overrides the transfer-trigger collaborators:
// pick returns the candidate voter (0 = none) and transfer records the
// transfer call. cooldown rate-limits successive transfers. Returns nothing;
// drive the decision via MaybeTransferLeadershipForTest.
func (db *DB) SetDiskTransferHooksForTest(pick func() uint64, transfer func(uint64), cooldown time.Duration) {
	db.pickTransferTargetFn = func(time.Time) uint64 { return pick() }
	db.transferLeadershipFn = transfer
	db.diskTransferCooldown = cooldown
}

// MaybeTransferLeadershipForTest runs one disk-pressure transfer decision at
// the given instant, against the current local disk state and the hooks set
// via SetDiskTransferHooksForTest.
func (db *DB) MaybeTransferLeadershipForTest(now time.Time) {
	db.maybeTransferLeadership(now, db.diskVerdict.Load())
}

// SetShutdownTransferHooksForTest overrides the graceful-shutdown leadership
// transfer collaborators so a test can drive Close's hand-off without a live
// cluster: target returns the voter to hand off to (0 = none), transfer records
// the transfer call, isLeader reports whether leadership has moved off this node
// yet, and timeout bounds the wait.
func (db *DB) SetShutdownTransferHooksForTest(target func() uint64, transfer func(uint64), isLeader func() bool, timeout time.Duration) {
	db.shutdownTransferTargetFn = target
	db.transferLeadershipFn = transfer
	db.isLeaderFn = isLeader
	db.shutdownTransferTimeout = timeout
}

// TransferLeadershipForTest exposes the raft-level leadership hand-off so the
// multinode harness can assert the plumbing under disk-pressure transfers:
// etcd raft catches the target up, sends MsgTimeoutNow, and the target wins
// an immediate election.
func (n *Raft) TransferLeadershipForTest(transferee uint64) {
	n.transferLeadership(transferee)
}

// HTTPDiskReportSenderForTest builds the production HTTP report sender (the
// exact code the disk coordinator uses) so a test can post a report through
// a real HTTP handler and assert the db↔http wire contract stays in sync.
func HTTPDiskReportSenderForTest(token string) func(ctx context.Context, leaderURL string, nodeID uint64, state string) (cluster.DiskVerdict, error) {
	return newHTTPDiskReportSender(nil, token)
}

// WaiterCountForTest reports how many Propose waiters are currently
// registered — the pipelined ingest worker's in-flight window is directly
// observable as concurrent waiters (the synchronous worker never held more
// than one).
func (db *DB) WaiterCountForTest() int {
	db.waitersMu.Lock()
	defer db.waitersMu.Unlock()
	return len(db.waiters)
}

// WaiterIDsForTest snapshots the RequestIDs of all currently registered
// Propose waiters. Lets a test fail EVERY in-flight proposal (its own rows
// plus any internal announce proposals the harness parked) when it cannot
// distinguish which rid belongs to which proposer.
func (db *DB) WaiterIDsForTest() []uint64 {
	db.waitersMu.Lock()
	defer db.waitersMu.Unlock()
	ids := make([]uint64, 0, len(db.waiters))
	for rid := range db.waiters {
		ids = append(ids, rid)
	}
	return ids
}

// HasSyncWorkerForTest reports whether a sync worker is registered for id.
func (db *DB) HasSyncWorkerForTest(id string) bool {
	db.workersMu.Lock()
	defer db.workersMu.Unlock()
	_, ok := db.syncWorkers[id]
	return ok
}

// HasIngestWorkerForTest reports whether an ingest worker is registered for id.
func (db *DB) HasIngestWorkerForTest(id string) bool {
	db.workersMu.Lock()
	defer db.workersMu.Unlock()
	_, ok := db.ingestWorkers[id]
	return ok
}

// CancelIngestWorkerForTest drives the unexported cancelIngestWorker (the
// delete/reconcile front half) so a test can exercise its supervisor-race
// window directly, without wiring a full reconcile or delete proposal.
func (db *DB) CancelIngestWorkerForTest(id string) {
	db.cancelIngestWorker(id)
}

// SetBeforeCancelIngestRelockForTest installs the drain-window seam
// cancelIngestWorker calls after it drops workersMu and drains, before it
// relocks to delete the map entry — the window a racing supervisor restart
// must not resurrect a condemned handle in.
func (db *DB) SetBeforeCancelIngestRelockForTest(fn func()) {
	db.beforeCancelIngestRelockForTest = fn
}

// SetIngestSupervisorRaceSeamsForTest installs the two superviseRestartIngest
// seams that make the cancel-vs-supervisor resurrection race deterministic:
// beforePreflight fires after the backoff, just before the supervisor
// reacquires workersMu (a poise point); afterAttempt fires when the
// supervisor's restart goroutine exits, on every path.
func (db *DB) SetIngestSupervisorRaceSeamsForTest(beforePreflight, afterAttempt func()) {
	db.beforeIngestSupervisorRelockForTest = beforePreflight
	db.afterIngestSupervisorAttemptForTest = afterAttempt
}

// SetAfterIngestSupervisorRestartForTest installs the seam that fires after a
// supervisor restart, carrying the frozen handle's ctx.Err() — so a test can
// assert the restart cancelled it (no leaked context node).
func (db *DB) SetAfterIngestSupervisorRestartForTest(fn func(frozenCtxErr error)) {
	db.afterIngestSupervisorRestartForTest = fn
}
