package db

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/metrics"
)

// ErrClosed is returned by db.Sync, db.Ingest, and db.Propose when the
// DB has been (or is being) closed. Callers can use errors.Is(err,
// db.ErrClosed) to distinguish a normal shutdown from other failures.
var ErrClosed = errors.New("db: closed")

// ErrProposalUnknown is returned from db.Propose when a raft leader
// change is observed while the proposal is in flight. The proposal
// MAY have committed under the new leader (if it was replicated before
// the old leader stepped down) or it MAY have been silently truncated
// (if the new leader's log didn't inherit it). The caller has no way
// to tell locally, so the contract is deliberately pessimistic: safe
// to retry only if the operation is idempotent at the application
// level (e.g., protected by a caller-supplied dedup key or naturally
// idempotent state). Callers that can't establish idempotency should
// surface the error to the end user rather than blind-retry.
//
// Returned after a short grace period following the observed
// transition (see WithLeaderChangeGracePeriod); the grace lets the
// normal apply path win for quick-transition cases where the new
// leader inherits and commits the entry within a handful of ticks.
var ErrProposalUnknown = errors.New("db: proposal status unknown after leader change")

// ErrProposalLost is returned from db.Propose when the raft log entry
// carrying this proposal was physically truncated by a higher-term
// leader's AppendEntries before it committed. Unlike ErrProposalUnknown,
// this is a definitive signal: this specific proposal attempt did NOT
// commit and is gone from the log, so it is always safe to retry — no
// application-level idempotency reasoning required.
//
// It is the stronger of the two fail-fast signals and only fires on a
// node that physically held the truncated entry (the leader at proposal
// time, or a follower the old leader replicated to before stepping
// down). A waiter on a node that never held the entry sees the
// conservative ErrProposalUnknown from the leader-change watcher instead;
// truncation detection upgrades the signal where physical evidence
// exists, it does not replace the leader-change path. The two can race on
// the same waiter — both mean "retry", and Lost simply wins when it
// arrives.
var ErrProposalLost = errors.New("db: proposal truncated before commit")

type Peers map[uint64]string

type DB struct {
	ErrorC      <-chan error
	proposeC    chan<- []byte
	confChangeC chan<- *raftpb.ConfChangeV2
	raft        *Raft
	storage     Storage
	ctx         context.Context
	cancelSyncs context.CancelFunc
	parser      Parser
	leaderState *LeaderState

	// waiters maps request IDs (set in db.Propose) to waiter records
	// whose ack channel receives nil after the proposal is applied.
	// The raft Ready loop dispatches to notifyApplied after each
	// successful ApplyCommitted, which looks up the waiter by
	// RequestID and signals it. db.Propose blocks on the waiter (or
	// ctx.Done) so callers see read-after-write.
	//
	// Each waiter carries the leader ID observed at submission time
	// (leaderState.Leader()). The leader-change watcher uses that
	// stamp to identify waiters at risk of having been orphaned after
	// a leader transition and signals them with ErrProposalUnknown
	// after a grace period if apply hasn't landed in the meantime.
	waitersMu     sync.Mutex
	waiters       map[uint64]*waiter
	nextRequestID atomic.Uint64

	// appliedNotifyCh is a broadcast channel for "AppliedIndex advanced".
	// LinearizableRead, after raft confirms a ReadIndex, blocks until the
	// local AppliedIndex catches up to that index; it waits on this
	// channel rather than polling. The raft Ready loop closes and replaces
	// it (notifyAppliedIndexAdvanced) once per iteration that applied any
	// committed entry, waking every waiter. Guarded by appliedMu.
	appliedMu       sync.Mutex
	appliedNotifyCh chan struct{}

	// leaderChangeGrace is how long the leader-change watcher waits
	// between observing a transition and signaling at-risk waiters.
	// Populated from options (default 3× tickInterval; tests override
	// via WithLeaderChangeGracePeriod).
	leaderChangeGrace time.Duration

	// workersMu guards syncWorkers / ingestWorkers / closed. The two
	// maps key running per-ID worker goroutines (one per syncable /
	// ingestable ID), so that a second db.Sync / db.Ingest call for
	// the same ID cancels and replaces the existing worker instead of
	// spawning a duplicate that would race with the first over the
	// same Reader, Position, and proposeC slot. db.Close cancels every
	// entry and waits for the workers' done channels.
	//
	// closed is set to true by db.Close after it has drained the
	// registry. db.Sync / db.Ingest check it under workersMu and
	// reject installs with ErrClosed when set, so a late caller (e.g.,
	// listenForSyncables waking up after the drain has run) can't
	// spawn an unobserved worker that escapes the drain. Without this
	// flag, the spawn-vs-Close race produced a brief leak window
	// where a goroutine could outlive Close by however long it took
	// to observe db.ctx.Done() on its own.
	workersMu     sync.Mutex
	syncWorkers   map[string]*workerHandle
	ingestWorkers map[string]*workerHandle
	// syncDeleteKeep records, per syncable ID, that an operator asked DELETE to
	// keep the destination state (keepData). DeleteSyncable sets it before
	// proposing; deleteSync reads-and-clears it on apply. Guarded by workersMu.
	syncDeleteKeep map[string]bool
	closed         bool

	// ingestSupervisorMu guards ingestSupervisorStates. Deliberately
	// separate from workersMu so the supervisor's bookkeeping doesn't
	// contend with the hot worker-registry path.
	ingestSupervisorMu     sync.Mutex
	ingestSupervisorStates map[string]*ingestSupervisorState

	// syncBreakerMu guards syncBreakerStates, the per-syncable run of
	// consecutive permanent sync errors (within a healthy window) driving the
	// sync circuit breaker (see sync_breaker.go). Separate from workersMu to
	// stay off the hot worker-registry path.
	syncBreakerMu     sync.Mutex
	syncBreakerStates map[string]*syncBreakerState

	// Cached supervisor tuning; resolved in New from options/defaults so
	// the freeze-restart hot path reads a struct field rather than
	// dereferencing the options config on each call.
	ingestSupervisorInitialBackoff time.Duration
	ingestSupervisorMaxBackoff     time.Duration
	ingestSupervisorMaxAttempts    int
	ingestSupervisorHealthyWindow  time.Duration

	// syncStuckThreshold is how long a worker must be continuously blocked
	// retrying a transient error before it publishes a replicated
	// SyncableStuck record (and lights the stuck gauge). Debounced so normal
	// fast-recovering transients never flag; only a genuine wedge surfaces.
	// Default 30s; tests override via WithSyncStuckThreshold.
	syncStuckThreshold time.Duration

	// scrubInterval is the automatic-scrub cadence (0 disables the scheduler).
	// scrubScheduler ticks at this rate and, when leader with RTBF backlog,
	// proposes a Scrub command. See scrubScheduler / maybeProposeScrub.
	scrubInterval time.Duration

	// advertisedAPIURL is this node's advertised HTTP API base URL, announced
	// into the replicated memberAPIURLs map by announceAPIURL. Empty disables
	// the announce. announceInterval is the poll cadence that goroutine uses
	// while waiting for this node to become a member with a reachable leader
	// (derived from tickInterval at New time). See raft-leader-read-proxy.md.
	advertisedAPIURL string
	announceInterval time.Duration

	maxProposalBytes uint64

	// diskState is the current disk-pressure level (a diskState value),
	// published by the disk-usage watcher goroutine and read by the propose
	// gate (checkDiskWritable). Stored as an int32 so it can be updated
	// atomically without a lock on the hot propose path. Stays diskOK when
	// no watcher is configured, so the gate is a no-op.
	diskState atomic.Int32

	// Cluster-aware admission (see disk_cluster.go). diskVerdict is the
	// cached cluster write-admission verdict the propose gate enforces
	// (atomic pointer: the gate reads it lock-free on the hot path; nil or
	// stale falls back to the node-local diskState). diskReports is the
	// leader-side map of member disk reports, guarded by diskReportsMu.
	// diskNudgeC wakes the coordinator loop early on a local disk-state
	// change. lastDiskTransfer rate-limits disk-pressure leadership
	// transfers; it is touched only from coordinator cycles.
	diskVerdict          atomic.Pointer[diskVerdictState]
	diskReportsMu        sync.Mutex
	diskReports          map[uint64]diskReport
	diskNudgeC           chan struct{}
	diskReportInterval   time.Duration
	diskTransferCooldown time.Duration
	lastDiskTransfer     time.Time
	diskReportSend       diskReportSender
	transferLeadershipFn func(transferee uint64)
	pickTransferTargetFn func(now time.Time) uint64

	// Graceful-shutdown leadership transfer (see shutdown.go). Injected as
	// fields, like the disk-pressure transfer collaborators above, so the
	// target choice and the wait-for-handoff loop are unit-testable without a
	// live cluster.
	shutdownTransferTargetFn func() uint64
	isLeaderFn               func() bool
	shutdownTransferTimeout  time.Duration

	// afterRebuildCheckpointReset is a test-only seam invoked by RebuildSyncable
	// immediately after the checkpoint-reset proposal applies (nil in
	// production, so zero overhead). It lets a test deterministically inject the
	// checkpoint-bump-vs-reset race that RebuildSyncable's ordering is
	// responsible for closing — see rebuildStopWorkerLocal and
	// TestRebuildSyncable_StaleWorkerBumpDoesNotDefeatReset.
	afterRebuildCheckpointReset func()

	// syncCh / ingestCh are the config-notification channels the apply path
	// (wal.Storage) sends on and listenForSyncables/Ingestables receive from.
	// Close keeps a drain goroutine reading them while it stops raft, so a
	// config entity applied during shutdown can't block the apply loop's send
	// (with the listeners already gone) and hang raft.Close.
	syncCh   <-chan *SyncableWithID
	ingestCh <-chan *IngestableWithID

	// workerDrainTimeout bounds how long the listener-path worker handoffs
	// (Sync/Ingest replace, deleteSync/deleteIngest) wait for a cancelled worker
	// to exit before abandoning it. Unbounded, a worker wedged in tx.Commit
	// against an unreachable destination would park the single-threaded listener
	// and stall the raft apply loop on its next config send. Defaults to
	// closeDrainTimeout; tests override it to keep the wedged-worker cases fast.
	workerDrainTimeout time.Duration

	logger  *zap.Logger
	metrics *metrics.Metrics
}

// workerHandle is the registry entry for a per-ID Sync or Ingest
// goroutine. cancel terminates the worker's context; done is closed
// by the worker itself just before it returns. Replace and Close
// both wait on done so they can guarantee the previous worker has
// fully exited (released its Reader, finished any in-flight Propose,
// returned from the user-supplied Sync/Ingest callback) before
// proceeding.
type workerHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
	// syncable is the parsed Syncable this worker runs, retained so the delete
	// path can reuse it as a teardown handle (e.g. DROP TABLE for a SQL
	// syncable) without re-parsing the config — which would re-run Init. It is
	// nil for ingest workers (deleteSync only reads it for syncables).
	syncable cluster.Syncable
	// ingestable is the parsed Ingestable this worker runs, retained so the
	// status path can ask it to decode its persisted position and query source
	// lag (IngestableStatus) without re-parsing. It is nil for syncable workers.
	ingestable cluster.Ingestable
}

// waiter is the per-request state used by blocking db.Propose. ack is
// a buffered (capacity 1) channel that carries either nil on apply or
// ErrProposalUnknown when the leader-change watcher decides the
// proposal is likely orphaned. leaderID is the leader observed at
// submission time — stamped so the watcher can tell which waiters are
// at risk of orphaning after a transition (any waiter whose stamp
// differs from the new leader).
type waiter struct {
	ack      chan error
	leaderID uint64
}

func New(id uint64, peers Peers, s Storage, p Parser, sync <-chan *SyncableWithID, ingest <-chan *IngestableWithID, opts ...Option) *DB {
	cfg := defaultOptions()
	for _, opt := range opts {
		opt(&cfg)
	}

	// Default grace to 3× tickInterval when the caller didn't pin it
	// explicitly. We defer this until after options are applied so the
	// grace scales with the tick override — a test that drops
	// tickInterval to 1ms gets a 3ms grace for free (still large
	// enough that apply can win in the fast case but short enough to
	// keep test runs tight).
	if cfg.leaderChangeGrace == 0 {
		cfg.leaderChangeGrace = 3 * cfg.tickInterval
	}

	if cfg.ingestSupervisorInitialBackoff == 0 {
		cfg.ingestSupervisorInitialBackoff = defaultIngestSupervisorInitialBackoff
	}
	if cfg.ingestSupervisorMaxBackoff == 0 {
		cfg.ingestSupervisorMaxBackoff = defaultIngestSupervisorMaxBackoff
	}
	if cfg.ingestSupervisorMaxAttempts == 0 {
		cfg.ingestSupervisorMaxAttempts = defaultIngestSupervisorMaxAttempts
	}
	if cfg.ingestSupervisorHealthyWindow == 0 {
		cfg.ingestSupervisorHealthyWindow = defaultIngestSupervisorHealthyWindow
	}
	if cfg.maxProposalBytes == 0 {
		cfg.maxProposalBytes = DefaultMaxProposalBytes
	}

	proposeC := make(chan []byte)
	confChangeC := make(chan *raftpb.ConfChangeV2)

	rpeers := make([]raft.Peer, len(peers))
	i := 0
	for k, v := range peers {
		rpeers[i] = raft.Peer{ID: k, Context: []byte(v)}
		i++
	}

	//nolint:gosec // G118: cancelSyncs is stored in db.cancelSyncs and called by Close.
	ctx, cancelSyncs := context.WithCancel(context.Background())

	db := &DB{
		proposeC:                       proposeC,
		confChangeC:                    confChangeC,
		storage:                        s,
		ctx:                            ctx,
		cancelSyncs:                    cancelSyncs,
		parser:                         p,
		waiters:                        make(map[uint64]*waiter),
		appliedNotifyCh:                make(chan struct{}),
		leaderChangeGrace:              cfg.leaderChangeGrace,
		syncWorkers:                    make(map[string]*workerHandle),
		ingestWorkers:                  make(map[string]*workerHandle),
		syncDeleteKeep:                 make(map[string]bool),
		syncStuckThreshold:             cfg.syncStuckThreshold,
		scrubInterval:                  cfg.scrubInterval,
		advertisedAPIURL:               cfg.advertisedAPIURL,
		announceInterval:               cfg.tickInterval,
		ingestSupervisorStates:         make(map[string]*ingestSupervisorState),
		ingestSupervisorInitialBackoff: cfg.ingestSupervisorInitialBackoff,
		ingestSupervisorMaxBackoff:     cfg.ingestSupervisorMaxBackoff,
		ingestSupervisorMaxAttempts:    cfg.ingestSupervisorMaxAttempts,
		ingestSupervisorHealthyWindow:  cfg.ingestSupervisorHealthyWindow,
		maxProposalBytes:               cfg.maxProposalBytes,
		diskReports:                    make(map[uint64]diskReport),
		diskNudgeC:                     make(chan struct{}, 1),
		diskReportInterval:             cfg.diskReportInterval,
		diskTransferCooldown:           cfg.diskTransferCooldown,
		diskReportSend:                 cfg.diskReportSender,
		logger:                         cfg.logger,
		metrics:                        cfg.metrics,
	}
	// Seed the RequestID counter at a random point in the top half of the uint64
	// space so each process lifetime's contiguous block of ids is disjoint from
	// every other process's — see randomRequestIDBase and notifyApplied. Set
	// before any proposal (the raft/goroutine wiring below) can draw an id.
	db.nextRequestID.Store(randomRequestIDBase())

	db.syncCh = sync
	db.ingestCh = ingest
	db.workerDrainTimeout = closeDrainTimeout

	if db.diskReportInterval == 0 {
		db.diskReportInterval = DefaultDiskReportInterval
	}
	if db.diskTransferCooldown == 0 {
		db.diskTransferCooldown = defaultDiskTransferCooldown
	}
	if db.diskReportSend == nil {
		db.diskReportSend = newHTTPDiskReportSender(cfg.diskReportClient, cfg.diskReportToken)
	}

	// Three hooks are wired into the raft layer. The per-entry applied
	// notifier (db.notifyApplied) receives each committed entry's bytes so
	// we can unmarshal once, look up the blocking-Propose waiter by
	// RequestID, and signal it. The per-Ready applied-index notifier
	// (db.notifyAppliedIndexAdvanced) fires once whenever AppliedIndex
	// advanced, waking LinearizableRead callers blocked on apply catch-up.
	// The first two work uniformly for wal.Storage (real apply) and the
	// in-memory test double — both go through the same raft.go iteration
	// over rd.CommittedEntries. The third, db.notifyLost, is installed on
	// the storage (not called from the Ready loop): wal.Storage fires it
	// from appendEntries when a higher-term leader truncates uncommitted
	// entries this node held, so their waiters get ErrProposalLost.
	errorC, raft := newRaftWithOptions(id, rpeers, s, proposeC, confChangeC, db.notifyApplied, db.notifyAppliedIndexAdvanced, db.notifyLost, cfg.logger, cfg)

	db.ErrorC = errorC
	db.raft = raft
	db.leaderState = raft.leaderState

	// The watcher subscribes to leader-ID transitions from LeaderState
	// BEFORE the first Ready iteration could land — subscribe() just
	// registers a channel, so events that arrive before we start
	// reading are buffered until the watcher goroutine catches up.
	// Buffer sized generously (64) so a burst of flaps during a slow
	// schedule doesn't cause us to silently drop transitions.
	go db.watchLeaderTransitions(db.leaderState.Subscribe(64))

	go db.listenForSyncables(sync)
	go db.listenForIngestables(ingest)
	go db.scrubScheduler()
	go db.announceAPIURL()
	if cfg.announceVersion {
		go db.announceVersion()
	}

	// Start the disk-usage watcher last, once db.raft is wired, so its
	// onDiskState callback can drive the compaction-pressure hint. An empty
	// Path leaves it disabled and diskState pinned at diskOK (gate is a
	// no-op). Stopped by db.ctx cancellation in db.Close.
	if cfg.diskWatcher.Path != "" {
		w := newDiskWatcher(cfg.diskWatcher, db.onDiskState, cfg.logger, cfg.metrics)
		go w.run(db.ctx)
	}

	// Cluster-aware disk admission (disk_cluster.go). The coordinator runs
	// even with no local watcher: this node's own state stays pinned at ok,
	// but its propose gate still needs the leader's verdict so it won't
	// forward writes into a cluster that can't safely take them. A negative
	// interval (WithDiskReportInterval(<=0)) disables it.
	db.transferLeadershipFn = db.raft.transferLeadership
	db.pickTransferTargetFn = db.pickTransferTarget
	db.shutdownTransferTargetFn = db.shutdownTransferTarget
	db.isLeaderFn = db.isLeader
	db.shutdownTransferTimeout = defaultShutdownTransferTimeout
	if db.diskReportInterval > 0 {
		go db.diskCoordinator()
	}

	return db
}

// onDiskState is the disk-usage watcher's callback, invoked on every change in
// disk-pressure level. It publishes the new state for the propose gate to read
// and nudges the raft compaction-pressure hint so the node frees raft-log disk
// sooner while space is critical/full. On the leader it then synchronously
// recomputes the cluster admission verdict (so the gate never enforces a
// verdict older than the state just published); on a follower it nudges the
// disk coordinator to report the transition to the leader right away. Called
// from the watcher goroutine (and tests via SetDiskStateForTest).
func (db *DB) onDiskState(s diskState) {
	db.diskState.Store(int32(s))
	if db.raft != nil {
		db.raft.setCompactionPressure(s >= diskCritical)
	}
	if db.leaderState != nil && db.leaderState.IsLeader() {
		db.recomputeDiskVerdict(time.Now())
		return
	}
	db.nudgeDiskCoordinator()
}

// listenForSyncables forwards syncable registrations from the input
// channel to db.Sync. It exits cleanly when db.ctx is canceled (db.Close)
// or when the input channel is closed. The previous "for sync != nil"
// shape was a pre-PR3 typo: a non-nil channel reference never becomes
// nil, so the loop ran forever and the goroutine leaked across Close.
// PR3's tighter shutdown semantics surfaced the leak as a race against
// db.Sync's closed-flag check, so we fix the listener to terminate
// properly here.
//
// nil input channels are handled explicitly so test setups that pass
// nil don't waste a goroutine waiting on db.ctx.
func (db *DB) listenForSyncables(sync <-chan *SyncableWithID) {
	if sync == nil {
		return
	}
	for {
		select {
		case syncable, ok := <-sync:
			if !ok {
				return
			}
			if syncable.Delete {
				db.deleteSync(syncable.ID)
				continue
			}
			if err := db.Sync(context.Background(), syncable.ID, syncable.Syncable); err != nil {
				// Sync only returns ErrClosed, which means db.Close
				// fired between our channel receive and the registry
				// check. Exit the listener (same as <-db.ctx.Done).
				db.logger.Debug("listenForSyncables exiting",
					zap.String("id", syncable.ID), zap.Error(err))
				return
			}
		case <-db.ctx.Done():
			return
		}
	}
}

func (db *DB) listenForIngestables(ingest <-chan *IngestableWithID) {
	if ingest == nil {
		return
	}
	for {
		select {
		case ingestable, ok := <-ingest:
			if !ok {
				return
			}
			if ingestable.Delete {
				db.deleteIngest(ingestable.ID)
				continue
			}
			if err := db.Ingest(context.Background(), ingestable.ID, ingestable.Ingestable); err != nil {
				// Ingest only returns ErrClosed, which means db.Close
				// fired between our channel receive and the registry
				// check. Exit the listener (same as <-db.ctx.Done).
				db.logger.Debug("listenForIngestables exiting",
					zap.String("id", ingestable.ID), zap.Error(err))
				return
			}
		case <-db.ctx.Done():
			return
		}
	}
}

// Propose submits a proposal to raft and blocks until it has been applied
// to bucket state on this node, or until ctx is canceled. Callers that
// need read-after-write semantics (HTTP handlers chaining "create type,
// then immediately use it") get them for free: by the time Propose
// returns nil, db.storage.Type/Database/etc. will see the new entity.
//
// On ctx cancellation Propose returns ctx.Err() and the waiter is
// unregistered. The proposal may still be applied later (raft has
// already accepted it via the proposeC send) — the caller just no
// longer waits.
//
// System-internal proposers that want to pipeline submissions (ingest
// position bumps, which collect acks for later drain) use proposeAsync
// directly and manage the ack lifecycle themselves.
func (db *DB) Propose(ctx context.Context, p *cluster.Proposal) error {
	start := time.Now()
	rid, ack, err := db.proposeAsync(ctx, p)
	if err != nil {
		return err
	}
	defer db.unregisterWaiter(rid)

	select {
	case err := <-ack:
		// Only record "applied" duration on a successful apply.
		// ErrProposalUnknown from the leader-change watcher isn't an
		// apply, so counting it in the histogram would distort the
		// p99/p999 shape (it always records around the grace period).
		if err == nil && db.metrics != nil {
			db.metrics.ProposalApplied(time.Since(start))
		}
		db.logger.Debug("proposal completed", zap.Uint64("requestID", rid), zap.Error(err))
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-db.ctx.Done():
		return db.ctx.Err()
	}
}

// randomRequestIDBase returns a per-process starting point for RequestID
// assignment, in the top half of the uint64 space (bit 63 set). New seeds
// nextRequestID with it and proposeAsync increments from there, so each process
// lifetime consumes a contiguous block of ids at an independent random offset —
// disjoint from every other lifetime's block with overwhelming probability.
//
// That is what keeps notifyApplied from matching a waiter THIS process
// registered against a proposal from a DIFFERENT process that carries the same
// RequestID: a replayed committed/uncommitted tail entry after a restart (the
// counter used to reset to 0 each start), or a concurrently-forwarded proposal
// from another node (each node's counter is independent). Both are the false-ACK
// root cause; a per-process-random block removes the collision rather than
// patching one path to it.
//
// The base is confined to [2^62, 2^63): bit 62 set keeps every assigned id far
// above 0 (the notifyApplied "no waiter" sentinel) and above the small ids in any
// pre-upgrade log (so a replayed old id can't match a new one); bit 63 clear
// leaves ~2^63 of headroom above the block so the counter can never wrap back
// toward 0/small, even over an absurdly long process lifetime.
func randomRequestIDBase() uint64 {
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		// crypto/rand is backed by the OS CSPRNG and does not fail on a supported
		// platform. A failure means no entropy is available, which we can't safely
		// start without: a fixed fallback base would reintroduce the cross-process
		// collision this seed exists to prevent.
		panic("db: crypto/rand failed generating a RequestID base: " + err.Error())
	}
	return (binary.LittleEndian.Uint64(b[:]) &^ (uint64(1) << 63)) | (uint64(1) << 62)
}

// proposeAsync is the unified submit primitive. It assigns a
// RequestID, registers a waiter stamped under the current leader,
// marshals, and sends on proposeC. On successful submit it returns
// (requestID, ack, nil); callers MUST either read the ack (once) AND
// call unregisterWaiter(rid), OR delegate both to the Propose wrapper.
// The ack channel is buffered cap=1, so notifyApplied and the
// leader-change watcher can each send without blocking; the first
// signal wins, subsequent signals fall through the default branch in
// their respective send sites.
//
// On submit failure (ctx or db.ctx canceled before the proposeC
// send completes, or Marshal fails), proposeAsync unregisters the
// waiter itself and returns a zero RequestID + nil ack + error.
//
// proposeAsync is the shared primitive beneath db.Propose; every
// caller (user/config proposals, sync's index bump, ingest's position
// bump) goes through the blocking Propose wrapper, so none use the raw
// ack form directly today. It stays factored out because Propose's
// select-on-ack logic is cleaner expressed against it, and because a
// future pipelined caller would build on it again.
func (db *DB) proposeAsync(ctx context.Context, p *cluster.Proposal) (uint64, <-chan error, error) {
	kind := proposalKind(p)

	// Disk-pressure gate, checked before we assign a RequestID or register a
	// waiter so a rejected proposal consumes nothing. At critical only
	// user-data proposals are rejected; at full everything is. Returns the
	// typed cluster.ErrInsufficientStorage the HTTP layer maps to 507.
	if err := db.checkDiskWritable(kind); err != nil {
		return 0, nil, err
	}

	p.RequestID = db.nextRequestID.Add(1)

	// Stamp the waiter with the leader this node currently believes in.
	// Any subsequent transition away from this ID marks the waiter
	// at-risk from the leader-change watcher's perspective. 0 means
	// no leader is known yet (pre-election or mid-transition); those
	// waiters are still at-risk on any non-zero transition because we
	// can't tell whether the proposal ever made it into a quorum.
	leaderAtSubmit := uint64(0)
	if db.leaderState != nil {
		leaderAtSubmit = db.leaderState.Leader()
	}
	w := &waiter{
		ack:      make(chan error, 1),
		leaderID: leaderAtSubmit,
	}
	db.waitersMu.Lock()
	db.waiters[p.RequestID] = w
	db.waitersMu.Unlock()

	bs, err := p.Marshal()
	if err != nil {
		db.unregisterWaiter(p.RequestID)
		return 0, nil, err
	}

	// Post-Marshal so the check runs against the exact bytes raft
	// replicates, and so every proposal source converges on one cap.
	if db.maxProposalBytes > 0 && uint64(len(bs)) > db.maxProposalBytes {
		db.unregisterWaiter(p.RequestID)
		return 0, nil, fmt.Errorf("%w: %d bytes > %d limit", cluster.ErrProposalTooLarge, len(bs), db.maxProposalBytes)
	}

	if db.metrics != nil {
		db.metrics.ProposalSubmitted(kind)
	}

	db.logger.Debug("proposing", zap.Uint64("requestID", p.RequestID), zap.Int("entities", len(p.Entities)))

	// db.ctx in the select handles the "db is shutting down" case so a
	// worker (e.g., the ingest goroutine) doesn't race against
	// db.Close's raft teardown and either panic on send or hang.
	select {
	case db.proposeC <- bs:
		return p.RequestID, w.ack, nil
	case <-ctx.Done():
		db.unregisterWaiter(p.RequestID)
		return 0, nil, ctx.Err()
	case <-db.ctx.Done():
		db.unregisterWaiter(p.RequestID)
		return 0, nil, db.ctx.Err()
	}
}

// unregisterWaiter removes the waiter registered under rid. Safe to
// call for a rid that is no longer registered (idempotent: the
// delete is a no-op). Used by Propose's defer, by proposeAsync's
// submit-failure cleanup, and by the ingest worker's in-flight bump
// drain.
func (db *DB) unregisterWaiter(rid uint64) {
	db.waitersMu.Lock()
	delete(db.waiters, rid)
	db.waitersMu.Unlock()
}

// proposalKind classifies a proposal for the ProposalSubmitted
// metric's "kind" attribute. System-internal bump proposals are
// bucketed separately from user-data and config proposals so
// operators can see ingestion-vs-sync-vs-user traffic at a glance.
//
// Order matters: the position/index checks run before the IsInternal
// catch-all so those two get their own buckets rather than being lumped
// under the generic "config" — both are internal, so without the earlier
// specific checks they'd fall into "config". Everything else internal
// (the configs themselves, dead-letters, scrub, …) is "config"; user
// topic data is "user".
func proposalKind(p *cluster.Proposal) string {
	if len(p.Entities) == 0 || p.Entities[0].Type == nil {
		return "user"
	}
	typeID := p.Entities[0].Type.ID
	switch {
	case cluster.IsIngestablePosition(typeID):
		return "position"
	case cluster.IsSyncableIndex(typeID):
		return "index"
	case cluster.IsInternal(typeID):
		return "config"
	}
	return "user"
}

func (db *DB) ProposeDeleteType(ctx context.Context, id string) error {
	deleteTypeEntity := cluster.NewDeleteTypeEntity(id)

	p := &cluster.Proposal{}
	p.Entities = append(p.Entities, deleteTypeEntity)

	return db.Propose(ctx, p)
}

// Scrub proposes a Scrub command at the current applied index (the freeze line)
// and returns once it has been applied — each node then runs the physical
// rewrite in the background. See the cluster.Cluster.Scrub contract. The bound
// is read at propose time but carried in the committed command, so every replica
// rewrites against the same frozen prefix.
func (db *DB) Scrub(ctx context.Context) error {
	e, err := cluster.NewScrubEntity(db.storage.AppliedIndex())
	if err != nil {
		return err
	}
	return db.Propose(ctx, &cluster.Proposal{Entities: []*cluster.Entity{e}})
}

// scrubBacklogReporter is the optional Storage extension that reports whether
// any delete-proposed (RTBF) entities remain unscrubbed in the permanent event
// log. wal.Storage implements it; the in-memory test double does not, in which
// case the scheduler simply never fires (the same optional-interface shape as
// configBuildErrorReporter).
type scrubBacklogReporter interface {
	HasScrubBacklog() bool
}

// scrubScheduler drives automatic scrubbing: on each tick, if this node is the
// leader and there is unscrubbed RTBF backlog, it proposes a Scrub command. The
// work funnels through one committed command, so a leader-only check (rather
// than every node racing to propose) is sufficient and keeps the rewrite
// deterministic. Exits when db.ctx is canceled (db.Close). A zero interval
// disables it.
func (db *DB) scrubScheduler() {
	if db.scrubInterval <= 0 {
		return
	}
	ticker := time.NewTicker(db.scrubInterval)
	defer ticker.Stop()
	for {
		select {
		case <-db.ctx.Done():
			return
		case <-ticker.C:
			db.maybeProposeScrub()
		}
	}
}

// maybeProposeScrub proposes a Scrub when this node is the leader and storage
// reports RTBF backlog. Best-effort: a transient propose failure (no quorum,
// leader change) is logged and retried on the next tick.
func (db *DB) maybeProposeScrub() {
	if db.raft.Leader() != db.ID() {
		return
	}
	reporter, ok := db.storage.(scrubBacklogReporter)
	if !ok || !reporter.HasScrubBacklog() {
		return
	}
	// Bound the propose so a wedged raft layer can't pin this goroutine past
	// the next tick; the next tick retries anyway.
	ctx, cancel := context.WithTimeout(db.ctx, db.scrubInterval)
	defer cancel()
	if err := db.Scrub(ctx); err != nil {
		db.logger.Warn("automatic scrub failed; will retry", zap.Error(err))
	}
}

// Close tears down the DB. Order matters:
//
//  1. Cancel db.ctx via cancelSyncs FIRST. Every worker's context is
//     derived from db.ctx (see db.Sync / db.Ingest), so this propagates
//     into every worker, every inner Ingest goroutine, and every
//     in-flight db.Propose / proposeAsync select. Both the ingest
//     position-bump path and sync's index-bump path now go through the
//     blocking Propose, which watches db.ctx on the send and in the
//     wait, so if we drained workers BEFORE canceling db.ctx and any
//     worker were stuck in a bump while raft was wedged (quorum loss,
//     slow Ready loop), the drain would hang forever. Cancel-first
//     guarantees workers can always reach exit.
//  2. Snapshot the worker registry under workersMu and wait on every
//     handle's done channel. By now the workers are already racing
//     toward exit (their contexts are canceled); the drain is just
//     waiting for them to finish unwinding so we know the user-supplied
//     Sync/Ingest callbacks have fully torn down before we touch raft.
//     We still call h.cancel() in the snapshot loop — it's a no-op
//     (the parent context already canceled the child), but explicit
//     and self-documenting.
//  3. Stop the raft layer, which signals closeC (telling serveChannels'
//     proposeC reader to exit) and waits for serveChannels to actually
//     stop. We do NOT close db.proposeC ourselves: the raft reader is
//     closeC-driven, and a worker that hasn't yet noticed db.ctx
//     cancellation could otherwise panic on send.
//
// db.Close is idempotent: cancelSyncs is a CancelFunc (safe to call
// multiple times), the registry drain is a no-op on the second call
// (the maps are empty after the first), and raft.Close uses sync.Once.
func (db *DB) Close() error {
	db.logger.Info("closing db")

	db.cancelSyncs()

	// The listeners have now been told to exit, but the raft apply loop is still
	// running below (until db.raft.Close). If it applies a committed config
	// entity, wal.Storage sends on syncCh/ingestCh — and with no listener
	// receiving, that bare send blocks the serveChannels goroutine forever, so
	// the db.raft.Close() that waits on it would hang, defeating graceful
	// shutdown. Drain the channels for the rest of Close so the send always has a
	// receiver. A config notification dropped here is re-emitted from storage on
	// the next start, so nothing is lost.
	drainStop := make(chan struct{})
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for {
			select {
			case <-db.syncCh:
			case <-db.ingestCh:
			case <-drainStop:
				return
			}
		}
	}()
	defer func() {
		close(drainStop)
		<-drainDone
	}()

	db.workersMu.Lock()
	db.closed = true
	handles := make([]*workerHandle, 0, len(db.ingestWorkers)+len(db.syncWorkers))
	for id, h := range db.ingestWorkers {
		handles = append(handles, h)
		h.cancel()
		delete(db.ingestWorkers, id)
	}
	for id, h := range db.syncWorkers {
		handles = append(handles, h)
		h.cancel()
		delete(db.syncWorkers, id)
	}
	db.workersMu.Unlock()

	if abandoned := drainWorkers(handles, closeDrainTimeout); abandoned > 0 {
		db.logger.Warn("close: worker drain timed out; abandoned wedged worker(s) and proceeded with shutdown "+
			"(a syncable stuck in tx.Commit against an unreachable destination is the usual cause)",
			zap.Int("abandoned", abandoned), zap.Duration("timeout", closeDrainTimeout))
	}
	// Release each drained worker's held resources: a syncable's prepared
	// statements, an ingestable's source handles. Each helper skips the other
	// kind (nil syncable / nil ingestable) and any worker still running after an
	// abandoned drain, so the two calls together cover every drained handle.
	for _, h := range handles {
		db.closeDrainedSyncable(h, "")
		db.closeDrainedIngestable(h, "")
	}

	// Hand leadership to a caught-up voter before stopping raft, so a graceful
	// restart (e.g. a rolling upgrade) doesn't force the cluster through a full
	// election. Best-effort and bounded — see transferLeadershipBeforeStop. Done
	// after the worker drain so the workers finish their unwind under this
	// node's leadership, then we hand off.
	db.transferLeadershipBeforeStop()

	// The WAL Storage is owned by the caller — it opens it, passes it to New, and
	// keeps using it independently of the DB (operators and tests query the
	// projected SQL sink via storage.Database *after* closing the DB). So Close
	// stops raft but does NOT close the Storage; the owner closes it (which stops
	// the scrubber and closes the WAL/bbolt + SQL handles). Storage.Close is
	// idempotent so the owner can close it unconditionally.
	return db.raft.Close()
}

// closeDrainTimeout bounds how long Close waits for in-flight sync/ingest workers
// to exit. A syncable wedged in tx.Commit (database/sql exposes no CommitContext,
// so a canceled worker ctx cannot interrupt an in-flight commit to an unreachable
// destination) can never close its done channel; without a bound, Close would
// block forever — defeating graceful shutdown and leadership hand-off and forcing
// the orchestrator to SIGKILL. Stragglers are abandoned: their goroutines die on
// process exit and the sync re-applies idempotently on the next start.
const closeDrainTimeout = 10 * time.Second

// waitDone waits for a cancelled worker's done channel, up to timeout. It
// returns true if the worker exited, false if the timeout fired first (a wedged
// worker to abandon). The listener-path handoffs (Sync/Ingest replace,
// deleteSync/deleteIngest) use it so a worker stuck in tx.Commit against an
// unreachable destination can't park the single-threaded listener — and thereby
// stall the raft apply loop on its next config send — indefinitely. An abandoned
// worker was already cancelled; it exits when its tx unwedges, and it's removed
// from the registry regardless so the caller doesn't spin on it.
func waitDone(done <-chan struct{}, timeout time.Duration) bool {
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// runBounded runs fn on its own goroutine and waits at most timeout for it to
// return, reporting fn's error and whether it completed. On timeout fn keeps
// running detached (a blocked Exec/Close cannot be interrupted from here) and
// its eventual error is discarded — a bounded leak that unwinds when the
// destination recovers or the kernel TCP timeout fires.
//
// This is the liveness guard for destination-touching calls on the
// single-threaded config-listener path (Teardown, Close): the drain leg of a
// delete/replace is already waitDone-bounded, but the teardown/close that
// FOLLOWS an abandoned drain talks to the same unreachable destination, and an
// unbounded call there parks the listener — the apply path's next config-channel
// send then blocks the raft Ready loop, stalling apply of ALL further committed
// entries. The bound holds for any implementation, including ones that don't
// ctx-bound themselves internally.
func runBounded(timeout time.Duration, fn func() error) (err error, completed bool) {
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err, true
	case <-time.After(timeout):
		return nil, false
	}
}

// closeDrainedSyncable releases a torn-down syncable's resources — a SQL
// syncable's prepared statements, a webhook syncable's idle connections — via
// Close, so an ordinary redeploy (re-POST / delete / rebuild / shutdown) doesn't
// leak a statement set on the shared, long-lived connection pool every time.
// Syncable.Close closes only what Init prepared, never the shared pool (other
// syncables share it).
//
// It closes ONLY once the worker goroutine has drained: a still-running worker
// (its drain timed out — wedged on an unreachable destination) may be mid-Sync
// against those statements, and closing them under it would race. The done
// channel is closed by the worker's defer after db.sync returns, so a
// non-blocking receive on it is the "no Sync in flight" signal. Best-effort:
// a close error is logged, not fatal. Safe with a nil handle or an ingest
// handle (nil syncable).
func (db *DB) closeDrainedSyncable(handle *workerHandle, id string) {
	if handle == nil || handle.syncable == nil {
		return
	}
	select {
	case <-handle.done:
		// Drained: the worker goroutine has returned, so no Sync is in flight and
		// the prepared statements are idle — safe to close.
	default:
		// Still running (drain timed out). Closing its statements now would race
		// the worker's use of them, so leave them; the pool reclaims them when the
		// connection is eventually recycled.
		return
	}
	// Bounded: Close writes statement-close packets to the destination, which
	// can block on a dead network until the TCP timeout — and this runs on the
	// config-listener path (see runBounded).
	if err, completed := runBounded(db.workerDrainTimeout, handle.syncable.Close); !completed {
		db.logger.Warn("syncable close did not return in time (unreachable destination?); abandoning it — prepared statements linger until the pool recycles the connection",
			zap.String("id", id), zap.Duration("timeout", db.workerDrainTimeout))
	} else if err != nil {
		db.logger.Warn("syncable close on teardown failed; prepared statements may linger until the pool recycles the connection",
			zap.String("id", id), zap.Error(err))
	}
}

// closeDrainedIngestable releases a torn-down ingest worker's Ingestable
// resources via Close — the ingest twin of closeDrainedSyncable — so an ordinary
// redeploy (replace / delete / shutdown) doesn't leak whatever the Ingestable
// holds (a source connection pool, a binlog syncer). Ingestable.Close is the
// release half of the lifecycle contract; the destructive source teardown (drop
// the replication slot) is separate and owner-only (see deleteIngest).
//
// It closes ONLY once the worker goroutine has drained: a still-running worker
// (its drain timed out — wedged on its source) may be mid-Ingest against those
// resources, and closing them under it would race. The done channel is closed by
// the worker's defer after ingest returns, so a non-blocking receive on it is the
// "no Ingest in flight" signal; an abandoned worker releases its resources when it
// finally exits (or on process exit). Best-effort: a close error is logged, not
// fatal. Safe with a nil handle or a sync handle (nil ingestable).
func (db *DB) closeDrainedIngestable(handle *workerHandle, id string) {
	if handle == nil || handle.ingestable == nil {
		return
	}
	select {
	case <-handle.done:
		// Drained: the worker goroutine has returned, so no Ingest is in flight and
		// the Ingestable's resources are idle — safe to close.
	default:
		// Still running (drain timed out). Closing its resources now would race the
		// worker's use of them, so leave them; they release when it eventually exits.
		return
	}
	if err := handle.ingestable.Close(); err != nil {
		db.logger.Warn("ingestable close on teardown failed; source resources may linger until the worker exits",
			zap.String("id", id), zap.Error(err))
	}
}

// drainWorkers waits for each worker handle's done channel, up to timeout in
// total, and returns the count still running when the deadline passed. Workers
// that have already exited drain instantly; the bound only bites on a wedged one.
func drainWorkers(handles []*workerHandle, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	abandoned := 0
	for _, h := range handles {
		// Check "already exited?" first, in its own non-blocking select (the
		// default makes it not wait), and skip on the fast path. This is NOT just
		// an optimization for the common case: once the deadline has passed,
		// time.Until(deadline) is negative, so the timer case in the second select
		// is ALSO ready — and when multiple select cases are ready Go picks one at
		// RANDOM. So a worker that exited cleanly could randomly land on the timeout
		// branch and be miscounted as abandoned. Draining a done worker here, before
		// it can reach the racy second select, keeps the abandoned count honest.
		select {
		case <-h.done:
			continue
		default:
		}
		// Still running: wait for it to exit, but no later than the shared
		// deadline. done wins -> drained; deadline wins -> abandoned (a wedged
		// worker). The deadline is shared across all handles, so the whole drain is
		// bounded by timeout regardless of how many workers are wedged.
		select {
		case <-h.done:
		case <-time.After(time.Until(deadline)):
			abandoned++
		}
	}
	return abandoned
}

// proposeIngestablePosition bumps the persisted Position for an
// ingestable after the upstream source advances, and BLOCKS until that
// bump is durably applied (the ingestablePositions bucket written via
// the apply path) or until ctx is canceled / a leader change orphans
// the proposal. The cost is one Raft round-trip per checkpoint; in
// exchange, the resume position is durable.
//
// This used to be fire-and-forget (proposeIngestablePositionAsync on
// db.ctx) with a worker-local in-flight-bump set drained on freeze —
// machinery that existed only because the bump was async. Blocking the
// bump makes the worker process one checkpoint at a time: there is never
// more than one bump in flight, so the supervisor's post-freeze
// storage.Position read is always definitive without a drain, and a hard
// crash re-emits at most the proposals since the last durable checkpoint
// instead of an unbounded storm.
//
// ctx is the INGEST WORKER's context, not db.ctx. On a registry replace
// or Close the worker ctx is canceled; Propose then returns ctx.Err and
// the position is not advanced. The ingestable's next resume reads the
// un-advanced (persisted) position from storage and re-emits — safe
// because those proposals either didn't commit, or committed but their
// bump didn't (the very race this guards). On ErrProposalUnknown the
// worker freezes, exactly like an unknown user proposal.
//
// On a successful (durable) bump the round-trip latency is recorded to
// committed_ingest_position_bump_duration_seconds.
func (db *DB) proposeIngestablePosition(ctx context.Context, p *cluster.IngestablePosition) error {
	entity, err := cluster.NewUpsertIngestablePositionEntity(p)
	if err != nil {
		return err
	}

	start := time.Now()
	err = db.Propose(ctx, &cluster.Proposal{Entities: []*cluster.Entity{entity}})
	if err == nil && db.metrics != nil {
		db.metrics.IngestPositionBumpCompleted(time.Since(start))
	}
	return err
}

func (db *DB) ID() uint64 {
	return db.raft.id
}

// Leader returns the raft node ID this DB believes is the current leader,
// or 0 if no leader is known. Pass-through to Raft.Leader, which reads
// through to etcd raft's Status() snapshot. Used by the /ready HTTP probe.
func (db *DB) Leader() uint64 {
	return db.raft.Leader()
}

// AddMember adds a voting node to the cluster via a joint-consensus
// (ConfChangeV2) membership change and blocks until this node observes the
// new member in the final configuration, ctx fires, or the DB shuts down.
//
// rawURL is the new node's advertised raft peer URL; it is carried in the
// conf change Context so every existing node learns the address to dial it
// (raft replicates only node ids, not addresses). The new node itself must
// be started in join mode (WithJoin / COMMITTED_JOIN) with a peer set that
// lets it reach the existing members. See docs/operations/membership.md.
//
// The change is safe under partition: joint consensus requires a majority of
// both the old and new configurations to agree throughout the transition, so
// no quorum shift can lose a committed entry. AddMember may be called against
// any node (a follower forwards the proposal to the leader).
func (db *DB) AddMember(ctx context.Context, id uint64, rawURL string) error {
	if id == 0 {
		return fmt.Errorf("%w: id must be non-zero", cluster.ErrInvalidMember)
	}
	if rawURL == "" {
		return fmt.Errorf("%w: url must be non-empty", cluster.ErrInvalidMember)
	}
	cc := &raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionJointImplicit.Enum(),
		Changes:    []*raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddNode.Enum(), NodeId: &id}},
		Context:    []byte(rawURL),
	}
	return db.proposeConfChange(ctx, cc, id, memberVoter)
}

// AddLearner adds a node as a non-voting learner via a joint-consensus
// (ConfChangeV2) membership change and blocks until this node observes the
// learner in the final configuration, ctx fires, or the DB shuts down.
//
// A learner receives the replicated log but does not count toward quorum: it
// doesn't vote and its acks don't help commit. That makes it the safe way to
// grow the cluster — a new node catches up as a learner with zero effect on
// quorum math, then is promoted to a voter (PromoteMember) once it is caught
// up, so the cluster never includes a not-yet-replicated node in its quorum.
//
// rawURL is the new node's advertised raft peer URL, carried in the conf
// change Context exactly like AddMember; the new node must be started in join
// mode and should set COMMITTED_API_URL so it self-announces its API address.
// Partition-safe and callable on any node. See docs/operations/membership.md.
func (db *DB) AddLearner(ctx context.Context, id uint64, rawURL string) error {
	if id == 0 {
		return fmt.Errorf("%w: id must be non-zero", cluster.ErrInvalidMember)
	}
	if rawURL == "" {
		return fmt.Errorf("%w: url must be non-empty", cluster.ErrInvalidMember)
	}
	cc := &raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionJointImplicit.Enum(),
		Changes:    []*raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddLearnerNode.Enum(), NodeId: &id}},
		Context:    []byte(rawURL),
	}
	return db.proposeConfChange(ctx, cc, id, memberLearner)
}

// PromoteMember promotes an existing learner to a voter via a joint-consensus
// (ConfChangeV2) membership change (a ConfChangeAddNode for the learner's id,
// which etcd/raft relocates from the Learners set to the Voters set) and
// blocks until this node observes it as a voter, ctx fires, or the DB shuts
// down. No Context is needed — the peer transport entry and the node's
// announced API address already exist from the learner add.
//
// PromoteMember validates only that id is a current learner; it does NOT judge
// whether the learner has caught up. That is deliberate: a "caught up"
// threshold is the caller's policy (it observes each member's matched index
// via GET /v1/membership and decides), the check would be racy regardless (the
// matched index is a snapshot), and the server has no principled threshold to
// pick. The caller minimizes the at-quorum window by polling before promoting.
// See docs/operations/membership.md and the ticket's "Decisions".
//
// Promoting a non-learner is rejected with cluster.ErrNotLearner rather than
// submitted: a ConfChangeAddNode for an unknown id would add a brand-new voter
// with no transport entry (a phantom voter that can't be reached), and for an
// existing voter it is a meaningless no-op. The check reads this node's applied
// configuration, so against a briefly-stale follower it can false-negative; an
// operator promotes ids it just added and observed via GET /v1/membership, and
// can retry. Partition-safe and callable on any node.
func (db *DB) PromoteMember(ctx context.Context, id uint64) error {
	if id == 0 {
		return fmt.Errorf("%w: id must be non-zero", cluster.ErrInvalidMember)
	}
	voters, learners, _ := db.raft.memberStatus()
	if _, isLearner := learners[id]; !isLearner {
		if _, isVoter := voters[id]; isVoter {
			return fmt.Errorf("%w: node %d is already a voter", cluster.ErrNotLearner, id)
		}
		return fmt.Errorf("%w: node %d is not a known learner", cluster.ErrNotLearner, id)
	}
	cc := &raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionJointImplicit.Enum(),
		Changes:    []*raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddNode.Enum(), NodeId: &id}},
	}
	return db.proposeConfChange(ctx, cc, id, memberVoter)
}

// RemoveMember removes a node from the cluster via a joint-consensus
// (ConfChangeV2) membership change and blocks until this node observes the
// node gone from the final configuration, ctx fires, or the DB shuts down.
// Like AddMember it is partition-safe and may be called against any node.
// Removing the node serving the request is allowed; that node steps out of
// the configuration once the change commits and should then be stopped.
func (db *DB) RemoveMember(ctx context.Context, id uint64) error {
	if id == 0 {
		return fmt.Errorf("%w: id must be non-zero", cluster.ErrInvalidMember)
	}
	// Refuse to remove the last voter. raft rejects an empty incoming voter set
	// only at apply time — by panicking (raft.go) — which commits an
	// unrecoverable entry that re-panics every node on restart. This best-effort
	// guard reads the local config (like PromoteMember it can false-negative
	// against a briefly stale follower; an operator observes membership via
	// GET /v1/membership and can retry), catching the foot-gun before it reaches
	// the log. Removing a learner, or a voter while other voters remain, is fine.
	voters, _, _ := db.raft.memberStatus()
	if _, isVoter := voters[id]; isVoter && len(voters) == 1 {
		return fmt.Errorf("%w: node %d is the only voter; add or promote another voter first",
			cluster.ErrWouldRemoveLastVoter, id)
	}
	cc := &raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionJointImplicit.Enum(),
		Changes:    []*raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeRemoveNode.Enum(), NodeId: &id}},
	}
	return db.proposeConfChange(ctx, cc, id, memberAbsent)
}

// membershipTarget is the post-change role waitForMembership blocks for:
// gone from the configuration (remove), a voter (add / promote), or a learner
// (add-learner).
type membershipTarget int

const (
	memberAbsent  membershipTarget = iota // absent from both voters and learners
	memberVoter                           // present as a voter
	memberLearner                         // present as a learner
)

// proposeConfChange submits cc to the raft propose loop, then blocks (via
// waitForMembership) until the membership change has fully taken effect on
// this node. Submission itself can fail fast if ctx fires or the DB is
// shutting down before the propose loop accepts the change.
func (db *DB) proposeConfChange(ctx context.Context, cc *raftpb.ConfChangeV2, id uint64, target membershipTarget) error {
	select {
	case db.confChangeC <- cc:
	case <-ctx.Done():
		return ctx.Err()
	case <-db.ctx.Done():
		return db.ctx.Err()
	}
	return db.waitForMembership(ctx, id, target)
}

// waitForMembership blocks until this node's applied raft configuration
// reaches target — id in the expected set (voter / learner / neither) AND the
// joint transition complete — ctx is canceled, or the DB is shutting down.
// It waits on the same applied-index broadcast (appliedNotify) the Ready loop
// fires after each apply, re-checking on every wake rather than polling: a
// membership change advances the applied index when both the enter-joint and
// the auto-leave entries apply, so each step wakes the waiter. Requiring
// joint==false means a successful return reflects the final (non-joint)
// configuration, not the transient joint state.
func (db *DB) waitForMembership(ctx context.Context, id uint64, target membershipTarget) error {
	settled := func() bool {
		voters, learners, joint := db.raft.memberStatus()
		if joint {
			return false
		}
		_, isVoter := voters[id]
		_, isLearner := learners[id]
		switch target {
		case memberVoter:
			return isVoter
		case memberLearner:
			return isLearner
		default: // memberAbsent
			return !isVoter && !isLearner
		}
	}
	for {
		if settled() {
			return nil
		}
		// Snapshot the current-generation channel before re-checking, so an
		// apply that lands between the check and the select doesn't strand
		// us — the channel we hold is already closed and the select returns
		// at once. Same close-to-broadcast pattern as waitForApplied.
		ch := db.appliedNotify()
		if settled() {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-db.ctx.Done():
			return db.ctx.Err()
		}
	}
}

// AppliedIndex returns the highest log index that has been fully applied
// to local application state. Pass-through to Storage.AppliedIndex. Used
// by the /ready HTTP probe to gate readiness on this node having caught
// up; a freshly-started node reports 0 until at least one entry applies.
func (db *DB) AppliedIndex() uint64 {
	return db.storage.AppliedIndex()
}

// ConfigBuildErrors returns the configs this node persisted but could not
// build into live objects (degraded — a node-local condition, usually a
// missing ${VAR} secret). Served by GET /node/status. Optional-interface
// assert on storage — the same fork the committed_config_build_errors
// gauge uses: real wal.Storage implements configBuildErrorReporter; the
// in-memory test double does not and reports none.
func (db *DB) ConfigBuildErrors() []cluster.ConfigBuildError {
	if r, ok := db.storage.(configBuildErrorReporter); ok {
		return r.ConfigBuildErrors()
	}
	return nil
}
