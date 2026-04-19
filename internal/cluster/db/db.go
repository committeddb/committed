package db

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/metrics"
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

// ingestSupervisor* constants govern the auto-restart behavior applied
// when an ingest worker parks in the ErrProposalUnknown freeze branch.
// Options (WithIngestSupervisor*) let callers override; zero values in
// options resolve to these defaults. See the ingest-worker-supervisor
// ticket for the motivation — in short, a cluster that flaps under
// load would otherwise leave one or more ingestables offline after
// each flap until an operator intervened.
const (
	defaultIngestSupervisorInitialBackoff = 100 * time.Millisecond
	defaultIngestSupervisorMaxBackoff     = 30 * time.Second
	defaultIngestSupervisorMaxAttempts    = 20
	defaultIngestSupervisorHealthyWindow  = 60 * time.Second
)

type Peers map[uint64]string

type DB struct {
	CommitC     <-chan []byte
	ErrorC      <-chan error
	proposeC    chan<- []byte
	confChangeC chan<- raftpb.ConfChange
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
	closed        bool

	// ingestSupervisorMu guards ingestSupervisorStates. Deliberately
	// separate from workersMu so the supervisor's bookkeeping doesn't
	// contend with the hot worker-registry path.
	ingestSupervisorMu     sync.Mutex
	ingestSupervisorStates map[string]*ingestSupervisorState

	// Cached supervisor tuning; resolved in New from options/defaults so
	// the freeze-restart hot path reads a struct field rather than
	// dereferencing the options config on each call.
	ingestSupervisorInitialBackoff time.Duration
	ingestSupervisorMaxBackoff     time.Duration
	ingestSupervisorMaxAttempts    int
	ingestSupervisorHealthyWindow  time.Duration

	// ingestFreezeDrainTimeout caps how long the ingest worker's
	// freeze path waits on its in-flight bump acks. Resolved in New
	// from options; 0 means "derived from leaderChangeGrace".
	ingestFreezeDrainTimeout time.Duration

	logger  *zap.Logger
	metrics *metrics.Metrics
}

// ingestSupervisorState tracks consecutive freeze-restart cycles for a
// single ingestable id. A freeze observed within
// ingestSupervisorHealthyWindow of the previous one counts as
// consecutive and grows the backoff; a longer gap means the restarted
// worker ran healthy long enough to reset the counter. giveup is set
// once the supervisor has declared the id unrecoverable so subsequent
// freezes (e.g., if an operator replaces the config later) don't
// silently carry forward old state forever.
type ingestSupervisorState struct {
	lastFreezeAt       time.Time
	consecutiveFreezes int
	backoff            time.Duration
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
	if cfg.ingestFreezeDrainTimeout == 0 {
		cfg.ingestFreezeDrainTimeout = 2 * cfg.leaderChangeGrace
	}

	proposeC := make(chan []byte)
	confChangeC := make(chan raftpb.ConfChange)

	rpeers := make([]raft.Peer, len(peers))
	i := 0
	for k, v := range peers {
		rpeers[i] = raft.Peer{ID: k, Context: []byte(v)}
		i++
	}

	ctx, cancelSyncs := context.WithCancel(context.Background())

	db := &DB{
		proposeC:                       proposeC,
		confChangeC:                    confChangeC,
		storage:                        s,
		ctx:                            ctx,
		cancelSyncs:                    cancelSyncs,
		parser:                         p,
		waiters:                        make(map[uint64]*waiter),
		leaderChangeGrace:              cfg.leaderChangeGrace,
		syncWorkers:                    make(map[string]*workerHandle),
		ingestWorkers:                  make(map[string]*workerHandle),
		ingestSupervisorStates:         make(map[string]*ingestSupervisorState),
		ingestSupervisorInitialBackoff: cfg.ingestSupervisorInitialBackoff,
		ingestSupervisorMaxBackoff:     cfg.ingestSupervisorMaxBackoff,
		ingestSupervisorMaxAttempts:    cfg.ingestSupervisorMaxAttempts,
		ingestSupervisorHealthyWindow:  cfg.ingestSupervisorHealthyWindow,
		ingestFreezeDrainTimeout:       cfg.ingestFreezeDrainTimeout,
		logger:                         cfg.logger,
		metrics:                        cfg.metrics,
	}

	// The applied notifier is wired into the raft Ready loop. After each
	// successful ApplyCommitted, raft.go calls db.notifyApplied with the
	// raw entry data so we can unmarshal once, look up the waiter by
	// p.RequestID, and signal it. This shape works uniformly for
	// wal.Storage (real apply) and testing.MemoryStorage (no-op apply) —
	// both go through the same raft.go iteration over rd.CommittedEntries.
	commitC, errorC, raft := newRaftWithOptions(id, rpeers, s, proposeC, confChangeC, db.notifyApplied, cfg.logger, cfg)

	db.CommitC = commitC
	db.ErrorC = errorC
	db.raft = raft
	db.leaderState = raft.leaderState

	// EatCommitC was previously the caller's responsibility — forgetting
	// to call it deadlocks the raft Ready loop on the unbuffered commitC
	// send. Now that the only legitimate use of CommitC was as a test
	// synchronization barrier (rendered redundant by blocking Propose),
	// it's safe to unconditionally drain. Tests that previously read
	// <-db.CommitC for sync now use blocking Propose instead.
	db.EatCommitC()

	// The watcher subscribes to leader-ID transitions from LeaderState
	// BEFORE the first Ready iteration could land — subscribe() just
	// registers a channel, so events that arrive before we start
	// reading are buffered until the watcher goroutine catches up.
	// Buffer sized generously (64) so a burst of flaps during a slow
	// schedule doesn't cause us to silently drop transitions.
	go db.watchLeaderTransitions(db.leaderState.Subscribe(64))

	go db.listenForSyncables(sync)
	go db.listenForIngestables(ingest)

	return db
}

// watchLeaderTransitions runs for the lifetime of the DB. On every
// LeaderTransition emitted by LeaderState, it snapshots the current
// waiter map, identifies every waiter stamped under a leader that
// isn't the new leader, and schedules a grace-delayed sweep that
// signals those waiters with ErrProposalUnknown unless they've been
// cleaned up by the apply path in the interim.
//
// The grace period is critical to reducing false positives: a leader
// hand-off where the new leader inherits and commits the entry fast
// will see notifyApplied fire before the grace timer, which deletes
// the waiter out from under the sweep. Only waiters that are still
// registered when the timer fires get ErrProposalUnknown.
//
// The watcher exits on db.ctx.Done (Close path) — no further sweeps
// are scheduled. Any in-flight sweep goroutine also observes db.ctx
// and exits without signaling (Propose returns db.ctx.Err on its own
// select branch during shutdown).
func (db *DB) watchLeaderTransitions(transitions <-chan LeaderTransition) {
	for {
		select {
		case t, ok := <-transitions:
			if !ok {
				return
			}
			db.logger.Debug("observed leader transition",
				zap.Uint64("old", t.Old),
				zap.Uint64("new", t.New))
			if db.metrics != nil {
				db.metrics.LeaderTransitionObserved()
			}

			// A transition whose old value is 0 is the initial
			// establishment of a leader (pre-election → elected),
			// not a leader change. Waiters stamped with 0 submitted
			// before a leader was known; raft internally held them
			// pending election and submits them to the new leader
			// when it emerges — there is no prior leader whose
			// acceptance got orphaned, so marking them at-risk here
			// would be a spurious false positive that fires every
			// cold start. Any subsequent flap (X→Y where X > 0)
			// still marks stamp=0 waiters at risk via the normal
			// leaderID != t.New check below.
			if t.Old == 0 {
				continue
			}

			// Snapshot waiters-at-risk under the lock so a Propose
			// currently mid-registration can't be observed in a
			// half-installed state. The snapshot captures pointers;
			// the grace-timer goroutine compares by pointer identity
			// at fire time to detect races where the waiter's slot
			// was reused for a new request with the same RequestID
			// (not currently possible — RequestID is monotonic via
			// atomic — but the check is cheap and defensive).
			db.waitersMu.Lock()
			atRisk := make(map[uint64]*waiter, len(db.waiters))
			for id, w := range db.waiters {
				if w.leaderID != t.New {
					atRisk[id] = w
				}
			}
			db.waitersMu.Unlock()

			if len(atRisk) == 0 {
				continue
			}
			go db.signalAfterGrace(atRisk)
		case <-db.ctx.Done():
			return
		}
	}
}

// signalAfterGrace waits leaderChangeGrace and then signals every
// still-registered waiter in atRisk with ErrProposalUnknown. A waiter
// that's been removed from the registry (Propose's defer ran after
// notifyApplied signaled it) is skipped naturally.
//
// The send to w.ack is non-blocking: notifyApplied may have won the
// race and filled the buffered chan with nil, in which case we hit
// the default branch and drop the ErrProposalUnknown so the caller
// sees "applied" instead of "unknown" (the preferred outcome when
// both signals race).
func (db *DB) signalAfterGrace(atRisk map[uint64]*waiter) {
	timer := time.NewTimer(db.leaderChangeGrace)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-db.ctx.Done():
		return
	}

	db.waitersMu.Lock()
	defer db.waitersMu.Unlock()
	for id, w := range atRisk {
		cur, ok := db.waiters[id]
		if !ok || cur != w {
			continue
		}
		select {
		case w.ack <- ErrProposalUnknown:
			db.logger.Debug("propose fail-fast",
				zap.Uint64("requestID", id),
				zap.Uint64("stampedLeader", w.leaderID))
			if db.metrics != nil {
				db.metrics.ProposeFailFastUnknown()
			}
		default:
		}
	}
}

// notifyApplied is invoked from the raft Ready loop after each successful
// ApplyCommitted. It unmarshals the entry, looks up any blocking
// db.Propose call by RequestID, and signals the waiter. Entries with
// RequestID == 0 are system-internal proposals (or pre-PR2 entries) that
// have no waiter; for those this is a no-op.
func (db *DB) notifyApplied(data []byte) {
	if data == nil {
		return
	}
	p := &cluster.Proposal{}
	if err := p.Unmarshal(data, db.storage); err != nil {
		// Defense-in-depth: ApplyCommitted also unmarshals the entry
		// and fatal-exits on failure, so reaching here with an
		// unmarshal error means the apply path succeeded on one
		// decode and this one failed — pathological, not expected.
		// Log and skip; Propose's caller will time out via ctx if
		// its waiter is never signaled.
		db.logger.Warn("notifyApplied unmarshal failed", zap.Error(err))
		return
	}
	if p.RequestID == 0 {
		return
	}
	db.waitersMu.Lock()
	w, ok := db.waiters[p.RequestID]
	db.waitersMu.Unlock()
	if !ok {
		return
	}
	// Buffered channel of capacity 1 set up by Propose, so this never
	// blocks. We don't delete the waiter here — Propose's defer cleans
	// it up after the receive. If the leader-change watcher has already
	// queued an ErrProposalUnknown on this slot, the default branch
	// fires and we leave the watcher's signal in place: a caller
	// treating ErrProposalUnknown as "unknown outcome, must confirm"
	// will just re-read and find the entry applied, which is safe.
	select {
	case w.ack <- nil:
	default:
	}
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

func (db *DB) EatCommitC() {
	go func() {
		for range db.CommitC {
			db.logger.Debug("ate a commit")
		}
	}()
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

// proposeAsync is the unified submit primitive. It assigns a
// RequestID, registers a waiter stamped under the current leader,
// marshals, and sends on proposeC. On successful submit it returns
// (requestID, ack, nil); callers MUST either read the ack (once) AND
// call unregisterWaiter(rid), OR delegate both to a wrapper like
// Propose / proposeAndDiscardAck. The ack channel is buffered
// cap=1, so notifyApplied and the leader-change watcher can each
// send without blocking; the first signal wins, subsequent signals
// fall through the default branch in their respective send sites.
//
// On submit failure (ctx or db.ctx canceled before the proposeC
// send completes, or Marshal fails), proposeAsync unregisters the
// waiter itself and returns a zero RequestID + nil ack + error.
//
// Callers that want simple fire-and-forget semantics (sync's index
// bump, which relies on downstream idempotency and doesn't care
// about outcome) use proposeAndDiscardAck. Callers that want
// pipelined submissions with later outcome tracking (ingest's
// position bump, which must drain before the worker freezes so the
// supervisor's subsequent storage.Position read reflects reality)
// use proposeAsync directly.
func (db *DB) proposeAsync(ctx context.Context, p *cluster.Proposal) (uint64, <-chan error, error) {
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

	kind := proposalKind(p)
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

// proposeAndDiscardAck submits a proposal and abandons the ack: a
// small cleanup goroutine drains the ack (or observes db shutdown
// via db.ctx) and then unregisters the waiter so the waiters map
// doesn't leak.
//
// Used by sync's index-bump path, which relies on downstream sink-
// level idempotency (SQL UPSERT) for correctness under flap and so
// doesn't need to observe whether the bump applied or was signaled
// ErrProposalUnknown. (Non-idempotent syncables — e.g. HTTP webhook —
// have a separate latent issue tracked in
// .claude-scratch/tickets/sync-fail-fast-bump-tracking.md.)
//
// Callers that DO care about outcome (ingest position bumps) use
// proposeAsync directly and track the ack in their own in-flight
// set for later drain.
func (db *DB) proposeAndDiscardAck(ctx context.Context, p *cluster.Proposal) error {
	rid, ack, err := db.proposeAsync(ctx, p)
	if err != nil {
		return err
	}
	go func() {
		select {
		case <-ack:
		case <-db.ctx.Done():
		}
		db.unregisterWaiter(rid)
	}()
	return nil
}

// unregisterWaiter removes the waiter registered under rid. Safe to
// call for a rid that is no longer registered (idempotent: the
// delete is a no-op). Used by Propose's defer, by proposeAsync's
// submit-failure cleanup, by proposeAndDiscardAck's drain goroutine,
// and by the ingest worker's in-flight bump drain.
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
// Order matters: the position/index checks run before IsSystem
// because IsSystem returns true for syncableIndexType but not for
// ingestablePositionType, and distinguishing the two via their own
// buckets is more useful than lumping syncable-index under "config".
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
	case cluster.IsSystem(typeID):
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

// Close tears down the DB. Order matters:
//
//  1. Cancel db.ctx via cancelSyncs FIRST. Every worker's context is
//     derived from db.ctx (see db.Sync / db.Ingest), so this propagates
//     into every worker, every inner Ingest goroutine, and every
//     in-flight db.Propose / proposeAsync select. Both the ingest
//     position-bump path (proposeAsync) and sync's index-bump path
//     (proposeAndDiscardAck, which also flows through proposeAsync)
//     watch db.ctx on the send, so if we drained workers BEFORE
//     canceling db.ctx and any worker were stuck in a bump while raft
//     was wedged (quorum loss, slow Ready loop), the drain would hang
//     forever. Cancel-first guarantees workers can always reach exit.
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

	for _, h := range handles {
		<-h.done
	}

	return db.raft.Close()
}

// proposeSyncableIndex bumps the persisted SyncableIndex for a
// syncable after a successful Sync. Called from the sync worker,
// which doesn't observe bump outcomes directly — the unified
// primitive registers a waiter under the hood and a cleanup
// goroutine unregisters it on apply, ErrProposalUnknown, or db
// shutdown, whichever comes first.
//
// Uses db.ctx (lifecycle) rather than a worker ctx because a
// registry replace shouldn't drop the bump: the replacement sync
// worker should start from the advanced index, not re-sync rows
// already pushed downstream. The Sync operation is required to be
// idempotent at the sink level (SQL UPSERT), so a lost bump just
// means extra downstream work, not incorrect state. (Non-idempotent
// syncables have a separate latent issue — see
// .claude-scratch/tickets/sync-fail-fast-bump-tracking.md.)
func (db *DB) proposeSyncableIndex(_ context.Context, i *cluster.SyncableIndex) error {
	entity, err := cluster.NewUpsertSyncableIndexEntity(i)
	if err != nil {
		return err
	}
	return db.proposeAndDiscardAck(db.ctx, &cluster.Proposal{Entities: []*cluster.Entity{entity}})
}

// proposeIngestablePositionAsync bumps the persisted Position for
// an ingestable after the upstream source advances. Unlike sync's
// bump, the ingest worker MUST be able to observe the bump's
// outcome: if a flap truncates the bump before it applies,
// storage.Position stays stale, and the supervisor's restart would
// replay rows that were already committed under the old leader —
// producing duplicate raft log entries.
//
// The worker registers the returned ack in its in-flight bump set
// and drains every outstanding entry on freeze (either applied or
// ErrProposalUnknown is fine; both leave storage.Position
// consistent with what the supervisor's post-freeze read will
// see). Passing db.ctx here (instead of a worker ctx) keeps the
// bump submittable even during a registry replace — same rationale
// as the old proposeIngestablePosition.
func (db *DB) proposeIngestablePositionAsync(_ context.Context, p *cluster.IngestablePosition) (uint64, <-chan error, error) {
	entity, err := cluster.NewUpsertIngestablePositionEntity(p)
	if err != nil {
		return 0, nil, err
	}
	return db.proposeAsync(db.ctx, &cluster.Proposal{Entities: []*cluster.Entity{entity}})
}

// superviseRestartIngest re-registers an ingestable whose worker
// parked in the ErrProposalUnknown freeze branch. It runs as a
// detached goroutine spawned from the freeze-exit branch of the
// worker-launch goroutine in spawnIngestWorkerLocked.
//
// Behavior:
//
//   - Records the freeze in the per-id state map; resets the
//     consecutive counter if the gap since the last freeze exceeds
//     ingestSupervisorHealthyWindow (the worker ran cleanly long enough
//     that this flap is "new", not a continuation).
//   - Gives up + emits IngestSupervisorGiveup once the consecutive
//     count exceeds ingestSupervisorMaxAttempts. The worker stays
//     parked; operator intervention is required.
//   - Otherwise waits a jittered backoff (exponential, capped at
//     ingestSupervisorMaxBackoff) before re-registering.
//   - Preflight AND install under a single workersMu hold so a
//     concurrent user replace can't slip in between (see the race
//     analysis in spawnIngestWorkerLocked's doc comment): if the
//     frozen handle is no longer the registered one when we acquire
//     the lock, we bail; otherwise we delete it and install the
//     supervisor's replacement without releasing the lock. A user
//     replace that arrives after the unlock still wins the final
//     state via db.Ingest's own replacement loop.
//   - On successful install, clears the frozen gauge and bumps the
//     IngestRestart counter.
func (db *DB) superviseRestartIngest(id string, i cluster.Ingestable, frozen *workerHandle) {
	backoff, consecutive, giveup := db.recordFreezeAndNextBackoff(id)
	if giveup {
		db.logger.Error("ingest supervisor giving up after repeated freezes",
			zap.String("id", id),
			zap.Int("consecutive_freezes", consecutive))
		if db.metrics != nil {
			db.metrics.IngestSupervisorGiveup(id)
		}
		return
	}

	db.logger.Info("ingest supervisor scheduled restart",
		zap.String("id", id),
		zap.Int("consecutive_freezes", consecutive),
		zap.Duration("backoff", backoff))

	// Jitter is drawn from [0, backoff/2]. Keeps concurrent freezes
	// across multiple ids from all trying to restart in lockstep.
	// math/rand/v2 is deliberate — this is scheduling jitter, not a
	// security primitive, and crypto/rand would add failure modes
	// (syscall error handling) for no benefit.
	jitter := time.Duration(0)
	if backoff/2 > 0 {
		jitter = time.Duration(rand.Int64N(int64(backoff / 2))) //nolint:gosec // G404: non-security-sensitive jitter
	}
	select {
	case <-time.After(backoff + jitter):
	case <-db.ctx.Done():
		return
	}

	db.workersMu.Lock()
	if db.closed {
		db.workersMu.Unlock()
		return
	}
	if db.ingestWorkers[id] != frozen {
		// A user-initiated replace already installed a fresh handle
		// while we were waiting. Nothing for us to do.
		db.workersMu.Unlock()
		db.logger.Debug("ingest supervisor skipping restart; handle replaced",
			zap.String("id", id))
		return
	}
	// Drop the frozen entry directly. Its goroutine exited already
	// (we're downstream of that exit) and its handle.done is closed,
	// so no drain step is needed — unlike db.Ingest's public replace
	// loop, which must assume the existing worker is still running.
	delete(db.ingestWorkers, id)
	db.spawnIngestWorkerLocked(id, i)
	db.workersMu.Unlock()

	if db.metrics != nil {
		db.metrics.SetWorkerRunning("ingest", id, true)
		db.metrics.IngestFrozen(id, false)
		db.metrics.IngestRestart(id)
	}
}

// recordFreezeAndNextBackoff bumps the consecutive-freeze counter for
// id and returns the backoff to apply before the next restart. If the
// gap since the previous freeze exceeds ingestSupervisorHealthyWindow
// the counter is reset first — the worker ran cleanly long enough that
// this flap is a fresh episode, not a continuation. Returns giveup =
// true when the (post-increment) counter exceeds
// ingestSupervisorMaxAttempts; callers surface the giveup metric and
// skip the restart.
func (db *DB) recordFreezeAndNextBackoff(id string) (backoff time.Duration, consecutive int, giveup bool) {
	db.ingestSupervisorMu.Lock()
	defer db.ingestSupervisorMu.Unlock()

	st, ok := db.ingestSupervisorStates[id]
	if !ok {
		st = &ingestSupervisorState{backoff: db.ingestSupervisorInitialBackoff}
		db.ingestSupervisorStates[id] = st
	}
	now := time.Now()
	if !st.lastFreezeAt.IsZero() && now.Sub(st.lastFreezeAt) > db.ingestSupervisorHealthyWindow {
		st.consecutiveFreezes = 0
		st.backoff = db.ingestSupervisorInitialBackoff
	}
	st.consecutiveFreezes++
	st.lastFreezeAt = now

	if st.consecutiveFreezes > db.ingestSupervisorMaxAttempts {
		return 0, st.consecutiveFreezes, true
	}

	backoff = st.backoff
	st.backoff *= 2
	if st.backoff > db.ingestSupervisorMaxBackoff {
		st.backoff = db.ingestSupervisorMaxBackoff
	}
	return backoff, st.consecutiveFreezes, false
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

// AppliedIndex returns the highest log index that has been fully applied
// to local application state. Pass-through to Storage.AppliedIndex. Used
// by the /ready HTTP probe to gate readiness on this node having caught
// up; a freshly-started node reports 0 until at least one entry applies.
func (db *DB) AppliedIndex() uint64 {
	return db.storage.AppliedIndex()
}
