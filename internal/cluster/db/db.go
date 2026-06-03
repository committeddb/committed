package db

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

import (
	"context"
	"errors"
	"fmt"
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

type Peers map[uint64]string

type DB struct {
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

	// syncStuckThreshold is how long a worker must be continuously blocked
	// retrying a transient error before it publishes a replicated
	// SyncableStuck record (and lights the stuck gauge). Debounced so normal
	// fast-recovering transients never flag; only a genuine wedge surfaces.
	// Default 30s; tests override via WithSyncStuckThreshold.
	syncStuckThreshold time.Duration

	maxProposalBytes uint64

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
		syncStuckThreshold:             cfg.syncStuckThreshold,
		ingestSupervisorStates:         make(map[string]*ingestSupervisorState),
		ingestSupervisorInitialBackoff: cfg.ingestSupervisorInitialBackoff,
		ingestSupervisorMaxBackoff:     cfg.ingestSupervisorMaxBackoff,
		ingestSupervisorMaxAttempts:    cfg.ingestSupervisorMaxAttempts,
		ingestSupervisorHealthyWindow:  cfg.ingestSupervisorHealthyWindow,
		maxProposalBytes:               cfg.maxProposalBytes,
		logger:                         cfg.logger,
		metrics:                        cfg.metrics,
	}

	// The applied notifier is wired into the raft Ready loop. After each
	// successful ApplyCommitted, raft.go calls db.notifyApplied with the
	// raw entry data so we can unmarshal once, look up the waiter by
	// p.RequestID, and signal it. This shape works uniformly for
	// wal.Storage (real apply) and testing.MemoryStorage (no-op apply) —
	// both go through the same raft.go iteration over rd.CommittedEntries.
	errorC, raft := newRaftWithOptions(id, rpeers, s, proposeC, confChangeC, db.notifyApplied, cfg.logger, cfg)

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

	return db
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

// AppliedIndex returns the highest log index that has been fully applied
// to local application state. Pass-through to Storage.AppliedIndex. Used
// by the /ready HTTP probe to gate readiness on this node having caught
// up; a freshly-started node reports 0 until at least one entry applies.
func (db *DB) AppliedIndex() uint64 {
	return db.storage.AppliedIndex()
}
