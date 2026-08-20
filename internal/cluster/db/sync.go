package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// callSync invokes the syncable implementation's Sync across the
// framework→implementation trust boundary, converting a panic into a TRANSIENT
// sync error instead of letting it unwind the process. A Sync panic originates
// in the implementation zone — a syncable's renderers, the migration wrapper,
// a driver — processing per-entry data: the same class the ingest side already
// converts at its boundary ("external input across the CDC trust boundary must
// never crash the node", see decodeLogicalMessage). Unrecovered, the panic
// killed the node and RestoreSyncableWorkers resumed into the SAME entry on
// restart — a deterministic panic became a whole-node crash-loop.
//
// As a transient error it instead enters the machinery that already exists for
// exactly this shape: retry, the stuck tracker's replicated + alertable signal,
// and the operator's manual dead-letter skip + replay. No new operational
// state, no new escape hatch. Transient — never permanent — because a panic is
// an unknown, and auto-dead-lettering would skip data on a guess (the
// stall-visibly posture stuck-syncables.md documents).
//
// PII: the returned error carries only the panic's TYPE — sync errors reach
// replicated stuck records, and a panic value can embed row data. The full
// value + stack go to the node-local log only.
//
// The worker FRAMEWORK outside this call deliberately stays fail-fast: a panic
// in committed's own reader/checkpoint logic is a core bug, the same posture
// as the apply path.
func (db *DB) callSync(ctx context.Context, id string, s cluster.Syncable, a *cluster.Actual) (snap cluster.ShouldSnapshot, err error) {
	defer func() {
		if r := recover(); r != nil {
			db.logger.Error("panic in syncable implementation; converted to a transient sync error (the worker retries; the stuck/skip flow applies)",
				zap.String("id", id), zap.Uint64("index", a.Index), zap.Any("panic", r), zap.Stack("stack"))
			snap = false
			err = fmt.Errorf("panic in syncable implementation (%T) at index %d; see the owning node's log for the value and stack", r, a.Index)
		}
	}()
	return s.Sync(ctx, a)
}

// callSyncBatch is callSync's batch twin — same boundary, same conversion. A
// batch-wide transient error retries the whole batch (and the stuck flow
// applies); if the operator skips, the batch fallback isolates per entry
// through callSync.
func (db *DB) callSyncBatch(ctx context.Context, id string, bs cluster.BatchSyncable, batch []*cluster.Actual) (snap bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			first, last := batch[0].Index, batch[len(batch)-1].Index
			db.logger.Error("panic in syncable implementation (batch); converted to a transient sync error (the worker retries; the stuck/skip flow applies)",
				zap.String("id", id), zap.Uint64("firstIndex", first), zap.Uint64("lastIndex", last),
				zap.Any("panic", r), zap.Stack("stack"))
			snap = false
			err = fmt.Errorf("panic in syncable implementation (%T) in batch [%d..%d]; see the owning node's log for the value and stack", r, first, last)
		}
	}()
	return bs.SyncBatch(ctx, batch)
}

// This file holds the sync worker core: registration (Sync), the
// single-vs-batch dispatch (sync), the two worker state machines
// (syncSingle, syncBatch + syncBatchFallback), and the success-path index
// bump (proposeSyncableIndex). The two cohesive subsystems the worker leans
// on live alongside it: the replicated stuck/skip debounce in
// stuck_tracker.go, and the dead-letter recording/query helpers in
// sync_dead_letter.go.

// syncBackoff{Min,Max} bound the polling interval for db.sync's idle
// loop. The worker has no event source to block on (the wal reader
// returns io.EOF when caught up rather than blocking on new entries),
// so without a backoff the loop spins on a sync.Mutex + atomic.Load
// at ~one CPU core per worker. The backoff doubles starting at Min on
// every consecutive idle iteration and caps at Max; any progress
// (state change, successful read, successful sync) resets it to Min.
//
// Trade-off: a freshly-committed entry takes up to syncBackoffMax to
// be picked up by an already-idle worker, but actively-syncing
// workers stay at syncBackoffMin and pay no measurable latency.
// 500ms is fine for the current "syncs trail the log by some bounded
// amount" semantics; if a future caller needs sub-millisecond sync
// latency, the right answer is option 3 from the audit (notification
// channel from ApplyCommitted), not lowering this constant.
const (
	syncBackoffMin = 1 * time.Millisecond
	syncBackoffMax = 500 * time.Millisecond
)

// Batch limits when the Syncable implements BatchSyncable: proposals buffer
// until min(Every, syncBatchCap) accumulate or MaxAge elapses since the first
// proposal in the batch, whichever comes first (a partial batch is also
// flushed on reader EOF). CheckpointPolicy.Every is the checkpoint CADENCE —
// it no longer inflates the sink-transaction size past syncBatchCap; see the
// derivation in syncBatch.
const (
	// syncBatchDefaultEvery is the default checkpoint cadence (proposals per
	// bump) when a batch syncable configures none. 2500 keeps out-of-box
	// replays off the bump-saturation cliff at a 2500-proposal crash
	// re-delivery window (the Syncable contract requires replay idempotency,
	// so keyed sinks absorb it). Configured syncables get exactly their Every.
	syncBatchDefaultEvery = 2500
	syncBatchMaxAge       = 50 * time.Millisecond
)

// syncBatchCap caps the sink-transaction size independently of the checkpoint
// cadence: batches amortize the sink's per-transaction fsync (~complete by a
// few hundred rows) while staying bounded for lock hold and failure-isolation
// granularity. A var, not a const, so tests can lower it to exercise
// multi-batch cadence accumulation cheaply.
var syncBatchCap = 500

// recoverStageState closes the gap between a syncable's NODE-LOCAL stage
// state and its replicated checkpoint before the worker consumes: after
// an ownership move (this node has no store, or a stale one) or a crash
// that lost the store's NoSync tail, folding forward from the checkpoint
// alone would silently produce incomplete aggregates. The design's
// checkpoint split: replay (frontier, checkpoint] folding WITHOUT sink
// emission (everything ≤ checkpoint is already durably applied), then
// consume normally. Lag, never loss — and loudly visible.
func (db *DB) recoverStageState(ctx context.Context, id string, rec cluster.StageRecoverer, checkpoint uint64) error {
	frontier, has, err := rec.StageFrontier()
	if err != nil {
		return err
	}
	if !has || frontier >= checkpoint {
		return nil
	}
	db.logger.Warn("stage state is behind the checkpoint — re-deriving before consuming (an ownership move or a lost NoSync tail; outputs below the checkpoint are already applied, so this pass folds without emitting)",
		zap.String("id", id), zap.Uint64("stage_frontier", frontier), zap.Uint64("checkpoint", checkpoint))
	r := db.storage.Reader("") // from index 0; entries ≤ frontier are skipped below
	folded := 0
	for {
		// The scan can cover the whole log; without this check a
		// delete/replace's cancel cannot interrupt it, the drain times
		// out, and the worker gets ABANDONED mid-recovery (the field
		// zombie: the store then closes under the scan).
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		a, err := r.Read()
		if err != nil {
			break // EOF (or read error — the main loop surfaces persistent ones)
		}
		if a.Index <= frontier {
			continue
		}
		if a.Index > checkpoint {
			break
		}
		if err := rec.FoldStagesOnly(a); err != nil {
			return err
		}
		folded++
	}
	db.logger.Info("stage state re-derived to the checkpoint", zap.String("id", id),
		zap.Int("actuals_folded", folded), zap.Uint64("checkpoint", checkpoint))
	return nil
}

// stageRecoveryFailed triages a recovery failure and reports whether the
// worker should EXIT (true) or back off and retry (false). Exit on
// cancellation (a delete/replace/Close interrupted the scan) and on a
// DELETED syncable — the field zombie was a deleted syncable's worker
// hot-looping "database not open" recovery attempts forever against a
// store its own teardown had closed; deletion must end the loop no
// matter how the worker got here.
func (db *DB) stageRecoveryFailed(ctx context.Context, id string, err error) bool {
	if ctx.Err() != nil {
		db.logger.Info("stage-state recovery interrupted by shutdown/replace; worker exiting",
			zap.String("id", id))
		return true
	}
	if exists, eerr := db.storage.SyncableExists(id); eerr == nil && !exists {
		db.logger.Info("stage-state recovery aborted: syncable deleted; worker exiting",
			zap.String("id", id), zap.Error(err))
		return true
	}
	db.logger.Error("stage-state recovery failed; backing off before retrying rather than consuming with incomplete state",
		zap.String("id", id), zap.Error(err))
	return false
}

// checkpointPolicyOf returns the syncable's configured checkpoint cadence, or
// the zero policy if it doesn't implement CheckpointConfigurable. The worker
// fills path-appropriate defaults (see syncSingle / syncBatch) for any zero
// field, so an unconfigured syncable runs exactly as before. The
// ModeAlwaysCurrent migration wrapper forwards this, so a wrapped syncable
// keeps its TOML cadence.
func checkpointPolicyOf(s cluster.Syncable) cluster.CheckpointPolicy {
	// Unwrap-chain resolution, outermost-first: the migration wrapper's own
	// deliberate CheckpointPolicy forward still wins (SyncableAs finds the
	// wrapper's implementation before the inner's), and any future wrapper
	// WITHOUT that forward no longer silently loses the cadence.
	if cc, ok := cluster.SyncableAs[cluster.CheckpointConfigurable](s); ok {
		return cc.CheckpointPolicy()
	}
	return cluster.CheckpointPolicy{}
}

// Sync registers a Syncable to run as a worker for the given ID. See
// db.Ingest for the registry semantics — Sync is the syncable-side
// counterpart and behaves identically: a duplicate call for the same
// ID cancels and replaces the existing worker, the worker context is
// derived from db.ctx (not the caller's ctx), and db.Close drains
// every registered worker before tearing the raft layer down.
//
// The Syncable passed here is taken as-is. Registration-time
// decorators (the always-current mode wrapper in
// internal/cluster/migration) are applied by the wal layer before the
// syncable reaches this method — db.sync doesn't know or care which
// mode a syncable is running under.
func (db *DB) Sync(_ context.Context, id string, s cluster.Syncable) error {
	if db.safeMode {
		// Safe mode (WithSafeMode): the config is stored and visible, but no
		// worker starts. Release what the parse built — a held syncable would
		// otherwise leak its destination resources on every apply/reconcile.
		// Bounded for the same reason closeDrainedSyncable is: Close writes to
		// the destination and this runs on the config-listener path.
		db.logger.Warn("SAFE MODE: sync worker held; delete or fix the config over the API, then restart without COMMITTED_SAFE_MODE to resume",
			zap.String("id", id))
		if s != nil {
			if err, completed := runBounded(db.workerDrainTimeout, s.Close); !completed {
				db.logger.Warn("held syncable close did not return in time", zap.String("id", id))
			} else if err != nil {
				db.logger.Warn("held syncable close failed", zap.String("id", id), zap.Error(err))
			}
		}
		return nil
	}
	db.workersMu.Lock()
	if db.closed {
		db.workersMu.Unlock()
		return ErrClosed
	}
	// See db.Ingest for the rationale behind the loop and the
	// re-check of db.closed after each wait.
	replaced := false
	for {
		existing, ok := db.syncWorkers[id]
		if !ok {
			break
		}
		replaced = true
		existing.cancel()
		db.workersMu.Unlock()
		if !waitDone(existing.done, db.workerDrainTimeout) {
			db.logger.Warn("sync replace: prior worker did not exit in time; abandoning it (wedged on its destination?) and proceeding",
				zap.String("id", id), zap.Duration("timeout", db.workerDrainTimeout))
		}
		// Release the superseded syncable's prepared statements (only if it
		// drained, so we don't race a wedged worker) — otherwise every re-POST
		// leaks a statement set on the shared pool.
		db.closeDrainedSyncable(existing, id)
		db.workersMu.Lock()
		if db.closed {
			db.workersMu.Unlock()
			return ErrClosed
		}
		if db.syncWorkers[id] == existing {
			delete(db.syncWorkers, id)
		}
	}

	if replaced && db.metrics != nil {
		db.metrics.WorkerReplaced("sync", id)
	}

	// cancel ownership passes to the workerHandle. It is invoked by
	// db.Close (see db.go:~390) and by the replace path above when a
	// duplicate Sync call supersedes this worker, so the cancel is
	// not leaked. gosec can't see through the handle indirection.
	workerCtx, cancel := context.WithCancel(db.ctx) //nolint:gosec // G118: cancel owned by workerHandle
	handle := &workerHandle{cancel: cancel, done: make(chan struct{}), syncable: s}
	db.syncWorkers[id] = handle
	db.workersMu.Unlock()

	if db.metrics != nil {
		db.metrics.SetWorkerRunning("sync", id, true)
	}

	go func() {
		defer func() {
			if db.metrics != nil {
				db.metrics.SetWorkerRunning("sync", id, false)
			}
			close(handle.done)
		}()
		db.resetSyncBreaker(id) // fresh breaker state for this worker run (e.g. after a replace)
		_ = db.sync(workerCtx, id, s, handle)
	}()

	return nil
}

// deleteSync tears down a syncable that was removed from the log: it cancels
// and drains the local worker, then — on the owner node only — tears down the
// syncable's destination. It is the delete-side counterpart of Sync, driven by
// the apply path (deleteSyncable → the sync channel → listenForSyncables).
//
// Two planes, as the rebuild/delete design requires:
//   - Worker cancel is node-local and idempotent: every node that built the
//     config has a worker, and every node runs this on apply, so each stops
//     its own goroutine. A node with no worker (degraded build) is a no-op.
//   - The destination teardown is the destructive side effect (a SQL syncable
//     DROPs its table), so it is gated on db.isNode(id) and run live only —
//     never reconstructed from replay. A catching-up/non-owner node has isNode
//     false and skips it; the owner tears down exactly once.
//
// The teardown is best-effort: the logical deletion already succeeded via
// consensus, so a failure only leaves orphaned destination state an operator
// can remove — it must never fail or panic. keepData (set by DeleteSyncable
// before the propose) skips the teardown entirely.
//
// Ownership note: by the time this runs the config is already deleted, so
// db.storage.Node(id) is 0 and isNode resolves to "this node is the leader."
// The leader tears down using its own already-built syncable handle, which is
// also the node the DELETE request landed on (writes proxy to the leader), so
// its keepData intent is the one that applies.
// reconcileRetryMin/Max bound the backoff for retrying a reconcile whose config
// LISTING failed (a genuine bbolt error — per-config decode/parse failures are
// tolerated by the closure). A listing failure leaves the data plane
// un-reconciled and the only other reconcile triggers are restart/snapshot, so
// the reconciler retries rather than silently stranding the node.
const (
	reconcileRetryMin = 50 * time.Millisecond
	reconcileRetryMax = 5 * time.Second
)

// reconcileSyncWorkers converges the running sync workers to the CURRENT
// config set: the closure (executed here, on the listener goroutine,
// serialized with the apply path's events) parses every config in bbolt; each
// listed worker is installed (replace-by-id) and every RUNNING worker whose
// id is absent is cancelled — the compacted-delete zombie: a delete that
// arrived inside an InstallSnapshot never ran the apply-path delete event, so
// nothing else will ever cancel it. Cancellation here is cancel-and-local-
// cleanup ONLY (keepData=true): reconcile never touches destinations — the
// live delete path did (or the owner will do) any teardown.
func (db *DB) reconcileSyncWorkers(list func() ([]*SyncableWithID, error)) {
	backoff := reconcileRetryMin
	var parsed []*SyncableWithID
	for {
		var err error
		parsed, err = list()
		if err == nil {
			break
		}
		// A genuine listing failure (not a per-config degrade, which the closure
		// tolerates). Do NOT silently return — the only other reconcile triggers
		// are restart/snapshot, so a transient failure would strand every worker
		// until then. Log loudly and retry until it succeeds or the db closes.
		db.logger.Error("sync reconcile: listing configs failed; sync workers not reconciled (data plane degraded), retrying",
			zap.Error(err))
		select {
		case <-db.ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, reconcileRetryMax)
	}
	present := make(map[string]struct{}, len(parsed))
	installed, degraded := 0, 0
	for _, sw := range parsed {
		present[sw.ID] = struct{}{}
		// A nil Syncable marks an existing-but-degraded config: it is PRESENT
		// (its worker is not cancelled) but not reconfigured — the prior good
		// worker keeps delivering until the config parses again.
		if sw.Syncable == nil {
			degraded++
			continue
		}
		if err := db.Sync(context.Background(), sw.ID, sw.Syncable); err != nil {
			return // ErrClosed: db shutting down
		}
		installed++
	}
	cancelled := 0
	for _, id := range db.syncWorkerIDs() {
		if _, ok := present[id]; !ok {
			db.logger.Warn("sync reconcile: cancelling worker for a config that no longer exists (deleted, incl. via snapshot)",
				zap.String("id", id))
			db.deleteSync(id, true)
			cancelled++
		}
	}
	db.logger.Info("sync reconcile complete",
		zap.Int("installed", installed), zap.Int("degraded", degraded), zap.Int("cancelled", cancelled))
}

// syncWorkerIDs snapshots the registered sync worker ids.
func (db *DB) syncWorkerIDs() []string {
	db.workersMu.Lock()
	defer db.workersMu.Unlock()
	ids := make([]string, 0, len(db.syncWorkers))
	for id := range db.syncWorkers {
		ids = append(ids, id)
	}
	return ids
}

func (db *DB) deleteSync(id string, keepData bool) {
	db.workersMu.Lock()
	handle, ok := db.syncWorkers[id]
	if ok {
		handle.cancel()
		db.workersMu.Unlock()
		if !waitDone(handle.done, db.workerDrainTimeout) {
			db.logger.Warn("delete sync: worker did not exit in time; abandoning it (wedged on its destination?) and proceeding",
				zap.String("id", id), zap.Duration("timeout", db.workerDrainTimeout))
		}
		db.workersMu.Lock()
		if db.syncWorkers[id] == handle {
			delete(db.syncWorkers, id)
		}
	}
	db.workersMu.Unlock()

	if !ok || handle.syncable == nil {
		// No worker built on this node — nothing to tear down. In safe mode
		// that is EVERY delete; outside it this is a DEGRADED config's
		// delete (its build failed, so no worker held a destination
		// handle). Either way the destination table, if any, survives —
		// say so rather than skipping silently (the same silent-skip
		// shape that hid the wrapper-masking bug).
		if db.safeMode {
			db.logger.Warn("safe mode: syncable deleted; owner-side destination teardown skipped (no worker was built) — the destination table, if any, remains",
				zap.String("id", id))
		} else {
			db.logger.Warn("syncable deleted but no worker was built on this node (degraded config?); owner-side destination teardown skipped — the destination table, if any, remains and a same-name re-POST will land on it",
				zap.String("id", id))
		}
		return
	}

	// Release the deleted syncable's prepared statements. Node-local resource
	// cleanup — done on every node that built a worker (if it drained), before
	// and independent of the owner-only destination teardown below.
	db.closeDrainedSyncable(handle, id)

	if keepData || !db.isNode(id) {
		return // operator opted to keep the data, or this node isn't the owner
	}

	// Resolved through the Unwrap chain: an always-current syncable's
	// Teardownable lives on the INNER syncable behind the migration
	// wrapper (the field incident: deleted projections' tables survived
	// because a bare assertion here silently failed on the wrapper).
	teardownable, ok := cluster.SyncableAs[cluster.Teardownable](handle.syncable)
	if !ok {
		return // syncable owns no external destination state
	}
	// Bounded (runBounded): this runs on the single-threaded config listener,
	// and the destination that wedged the worker above is the same one Teardown
	// is about to talk to — an unbounded DROP there would park the listener and
	// stall the raft apply loop on its next config send.
	if err, completed := runBounded(db.workerDrainTimeout, teardownable.Teardown); !completed {
		db.logger.Error("syncable deleted but destination teardown did not return in time (unreachable destination?); abandoning it (orphaned destination state; remove it manually)",
			zap.String("id", id), zap.Duration("timeout", db.workerDrainTimeout))
	} else if err != nil {
		// Best-effort: the logical delete already committed. Log loudly and
		// move on — the worst case is orphaned destination state.
		db.logger.Error("syncable deleted but destination teardown failed (orphaned destination state; remove it manually)",
			zap.String("id", id), zap.Error(err))
	}
}

func (db *DB) sync(ctx context.Context, id string, s cluster.Syncable, handle *workerHandle) error {
	bs, isBatch := s.(cluster.BatchSyncable)
	if isBatch {
		return db.syncBatch(ctx, id, s, bs, handle)
	}
	return db.syncSingle(ctx, id, s, handle)
}

func (db *DB) syncSingle(ctx context.Context, id string, s cluster.Syncable, handle *workerHandle) error {
	isNode := false
	var r ActualReader
	backoff := syncBackoffMin

	// Checkpoint cadence. every is how many successful syncs accumulate
	// before a mid-stream bump (default 1 = checkpoint every sync, today's
	// behavior); maxAge optionally bounds how long a pending checkpoint may
	// sit (0 = no age bound — EOF and the count still flush, so an idle
	// syncable never lags). See cluster.CheckpointPolicy.
	policy := checkpointPolicyOf(s)
	every := policy.Every
	if every < 1 {
		every = 1
	}
	maxAge := policy.MaxAge

	// retryActual holds an Actual that failed with a transient error. On
	// the next iteration the worker retries the same Actual instead of
	// reading a new one from the log — transient errors retry forever, so
	// the worker stalls (visibly) rather than losing data. retryErr is the
	// most recent transient error, recorded as the dead-letter message if
	// an operator skips the wedged Actual. Both are cleared on success,
	// permanent error, manual skip, or leadership transition.
	var retryActual *cluster.Actual
	var retryErr error
	tracker := db.newStuckTracker(id)

	// lastSeen is the highest index this worker has fully DECIDED on in the
	// current leadership stint — synced, skipped because it wasn't this
	// syncable's topic (shouldSnapshot=false), or dead-lettered. It is the
	// consumed head. lastBumped is the highest index durably checkpointed
	// (via a successful proposeSyncableIndex) in this stint. The gap between
	// them is the trailing run of entries the worker read and cheaply skipped
	// without bumping; the io.EOF handler closes it with one bump to lastSeen
	// so a selective syncable's lag reads 0 at rest (consumed-head semantics —
	// see syncable-progress-lag). Both reset on a leadership transition; the
	// batch path already advances to the consumed head and needs no analogue.
	var lastSeen, lastBumped uint64

	// pendingCount is the number of successful syncs (validated
	// shouldSnapshot=true boundaries) accumulated since the last durable
	// checkpoint; pendingSince is when the first of them happened. The
	// mid-stream bump fires once pendingCount reaches `every` OR maxAge
	// elapses — thinning checkpoints per the cadence (sync-checkpoint-cadence).
	// Reset on a leadership transition and after each successful bump.
	var pendingCount int
	var pendingSince time.Time

	for {
		// Cheap non-blocking ctx check at the top so a cancellation
		// observed mid-iteration short-circuits the next round.
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// Run one iteration of the state machine and decide whether
		// the iteration made progress. Progress is defined as: state
		// transition (gain/lose leadership) OR a successful read+sync
		// of a proposal. Progress resets the backoff. Idle iterations
		// (no leader, or leader-but-EOF) double the backoff.
		progressed := false
		switch {
		case isNode && !db.isNode(id):
			db.logger.Info("stopping sync", zap.String("id", id))
			handle.activeReader.Store(nil)
			r = nil
			isNode = false
			retryActual = nil
			lastSeen, lastBumped = 0, 0
			pendingCount = 0
			// Do NOT clear the stuck record on leadership loss. The record is
			// cluster-wide replicated state — losing leadership does not unstick
			// the syncable; the new owner adopts it (or re-wedges) and clears it on
			// progress. Deleting it here would flap it (and, via the derived gauge,
			// drop then re-raise the alert) on every leadership change.
			progressed = true
		case !isNode && db.isNode(id):
			r = db.storage.Reader(id)
			handle.activeReader.Store(&readerRef{r: r})
			// checkpoint/head on the start line turn every future "young mirror
			// frozen at 0" question into a one-line answer: this worker begins a
			// scan of head-checkpoint entries (the field incident this serves was
			// undiagnosable precisely for lack of it).
			cp, head, _ := db.SyncableProgress(id)
			db.logger.Info("starting sync", zap.String("id", id),
				zap.Uint64("checkpoint", cp), zap.Uint64("head", head))
			if rec, ok := cluster.SyncableAs[cluster.StageRecoverer](s); ok {
				if err := db.recoverStageState(ctx, id, rec, cp); err != nil {
					if exit := db.stageRecoveryFailed(ctx, id, err); exit {
						return nil
					}
					break // idle backoff, then retry — never a hot loop
				}
			}
			isNode = true
			retryActual = nil
			lastSeen, lastBumped = 0, 0
			pendingCount = 0
			// Re-derive the tracker from the applied record. This worker goroutine
			// outlives a leadership flap, so its in-memory published/index can be
			// stale (another node cleared or re-indexed the record while we idled).
			// resync adopts a present record and resets on an absent one, so a
			// genuine re-wedge re-publishes instead of being suppressed as already-
			// published — see stuckTracker.resync. This is NOT a clear: the record
			// itself is untouched, so a still-stuck syncable never flaps.
			tracker.resync()
			// Do NOT clear the stuck record on startup. A replacement/restart
			// worker adopts any replicated SyncableStuck record (published=true);
			// clearing here would DELETE it before the worker re-tries the wedge,
			// so a still-stuck syncable would flap (delete → re-wedge → re-publish
			// after the debounce), 409ing the operator's skip/dead-letter lever in
			// between. The record is deleted only on genuine PROGRESS past the
			// wedge (the cleared() on a successful sync below), per the tracker's
			// own contract. The resumed worker re-reads AT the wedge index (the
			// checkpoint sits at N-1), so progress means the wedge itself cleared.
			progressed = true
		case isNode && db.isNode(id):
			var i uint64
			var a *cluster.Actual
			var readErr error

			if retryActual != nil {
				a = retryActual
				i = a.Index
				// The worker is blocked retrying this Actual. If an
				// operator asked to dead-letter what it's stuck on, honor
				// it now: record the skip (kind "manual", carrying the last
				// transient error) and advance past it instead of syncing.
				if tracker.skipRequested(ctx, i) {
					db.logger.Warn("operator dead-letter: skipping wedged proposal",
						zap.String("id", id), zap.Uint64("index", i), zap.Error(retryErr))
					if !db.recordSyncDeadLetter(ctx, id, i, "manual", retryErr) {
						// Record orphaned (leader flap / ctx). Stay wedged —
						// the skip request is still pending (honored() not
						// called), so the next iteration re-records. Never
						// advance past a skip with no durable record.
						break
					}
					retryActual = nil
					retryErr = nil
					lastSeen = i // decided (dead-lettered); part of the consumed head
					tracker.honored(ctx)
					progressed = true
					break
				}
			} else {
				a, readErr = r.Read()
				if readErr == nil {
					i = a.Index
				}
			}

			switch {
			case readErr == io.EOF:
				// Caught up. If the consumed head (lastSeen) is past the last
				// index durably checkpointed (lastBumped), advance the
				// checkpoint to it with one bump. For a selective single
				// syncable this closes the trailing run of other-topic /
				// dead-lettered entries it read and cheaply skipped without
				// bumping, so its lag reads 0 at rest instead of a phantom
				// backlog (consumed-head semantics — see syncable-progress-lag).
				//
				// Safe: advancing to lastSeen never skips THIS syncable's
				// un-synced data. Every entry ≤ lastSeen was synced
				// (downstream-committed), was not this syncable's topic
				// (nothing to sync), or was dead-lettered (durably recorded;
				// the restart-time HasSyncableDeadLetter check re-excludes it
				// regardless of where the checkpoint sits). The only entries it
				// moves past are ones that were never this syncable's work.
				//
				// Cost: one bump per EOF only while a gap exists — bounded by
				// EOF frequency, not entry count, so it does not reintroduce
				// per-entry bumps and does not grow the log while idle (once
				// lastBumped == lastSeen, subsequent EOFs are no-ops).
				//
				// This EOF flush is also the cadence's caught-up trigger: it
				// persists any pending sub-`every` tail the count hasn't flushed
				// yet, so a low-traffic syncable never lags its checkpoint
				// forever (sync-checkpoint-cadence constraint 2).
				if lastSeen > lastBumped {
					if err := db.proposeSyncableIndex(ctx, &cluster.SyncableIndex{ID: id, Index: lastSeen}); err != nil {
						// Bump orphaned (ctx canceled, or leader change). Leave
						// lastBumped behind; the next EOF retries. Don't set
						// progressed — back off rather than spin on a wedged bump.
						db.logger.Warn("EOF checkpoint bump error, will retry",
							zap.String("id", id), zap.Uint64("index", lastSeen), zap.Error(err))
					} else {
						lastBumped = lastSeen
						pendingCount = 0
						progressed = true
					}
				}
			case readErr != nil:
				db.logSyncReadError(id, readErr)
			default:
				// A proposal already dead-lettered (a permanent skip, or an
				// operator's manual skip) stays skipped across restarts:
				// exclude it without re-syncing. This is what makes a manual
				// skip durable — unlike a permanent error, the syncable won't
				// re-declare a transient failure skippable on the re-read
				// after restart, so the durable record is the source of truth.
				if dl, derr := db.storage.HasSyncableDeadLetter(id, i); derr != nil {
					db.logger.Warn("dead-letter lookup failed; proceeding to sync",
						zap.String("id", id), zap.Uint64("index", i), zap.Error(derr))
				} else if dl {
					db.logger.Info("skipping already dead-lettered proposal",
						zap.String("id", id), zap.Uint64("index", i))
					retryActual = nil
					retryErr = nil
					lastSeen = i // decided (already dead-lettered); part of the consumed head
					tracker.cleared(ctx)
					progressed = true
					break
				}
				// Pass the worker's ctx (not db.ctx) so a replace or
				// Close-driven cancellation propagates into the user's
				// Sync implementation. Without this, a slow Sync keeps
				// the worker alive past the registry replace, leaving
				// the new worker waiting on the old one's done channel.
				// Re-syncing the same proposal on the replacement worker
				// after a cancel relies on the Syncable contract's
				// replay-idempotency requirement (the SQL dialects satisfy
				// it via upsert; non-idempotent sinks are the operator's
				// responsibility — see cluster.Syncable).
				syncStart := time.Now()
				shouldSnapshot, syncErr := db.callSync(ctx, id, s, a)
				if db.metrics != nil {
					db.metrics.SyncCompleted(id, time.Since(syncStart))
				}
				if syncErr != nil {
					if errors.Is(syncErr, cluster.ErrPermanent) {
						if c, tripped := db.recordSyncPermanent(id, i); tripped {
							db.tripSyncBreaker(id, c, syncErr)
							db.publishSyncableParked(ctx, id, i, syncErr)
							return nil // park: hold the checkpoint, stop dead-lettering the topic
						}
						db.logger.Error("permanent sync error, skipping proposal",
							zap.String("id", id), zap.Uint64("index", i), zap.Error(syncErr))
						if !db.recordSyncPermanentError(ctx, id, i, syncErr) {
							// Record orphaned. Hold position (lastSeen must not
							// reach i) and re-run decide+record next iteration —
							// re-invoking Sync is safe under the contract's
							// replay-idempotency requirement, and a permanent
							// error is deterministic by declaration.
							retryActual = a
							retryErr = syncErr
							break
						}
						retryActual = nil
						retryErr = nil
						tracker.cleared(ctx)
					} else {
						db.recordSyncTransientError(id)
						retryActual = a
						retryErr = syncErr
						// Publish (after the debounce) the index the worker is
						// blocked on so any node can report it and an operator
						// can skip it. Don't set progressed — the backoff slows
						// the retry loop. Log the transient error only on the wedge
						// transition (wedged returns true once per new index), not on
						// every retry — that would flood the log for a stuck worker.
						if tracker.wedged(ctx, i, syncErr) {
							db.logger.Warn("transient sync error, will retry",
								zap.String("id", id), zap.Uint64("index", i), zap.Error(syncErr))
						}
						break
					}
				} else {
					retryActual = nil
					retryErr = nil
					tracker.cleared(ctx)
				}
				// Decided this entry (synced — matched or topic-skipped via
				// shouldSnapshot=false — or permanently errored just above):
				// it's part of the consumed head, even if no checkpoint is
				// written here. The io.EOF handler closes any lastSeen−lastBumped
				// gap when the worker catches up.
				lastSeen = i

				// Count validated checkpoint boundaries (shouldSnapshot=true)
				// toward the cadence; the mid-stream bump THINS them. It fires
				// once `every` have accumulated, or once maxAge has elapsed since
				// the first pending one (maxAge==0 disables the age bound — the
				// count and the EOF flush still bound staleness). Skips don't
				// count, but an age-triggered flush still advances the checkpoint
				// past them since it targets lastSeen, the consumed head. At the
				// default every==1 this bumps on every matched sync, exactly as
				// before.
				if shouldSnapshot {
					if pendingCount == 0 {
						pendingSince = time.Now()
					}
					pendingCount++
				}
				if pendingCount > 0 && (pendingCount >= every || (maxAge > 0 && time.Since(pendingSince) >= maxAge)) {
					if err := db.proposeSyncableIndex(ctx, &cluster.SyncableIndex{ID: id, Index: lastSeen}); err != nil {
						// The bump did not durably apply — ctx canceled by a
						// replace/Close, or ErrProposalUnknown/ErrProposalLost
						// after a leader change orphaned the bump. Do NOT advance:
						// retry the same entry + bump on the next iteration (we
						// don't read ahead, so the synced-but-unbumped set can't
						// exceed the cadence). A crash here re-delivers at most
						// `every` already-synced proposals. The re-sync relies on
						// the Syncable contract's replay-idempotency requirement
						// (see cluster.Syncable); on ctx cancellation the loop-top
						// check exits before retrying.
						db.logger.Warn("proposeSyncableIndex error, will retry",
							zap.String("id", id), zap.Uint64("index", lastSeen), zap.Error(err))
						retryActual = a
						break
					}
					lastBumped = lastSeen // durably checkpointed through here
					pendingCount = 0
				}
				progressed = true
			}
			// case !isNode && !db.isNode(id): no work, no state change.
			// fall through to backoff sleep.
		}

		if progressed {
			backoff = syncBackoffMin
			continue
		}

		// Idle iteration. Sleep with backoff, but stay interruptible
		// by ctx cancellation so registry replace and Close get prompt
		// shutdowns.
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil
		}
		backoff *= 2
		if backoff > syncBackoffMax {
			backoff = syncBackoffMax
		}
	}
}

func (db *DB) syncBatch(ctx context.Context, id string, s cluster.Syncable, bs cluster.BatchSyncable, handle *workerHandle) error {
	isNode := false
	var r ActualReader
	backoff := syncBackoffMin

	// CheckpointPolicy.Every is the CHECKPOINT CADENCE (its documented
	// meaning: persist once per Every successful syncs, crash re-delivers at
	// most that many), and the sink-transaction size is derived SEPARATELY as
	// min(Every, syncBatchCap). The two used to be conflated — Every doubled
	// as the batch size, so one bump fired per batch — which made a 17-sink
	// replay saturate the raft fsync pipeline with ~53 checkpoint proposals/s
	// (85% of every worker cycle was waiting on its bump's round trip;
	// aggregate throughput ~1.3K rows/s with the hardware idle). Field A/B:
	// raising the cadence 500→5000 alone measured 5-8.5x. Decoupling keeps
	// sink transactions at the sink-optimal cap while bumps thin to the
	// configured cadence; the single-flush path always worked this way, so
	// both paths now honor the same contract. For Every > syncBatchCap the
	// bump lands on the first VALIDATED batch boundary at or past Every, so
	// the re-delivery window is Every rounded up to the enclosing batch
	// (<= Every + syncBatchCap - 1); for Every <= syncBatchCap behavior is
	// bit-for-bit the old one (batch = Every, bump per batch).
	policy := checkpointPolicyOf(s)
	every := policy.Every
	if every < 1 {
		every = syncBatchDefaultEvery
	}
	maxSize := every
	if maxSize > syncBatchCap {
		maxSize = syncBatchCap
	}
	maxAge := policy.MaxAge
	if maxAge <= 0 {
		maxAge = syncBatchMaxAge
	}

	// batch accumulates Actuals until a flush; each Actual carries its own
	// Index, so the flush can advance SyncableIndex to the last in the batch.
	var batch []*cluster.Actual
	var batchStart time.Time
	// retryBatch is set when a flush fails with a transient error. The
	// next iteration retries the flush instead of reading more proposals.
	retryBatch := false
	// Checkpoint-cadence state (see the derivation comment above): the bump
	// fires on the first VALIDATED batch boundary once at least `every`
	// proposals have synced since the last persisted checkpoint.
	// syncedSinceBump counts flushed proposals not yet covered by a bump;
	// pendingBumpIndex is the latest shouldSnapshot=true boundary awaiting
	// persistence (0 = none) — a vetoed batch accumulates count but cannot
	// carry the bump ("cadence can only thin which VALID boundaries get
	// persisted", cluster.CheckpointPolicy).
	syncedSinceBump := 0
	var pendingBumpIndex uint64
	tracker := db.newStuckTracker(id)

	flush := func() bool {
		if len(batch) == 0 {
			return true
		}

		syncStart := time.Now()
		shouldSnapshot, syncErr := db.callSyncBatch(ctx, id, bs, batch)
		if db.metrics != nil {
			db.metrics.SyncCompleted(id, time.Since(syncStart))
		}

		if syncErr != nil {
			if errors.Is(syncErr, cluster.ErrPermanent) {
				// A permanent error from the batch means at least one
				// proposal is bad. Fall back to per-proposal Sync on
				// this batch to isolate the offending proposal(s).
				db.logger.Warn("permanent batch error, falling back to per-proposal sync",
					zap.String("id", id), zap.Int("batch_size", len(batch)), zap.Error(syncErr))
				ok := db.syncBatchFallback(ctx, id, s, batch, "")
				if ok {
					batch = batch[:0]
					retryBatch = false
					tracker.cleared(ctx)
				}
				return ok
			}
			// Transient error — don't clear the batch so it will be
			// retried on the next iteration.
			db.recordSyncTransientError(id)
			retryBatch = true
			// Publish (after the debounce) the head of the blocked batch so
			// an operator can dead-letter what the syncable is stuck on. A
			// batch fails atomically, so the head is the cursor; honoring the
			// request isolates the batch per-proposal (see the retryBatch
			// branch). Log only on the wedge transition, not every retry.
			if tracker.wedged(ctx, batch[0].Index, syncErr) {
				db.logger.Warn("transient batch sync error, will retry",
					zap.String("id", id), zap.Int("batch_size", len(batch)), zap.Error(syncErr))
			}
			return false
		}

		// Cadence accounting: this batch's proposals count toward the stride,
		// and a validated boundary refreshes the pending bump index. The bump
		// itself fires only once `every` proposals have accumulated — the
		// checkpoint cadence, decoupled from the batch size.
		prospectiveCount := syncedSinceBump + len(batch)
		prospectivePending := pendingBumpIndex
		if shouldSnapshot {
			prospectivePending = batch[len(batch)-1].Index
		}
		if prospectivePending != 0 && prospectiveCount >= every {
			if err := db.proposeSyncableIndex(ctx, &cluster.SyncableIndex{ID: id, Index: prospectivePending}); err != nil {
				// The bump did not durably apply (ctx canceled, or
				// ErrProposalUnknown/ErrProposalLost after a leader change
				// orphaned the bump). Keep the batch and retry the SyncBatch +
				// bump on the next iteration rather than advancing past an
				// unconfirmed index; the accumulators are deliberately NOT
				// updated here, so the retried batch is counted exactly once.
				// Re-running the batch relies on the Syncable contract's
				// replay-idempotency requirement (see cluster.Syncable).
				db.logger.Warn("proposeSyncableIndex error, will retry batch",
					zap.String("id", id), zap.Error(err))
				retryBatch = true
				return false
			}
			syncedSinceBump = 0
			pendingBumpIndex = 0
		} else {
			syncedSinceBump = prospectiveCount
			pendingBumpIndex = prospectivePending
		}

		batch = batch[:0]
		retryBatch = false
		tracker.cleared(ctx)
		return true
	}

	// bumpPending persists the latest validated boundary regardless of the
	// cadence stride — the reader-EOF flush ("persist whenever it catches
	// up"), so a low-traffic syncable's checkpoint never lags behind its
	// consumed head for long. A failure is retried at the next EOF or stride
	// boundary; the pending index stays armed.
	bumpPending := func() bool {
		if pendingBumpIndex == 0 {
			return false
		}
		if err := db.proposeSyncableIndex(ctx, &cluster.SyncableIndex{ID: id, Index: pendingBumpIndex}); err != nil {
			db.logger.Warn("proposeSyncableIndex error at EOF, will retry",
				zap.String("id", id), zap.Error(err))
			return false
		}
		syncedSinceBump = 0
		pendingBumpIndex = 0
		return true
	}

	for {
		// A prior flush's per-proposal fallback may have tripped the circuit
		// breaker; park the worker (checkpoint held) rather than spin on the
		// systematically-failing batch. The reset is per worker launch, so a
		// replacement after a config fix starts fresh.
		if db.syncBreakerTripped(id) {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		progressed := false
		switch {
		case isNode && !db.isNode(id):
			db.logger.Info("stopping sync", zap.String("id", id))
			handle.activeReader.Store(nil)
			r = nil
			isNode = false
			batch = batch[:0]
			retryBatch = false
			// Drop the cadence accumulators without bumping: we must not
			// propose while not the owner, and the new owner re-derives its
			// position from the durable checkpoint — the un-persisted tail
			// (< every) is simply re-delivered there, within the documented
			// re-delivery window.
			syncedSinceBump = 0
			pendingBumpIndex = 0
			// Do NOT clear the stuck record on leadership loss — it is cluster-wide;
			// the new owner clears it on progress. See the single-flush worker.
			progressed = true
		case !isNode && db.isNode(id):
			r = db.storage.Reader(id)
			handle.activeReader.Store(&readerRef{r: r})
			// checkpoint/head on the start line turn every future "young mirror
			// frozen at 0" question into a one-line answer: this worker begins a
			// scan of head-checkpoint entries (the field incident this serves was
			// undiagnosable precisely for lack of it).
			cp, head, _ := db.SyncableProgress(id)
			db.logger.Info("starting sync", zap.String("id", id),
				zap.Uint64("checkpoint", cp), zap.Uint64("head", head))
			if rec, ok := cluster.SyncableAs[cluster.StageRecoverer](s); ok {
				if err := db.recoverStageState(ctx, id, rec, cp); err != nil {
					if exit := db.stageRecoveryFailed(ctx, id, err); exit {
						return nil
					}
					break // idle backoff, then retry — never a hot loop
				}
			}
			isNode = true
			batch = batch[:0]
			retryBatch = false
			// Re-derive the tracker from the applied record — the worker outlives a
			// leadership flap, so its in-memory published/index can be stale. See
			// the single-flush worker's start branch and stuckTracker.resync.
			tracker.resync()
			// Do NOT clear the stuck record on startup — the adopted record must
			// survive until genuine progress past the wedge, or a still-stuck
			// replacement flaps it. See the single-flush worker's start branch.
			progressed = true
		case isNode && db.isNode(id):
			// If a previous flush failed with a transient error, retry
			// the flush before reading more proposals.
			if retryBatch {
				// Honor an operator's request to dead-letter what the batch
				// is stuck on: isolate it per-proposal, dead-lettering (kind
				// "manual") only the proposals that still fail and advancing
				// the rest. One operator action clears one wedged batch.
				if len(batch) > 0 && tracker.skipRequested(ctx, batch[0].Index) {
					db.logger.Warn("operator dead-letter: isolating wedged batch",
						zap.String("id", id), zap.Int("batch_size", len(batch)), zap.Uint64("head_index", batch[0].Index))
					if db.syncBatchFallback(ctx, id, s, batch, "manual") {
						batch = batch[:0]
						retryBatch = false
						tracker.honored(ctx)
						progressed = true
					}
					break
				}
				if flush() {
					progressed = true
				}
				break
			}

			a, readErr := r.Read()
			switch {
			case readErr == io.EOF:
				// Caught up. Flush any partial batch immediately, then persist
				// any sub-stride pending boundary — the EOF flush of the
				// CheckpointPolicy contract, which keeps a caught-up
				// syncable's checkpoint at its consumed head no matter how
				// coarse the cadence.
				if len(batch) > 0 {
					if flush() {
						progressed = true
					}
				}
				if !retryBatch && bumpPending() {
					progressed = true
				}
			case readErr != nil:
				db.logSyncReadError(id, readErr)
			default:
				i := a.Index
				// Exclude a proposal already dead-lettered (a permanent skip
				// or an operator's manual skip) from the batch, so the skip
				// survives restart and a manually-isolated poison proposal
				// stays out of future batches — a batch fails atomically, so a
				// re-included poison would re-wedge the whole batch.
				if dl, derr := db.storage.HasSyncableDeadLetter(id, i); derr != nil {
					db.logger.Warn("dead-letter lookup failed; including proposal in batch",
						zap.String("id", id), zap.Uint64("index", i), zap.Error(derr))
				} else if dl {
					db.logger.Info("skipping already dead-lettered proposal",
						zap.String("id", id), zap.Uint64("index", i))
					progressed = true
					break
				}
				if len(batch) == 0 {
					batchStart = time.Now()
				}
				batch = append(batch, a)

				// Flush if batch is full or has aged past the deadline.
				if len(batch) >= maxSize || time.Since(batchStart) >= maxAge {
					if flush() {
						progressed = true
					}
				} else {
					// More room in the batch — immediately try to read
					// more without sleeping.
					progressed = true
				}
			}
		}

		if progressed {
			backoff = syncBackoffMin
			continue
		}

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil
		}
		backoff *= 2
		if backoff > syncBackoffMax {
			backoff = syncBackoffMax
		}
	}
}

// syncBatchFallback processes a failed batch one proposal at a time using
// the per-proposal Sync method. Permanent errors always skip (dead-letter)
// the offending proposal. The transient-error reaction depends on
// transientSkipKind:
//
//   - "" (permanent-isolation fallback): stop the fallback and leave the
//     remaining proposals for the caller to retry — a transient blip while
//     isolating a permanent error shouldn't drop data.
//   - non-empty (operator manual dead-letter): the operator chose to give up
//     on what the syncable is stuck on, so dead-letter the still-failing
//     proposal with that kind ("manual") and continue, isolating the bad
//     proposal(s) while letting the healthy ones in the batch through.
func (db *DB) syncBatchFallback(ctx context.Context, id string, s cluster.Syncable, entries []*cluster.Actual, transientSkipKind string) bool {
	for _, e := range entries {
		syncStart := time.Now()
		shouldSnapshot, syncErr := db.callSync(ctx, id, s, e)
		if db.metrics != nil {
			db.metrics.SyncCompleted(id, time.Since(syncStart))
		}
		if syncErr != nil {
			if errors.Is(syncErr, cluster.ErrPermanent) {
				if c, tripped := db.recordSyncPermanent(id, e.Index); tripped {
					db.tripSyncBreaker(id, c, syncErr)
					db.publishSyncableParked(ctx, id, e.Index, syncErr)
					return false // stop the batch; syncBatch parks (checks syncBreakerTripped)
				}
				db.logger.Error("permanent sync error, skipping proposal",
					zap.String("id", id), zap.Uint64("index", e.Index), zap.Error(syncErr))
				if !db.recordSyncPermanentError(ctx, id, e.Index, syncErr) {
					// Record orphaned — same posture as the bump failure below:
					// stop the fallback and leave the batch for the caller to
					// retry rather than advancing past an unrecorded skip
					// (replay-idempotency covers the re-pushed prefix).
					return false
				}
				continue
			}
			if transientSkipKind != "" {
				// Operator-requested isolation: skip the still-failing
				// proposal rather than re-blocking on it.
				db.logger.Warn("operator dead-letter: skipping proposal that still fails in isolation",
					zap.String("id", id), zap.Uint64("index", e.Index), zap.Error(syncErr))
				if !db.recordSyncDeadLetter(ctx, id, e.Index, transientSkipKind, syncErr) {
					return false // stop; retry re-records (see the permanent twin above)
				}
				continue
			}
			// Transient error in fallback — stop here. The caller
			// should not retry this batch (the successful prefix
			// was already pushed downstream; re-pushing it relies on
			// the Syncable contract's replay-idempotency requirement).
			db.logger.Warn("transient sync error in fallback, stopping",
				zap.String("id", id), zap.Uint64("index", e.Index), zap.Error(syncErr))
			db.recordSyncTransientError(id)
			return false
		}
		if shouldSnapshot {
			if err := db.proposeSyncableIndex(ctx, &cluster.SyncableIndex{ID: id, Index: e.Index}); err != nil {
				// The bump did not durably apply. Stop the fallback and
				// leave the remaining batch for the caller to retry rather
				// than advancing past an unconfirmed index. The successful
				// prefix was already pushed downstream, so re-processing it
				// on retry relies on the Syncable contract's replay-idempotency
				// requirement (see cluster.Syncable).
				db.logger.Warn("proposeSyncableIndex error in fallback, stopping",
					zap.String("id", id), zap.Uint64("index", e.Index), zap.Error(err))
				return false
			}
		}
	}
	return true
}

// proposeSyncableIndex bumps the persisted SyncableIndex for a
// syncable after a successful Sync, and BLOCKS until that bump is
// durably applied (the appliedIndex/SyncableIndex bucket fsynced via
// the apply path) or until ctx is canceled / a leader change orphans
// the proposal. The cost is one Raft round-trip per bump; in exchange,
// recovery is deterministic.
//
// This used to be fire-and-forget (proposeAndDiscardAck on db.ctx),
// which let an arbitrary number of bumps sit un-applied: a crash
// between the Sync returning and the bumps applying made the restarted
// worker re-deliver every proposal since the last *persisted* index — a
// duplicate storm. Blocking caps recovery at one duplicate: the worker
// never advances past a proposal whose bump hasn't durably landed.
//
// ctx is the SYNC WORKER's context, not db.ctx. On a registry replace
// or Close the worker ctx is canceled; Propose then returns ctx.Err and
// the worker does NOT advance its index. A leader flap can likewise
// orphan the bump's log entry — Propose returns ErrProposalUnknown /
// ErrProposalLost — with the same "do not advance" outcome. Either way
// the replacement worker re-syncs from the un-advanced (persisted) index.
// Re-syncing the replayed range is safe ONLY because the Syncable contract
// requires downstream idempotency under replay (see the cluster.Syncable
// doc); a non-idempotent sink will double-emit, which is the operator's
// responsibility today. The opt-in mechanism to bound that is tracked in
// .claude-scratch/tickets/sync-two-phase-syncable.md.
//
// On a successful (durable) bump the round-trip latency is recorded to
// committed_sync_bump_duration_seconds so the extra cost is observable.
// logSyncReadError reports a sync read failure without ever killing the
// process — the corruption posture is LOUD, ALIVE, REPAIRABLE. The unit of
// failure matches the unit of damage: one unreadable entry wedges the
// syncables that need it (Reader.Read never advances past a corrupt entry, so
// nothing is silently skipped and the checkpoint holds), while the node stays
// up serving raft, ingest, the API, and every other syncable. That running
// cluster is the operator's debugging instrument — status, metrics
// (committed_wal_corrupt_entries counts every corrupt read), and these logs —
// and the shutdown for the repair happens on the operator's schedule, not
// mid-crashloop with no API window (the post-scrub hollow-segment incident).
// Process-fatal remains reserved for continuing-compounds-damage cases
// (raft.go's failed apply-committed-entry, where proceeding diverges state).
//
// Both corruption flavors take this wedge, with different log shapes:
//
//   - cluster.ErrCorruptEntry (a CRC mismatch inside one entry's frame): the
//     entry's bytes are wrong, the log is damaged, and a mid-log corruption
//     can't self-heal via raft — the node's matchIndex already covers that
//     index so AppendEntries never re-sends it, and the event log sits
//     downstream of the raft log entirely. Logged at Error with the repair
//     guidance: rebuild from a healthy replica, or `committed wal repair` to
//     check for a truncatable torn tail. (This branch fatal-exited before
//     0.7.6; converted per the posture above — a CRC hit near a checkpoint
//     had the same no-API-window crashloop shape as the mis-tiled incident.)
//   - anything else, including the forked wal's structural ErrCorrupt (a
//     hollow or mis-tiled segment file) and transient read errors: the
//     generic warn, retried under the worker backoff.
//
// The retry cadence is bounded by syncBackoffMax, so a wedged syncable logs
// at ~2/s, matching the mis-tiled flavor's shipped behavior. Pinned by
// TestCorruptEntryReadWedgesInsteadOfFatal and
// TestMisTiledSegmentReadDoesNotFatal.
func (db *DB) logSyncReadError(id string, readErr error) {
	if errors.Is(readErr, cluster.ErrCorruptEntry) {
		db.logger.Error("corrupt event-log entry on sync read; this syncable is wedged at the corrupt entry (nothing is skipped) and the node stays up — rebuild this node from a healthy replica, or run `committed wal repair` to check for a torn tail (see docs/operations/rebuild.md)",
			zap.String("id", id), zap.Error(readErr))
		return
	}
	db.logger.Warn("sync read error", zap.String("id", id), zap.Error(readErr))
}

func (db *DB) proposeSyncableIndex(ctx context.Context, i *cluster.SyncableIndex) error {
	entity, err := cluster.NewUpsertSyncableIndexEntity(i)
	if err != nil {
		return err
	}

	start := time.Now()
	err = db.Propose(ctx, &cluster.Proposal{Entities: []*cluster.Entity{entity}})
	if err == nil && db.metrics != nil {
		db.metrics.SyncBumpCompleted(time.Since(start))
	}
	return err
}

// syncableProgressReporter is the optional Storage extension that exposes the
// two numbers SyncableProgress needs: a syncable's persisted checkpoint
// (GetSyncableIndex) and the global data-entry head (DataEventIndex).
// wal.Storage implements it; the in-memory test doubles do not (progress is a
// wal.Storage feature, exercised by wal-backed tests), so SyncableProgress
// reports zeros on them rather than failing — same optional-interface shape as
// scrubBacklogReporter.
type syncableProgressReporter interface {
	GetSyncableIndex(id string) (uint64, error)
	DataEventIndex() uint64
}

// SyncableProgress returns the syncable's persisted checkpoint (the consumed
// head it has synced / topic-skipped / dead-lettered through) and the local
// data head (DataEventIndex). The HTTP status handler turns these into
// lag = max(0, head − checkpoint) and caught_up. Both are O(1) local reads,
// answerable on any node without a leader hop. A never-checkpointed syncable
// reports checkpoint 0 (so lag == head). On a storage that doesn't track
// progress (the in-memory test double) it reports (0, 0, nil); production
// always uses wal.Storage. See cluster.Cluster.SyncableProgress.
func (db *DB) SyncableProgress(id string) (checkpoint, head uint64, err error) {
	r, ok := db.storage.(syncableProgressReporter)
	if !ok {
		return 0, 0, nil
	}
	checkpoint, err = r.GetSyncableIndex(id)
	if err != nil {
		return 0, 0, err
	}
	return checkpoint, r.DataEventIndex(), nil
}

// SyncableOwner returns the raft node ID that owns syncable id's worker: the
// pinned node when the config names one (storage.Node), otherwise the current
// leader (0 when no leader is known). Derived from replicated state, so it
// answers identically on any node — the HTTP status handler reports it
// unconditionally as ownerNode and uses it to route the opt-in readPosition
// proxy to the owner.
func (db *DB) SyncableOwner(id string) uint64 {
	if n := db.storage.Node(id); n != 0 {
		return n
	}
	return db.Leader()
}

// SyncableStageKeyCounts reports the per-stage output key counts of
// syncable id's worker ON THIS NODE (nil, false when no worker is
// registered here or the syncable declares no stages). Owner-local like
// SyncableReadPosition — the stage store lives with the worker — so the
// HTTP layer proxies to the owner for the any-node answer.
func (db *DB) SyncableStageKeyCounts(id string) (map[string]int, bool) {
	db.workersMu.Lock()
	handle, ok := db.syncWorkers[id]
	db.workersMu.Unlock()
	if !ok || handle.syncable == nil {
		return nil, false
	}
	in, ok := cluster.SyncableAs[cluster.StageIntrospector](handle.syncable)
	if !ok {
		return nil, false
	}
	counts, err := in.StageKeyCounts()
	if err != nil || counts == nil {
		return nil, false
	}
	return counts, true
}

// SyncableStageKeyExists probes one stage output key of syncable id's
// worker ON THIS NODE (ok=false: no worker registered here, or no
// stages; err: the stage name is not declared). Owner-local like the
// counts — the HTTP layer proxies to the owner.
func (db *DB) SyncableStageKeyExists(id, stage, key string) (bool, bool, error) {
	db.workersMu.Lock()
	handle, ok := db.syncWorkers[id]
	db.workersMu.Unlock()
	if !ok || handle.syncable == nil {
		return false, false, nil
	}
	in, ok := cluster.SyncableAs[cluster.StageIntrospector](handle.syncable)
	if !ok {
		return false, false, nil
	}
	exists, err := in.StageKeyExists(stage, key)
	if err != nil {
		return false, true, err
	}
	return exists, true, nil
}

// SyncableReadPosition reports the live scan position of syncable id's worker
// ON THIS NODE: the raft index of the last log entry the worker's reader
// examined, advancing per entry scanned — including entries skipped as other
// topics' — so a long foreign-topic wade is visible as motion, not a frozen
// checkpoint. False means this node has no live position: no worker
// registered, the worker idle as a non-owner (no reader published), or a
// reader without the optional Position capability. Owner-local by
// construction — non-owner workers hold no reader — so the HTTP layer
// proxies the call to SyncableOwner's node for the any-node answer.
func (db *DB) SyncableReadPosition(id string) (uint64, bool) {
	db.workersMu.Lock()
	handle, ok := db.syncWorkers[id]
	db.workersMu.Unlock()
	if !ok {
		return 0, false
	}
	ref := handle.activeReader.Load()
	if ref == nil {
		return 0, false
	}
	p, ok := ref.r.(interface{ Position() uint64 })
	if !ok {
		return 0, false
	}
	return p.Position(), true
}
