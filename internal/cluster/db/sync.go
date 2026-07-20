package db

import (
	"context"
	"errors"
	"io"
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

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

// syncBatch{MaxSize,MaxAge} are the DEFAULT batch limits when the Syncable
// implements BatchSyncable: proposals buffer until either MaxSize accumulate
// or MaxAge elapses since the first proposal in the batch, whichever comes
// first (a partial batch is also flushed on reader EOF). A syncable's
// CheckpointPolicy overrides these per-syncable (Every→size, MaxAge→age).
const (
	syncBatchMaxSize = 100
	syncBatchMaxAge  = 50 * time.Millisecond
)

// checkpointPolicyOf returns the syncable's configured checkpoint cadence, or
// the zero policy if it doesn't implement CheckpointConfigurable. The worker
// fills path-appropriate defaults (see syncSingle / syncBatch) for any zero
// field, so an unconfigured syncable runs exactly as before. The
// ModeAlwaysCurrent migration wrapper forwards this, so a wrapped syncable
// keeps its TOML cadence.
func checkpointPolicyOf(s cluster.Syncable) cluster.CheckpointPolicy {
	if cc, ok := s.(cluster.CheckpointConfigurable); ok {
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
		_ = db.sync(workerCtx, id, s)
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
func (db *DB) deleteSync(id string) {
	db.workersMu.Lock()
	handle, ok := db.syncWorkers[id]
	keepData := db.syncDeleteKeep[id]
	delete(db.syncDeleteKeep, id)
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
		return // no worker built on this node — nothing to tear down
	}

	// Release the deleted syncable's prepared statements. Node-local resource
	// cleanup — done on every node that built a worker (if it drained), before
	// and independent of the owner-only destination teardown below.
	db.closeDrainedSyncable(handle, id)

	if keepData || !db.isNode(id) {
		return // operator opted to keep the data, or this node isn't the owner
	}

	teardownable, ok := handle.syncable.(cluster.Teardownable)
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

func (db *DB) sync(ctx context.Context, id string, s cluster.Syncable) error {
	bs, isBatch := s.(cluster.BatchSyncable)
	if isBatch {
		return db.syncBatch(ctx, id, s, bs)
	}
	return db.syncSingle(ctx, id, s)
}

func (db *DB) syncSingle(ctx context.Context, id string, s cluster.Syncable) error {
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
	tracker := &stuckTracker{db: db, id: id}

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
			r = nil
			isNode = false
			retryActual = nil
			lastSeen, lastBumped = 0, 0
			pendingCount = 0
			tracker.cleared(ctx)
			progressed = true
		case !isNode && db.isNode(id):
			db.logger.Info("starting sync", zap.String("id", id))
			r = db.storage.Reader(id)
			isNode = true
			retryActual = nil
			lastSeen, lastBumped = 0, 0
			pendingCount = 0
			tracker.cleared(ctx)
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
					db.recordSyncDeadLetter(ctx, id, i, "manual", retryErr)
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
				db.fatalOnCorruptRead(id, readErr)
				db.logger.Warn("sync read error", zap.String("id", id), zap.Error(readErr))
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
				shouldSnapshot, syncErr := s.Sync(ctx, a)
				if db.metrics != nil {
					db.metrics.SyncCompleted(id, time.Since(syncStart))
				}
				if syncErr != nil {
					if errors.Is(syncErr, cluster.ErrPermanent) {
						if c, tripped := db.recordSyncPermanent(id); tripped {
							db.tripSyncBreaker(id, c, syncErr)
							return nil // park: hold the checkpoint, stop dead-lettering the topic
						}
						db.logger.Error("permanent sync error, skipping proposal",
							zap.String("id", id), zap.Uint64("index", i), zap.Error(syncErr))
						db.recordSyncPermanentError(ctx, id, i, syncErr)
						retryActual = nil
						retryErr = nil
						tracker.cleared(ctx)
					} else {
						db.logger.Warn("transient sync error, will retry",
							zap.String("id", id), zap.Uint64("index", i), zap.Error(syncErr))
						db.recordSyncTransientError(id)
						retryActual = a
						retryErr = syncErr
						// Publish (after the debounce) the index the worker is
						// blocked on so any node can report it and an operator
						// can skip it. Don't set progressed — the backoff slows
						// the retry loop.
						tracker.wedged(ctx, i, syncErr)
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

func (db *DB) syncBatch(ctx context.Context, id string, s cluster.Syncable, bs cluster.BatchSyncable) error {
	isNode := false
	var r ActualReader
	backoff := syncBackoffMin

	// Batch limits come from the syncable's CheckpointPolicy (Every→size,
	// MaxAge→age), each falling back to the package default when unset. One
	// bump per batch is already the cadence, so this is the whole of cadence
	// for the batch path: a bigger batch checkpoints less often and
	// re-delivers up to one batch on crash. See cluster.CheckpointPolicy.
	policy := checkpointPolicyOf(s)
	maxSize := policy.Every
	if maxSize < 1 {
		maxSize = syncBatchMaxSize
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
	tracker := &stuckTracker{db: db, id: id}

	flush := func() bool {
		if len(batch) == 0 {
			return true
		}

		syncStart := time.Now()
		shouldSnapshot, syncErr := bs.SyncBatch(ctx, batch)
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
			db.logger.Warn("transient batch sync error, will retry",
				zap.String("id", id), zap.Int("batch_size", len(batch)), zap.Error(syncErr))
			db.recordSyncTransientError(id)
			retryBatch = true
			// Publish (after the debounce) the head of the blocked batch so
			// an operator can dead-letter what the syncable is stuck on. A
			// batch fails atomically, so the head is the cursor; honoring the
			// request isolates the batch per-proposal (see the retryBatch
			// branch).
			tracker.wedged(ctx, batch[0].Index, syncErr)
			return false
		}

		lastIndex := batch[len(batch)-1].Index
		if shouldSnapshot {
			if err := db.proposeSyncableIndex(ctx, &cluster.SyncableIndex{ID: id, Index: lastIndex}); err != nil {
				// The bump did not durably apply (ctx canceled, or
				// ErrProposalUnknown/ErrProposalLost after a leader change
				// orphaned the bump). Keep the batch and retry the SyncBatch +
				// bump on the next iteration rather than advancing past an
				// unconfirmed index. Re-running the batch relies on the Syncable
				// contract's replay-idempotency requirement (see cluster.Syncable);
				// not advancing keeps recovery to at most one duplicate batch.
				db.logger.Warn("proposeSyncableIndex error, will retry batch",
					zap.String("id", id), zap.Error(err))
				retryBatch = true
				return false
			}
		}

		batch = batch[:0]
		retryBatch = false
		tracker.cleared(ctx)
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
			r = nil
			isNode = false
			batch = batch[:0]
			retryBatch = false
			tracker.cleared(ctx)
			progressed = true
		case !isNode && db.isNode(id):
			db.logger.Info("starting sync", zap.String("id", id))
			r = db.storage.Reader(id)
			isNode = true
			batch = batch[:0]
			retryBatch = false
			tracker.cleared(ctx)
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
				// Caught up. Flush any partial batch immediately.
				if len(batch) > 0 {
					if flush() {
						progressed = true
					}
				}
			case readErr != nil:
				db.fatalOnCorruptRead(id, readErr)
				db.logger.Warn("sync read error", zap.String("id", id), zap.Error(readErr))
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
		shouldSnapshot, syncErr := s.Sync(ctx, e)
		if db.metrics != nil {
			db.metrics.SyncCompleted(id, time.Since(syncStart))
		}
		if syncErr != nil {
			if errors.Is(syncErr, cluster.ErrPermanent) {
				if c, tripped := db.recordSyncPermanent(id); tripped {
					db.tripSyncBreaker(id, c, syncErr)
					return false // stop the batch; syncBatch parks (checks syncBreakerTripped)
				}
				db.logger.Error("permanent sync error, skipping proposal",
					zap.String("id", id), zap.Uint64("index", e.Index), zap.Error(syncErr))
				db.recordSyncPermanentError(ctx, id, e.Index, syncErr)
				continue
			}
			if transientSkipKind != "" {
				// Operator-requested isolation: skip the still-failing
				// proposal rather than re-blocking on it.
				db.logger.Warn("operator dead-letter: skipping proposal that still fails in isolation",
					zap.String("id", id), zap.Uint64("index", e.Index), zap.Error(syncErr))
				db.recordSyncDeadLetter(ctx, id, e.Index, transientSkipKind, syncErr)
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
// fatalOnCorruptRead fatal-exits the node when a sync read returned a corrupt
// (checksum-failed) committed event. Reader.Read does NOT advance past a corrupt
// entry, so the syncable would otherwise re-read the same seq forever; and the
// on-disk event log is untrustworthy. A mid-log corruption can't self-heal via
// raft — the node's matchIndex already covers that index, so AppendEntries never
// re-sends it, and an applied event sits downstream of the raft log entirely —
// so the node must be restarted and rebuilt from a healthy replica (single-node:
// restore/splice from a backup). This mirrors raft.go's fatal on a failed
// apply-committed-entry: a committed record we cannot process means the log is
// no longer trustworthy. `committed wal repair` confirms whether it is instead a
// truncatable torn tail. Non-corrupt (transient) read errors fall through to a
// warn-and-retry.
func (db *DB) fatalOnCorruptRead(id string, readErr error) {
	if errors.Is(readErr, cluster.ErrCorruptEntry) {
		db.logger.Fatal("corrupt event-log entry on sync read; the on-disk log is untrustworthy — rebuild this node from a healthy replica, or run `committed wal repair` to check for a torn tail (see docs/operations/rebuild.md)",
			zap.String("id", id), zap.Error(readErr))
	}
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
