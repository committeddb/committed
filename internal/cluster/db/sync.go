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

// syncBatch{MaxSize,MaxAge} control how db.sync batches proposals when
// the Syncable implements BatchSyncable. Proposals are buffered until
// either MaxSize proposals accumulate or MaxAge elapses since the first
// proposal in the batch, whichever comes first. A partial batch is also
// flushed immediately when the reader returns EOF (caught up).
const (
	syncBatchMaxSize = 100
	syncBatchMaxAge  = 50 * time.Millisecond
)

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
		<-existing.done
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
	handle := &workerHandle{cancel: cancel, done: make(chan struct{})}
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
		_ = db.sync(workerCtx, id, s)
	}()

	return nil
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
			tracker.cleared(ctx)
			progressed = true
		case !isNode && db.isNode(id):
			db.logger.Info("starting sync", zap.String("id", id))
			r = db.storage.Reader(id)
			isNode = true
			retryActual = nil
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
				// caught up; idle, will sleep below
			case readErr != nil:
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
					tracker.cleared(ctx)
					progressed = true
					break
				}
				// Pass the worker's ctx (not db.ctx) so a replace or
				// Close-driven cancellation propagates into the user's
				// Sync implementation. Without this, a slow Sync keeps
				// the worker alive past the registry replace, leaving
				// the new worker waiting on the old one's done channel.
				// Sync operations are expected to be idempotent (the
				// SQL dialect uses upsert), so the replacement worker
				// re-syncing the same proposal after a cancel is safe.
				syncStart := time.Now()
				shouldSnapshot, syncErr := s.Sync(ctx, a)
				if db.metrics != nil {
					db.metrics.SyncCompleted(id, time.Since(syncStart))
				}
				if syncErr != nil {
					if errors.Is(syncErr, cluster.ErrPermanent) {
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
				if shouldSnapshot {
					if err := db.proposeSyncableIndex(ctx, &cluster.SyncableIndex{ID: id, Index: i}); err != nil {
						// The bump did not durably apply — ctx canceled by a
						// replace/Close, or ErrProposalUnknown after a leader
						// change. Do NOT advance past this proposal: re-sync
						// and re-bump it on the next iteration so the persisted
						// index never trails the reader by more than the one
						// in-flight proposal. A crash here costs at most one
						// duplicate on recovery instead of a storm. Sync is
						// idempotent (contract), so the re-sync is safe; on ctx
						// cancellation the loop-top check exits before retrying.
						db.logger.Warn("proposeSyncableIndex error, will retry",
							zap.String("id", id), zap.Uint64("index", i), zap.Error(err))
						retryActual = a
						break
					}
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
				// ErrProposalUnknown after a leader change). Keep the batch
				// and retry the SyncBatch + bump on the next iteration rather
				// than advancing past an unconfirmed index. SyncBatch is
				// idempotent (contract), so re-running it is safe, and not
				// advancing keeps recovery to at most one duplicate batch.
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
				if len(batch) >= syncBatchMaxSize || time.Since(batchStart) >= syncBatchMaxAge {
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
			// was already committed via Sync's idempotent upsert).
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
				// prefix was already pushed downstream via the idempotent
				// Sync, so re-processing on retry is safe.
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
// the worker does NOT advance its index. The replacement worker re-syncs
// from the un-advanced (persisted) index. Sync is required to be
// idempotent at the sink level (SQL UPSERT), so re-syncing is safe.
// (Non-idempotent syncables have a separate latent issue — see
// .claude-scratch/tickets/sync-fail-fast-bump-tracking.md.)
//
// On a successful (durable) bump the round-trip latency is recorded to
// committed_sync_bump_duration_seconds so the extra cost is observable.
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
