package db

import (
	"context"
	"errors"
	"io"
	"time"

	"go.uber.org/zap"

	"github.com/philborlin/committed/internal/cluster"
)

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

// recordSyncDeadLetter emits the error metric for `kind` and durably
// dead-letters the skipped proposal so an operator can later query (and,
// once replay lands, re-drive) it. Best-effort on the dead-letter write:
// the proposal is skipped regardless, so a failed record propose (ctx
// canceled by replace/Close, or a leader change) is logged, not fatal.
// `kind` is "permanent" (Sync returned ErrPermanent) or "manual" (an
// operator skipped a syncable wedged on a transient error). ctx is the
// sync worker's context.
func (db *DB) recordSyncDeadLetter(ctx context.Context, id string, index uint64, kind string, syncErr error) {
	if db.metrics != nil {
		db.metrics.SyncError(id, kind)
	}
	msg := "operator dead-letter"
	if syncErr != nil {
		msg = syncErr.Error()
	}
	dl := &cluster.SyncableDeadLetter{
		ID:                id,
		Index:             index,
		TimestampUnixNano: time.Now().UnixNano(),
		Kind:              kind,
		Message:           truncateDeadLetterMessage(msg),
	}
	if err := db.proposeSyncableDeadLetter(ctx, dl); err != nil {
		db.logger.Warn("dead-letter record not persisted (best-effort; proposal still skipped)",
			zap.String("id", id), zap.Uint64("index", index), zap.String("kind", kind), zap.Error(err))
	}
}

// recordSyncPermanentError dead-letters a proposal that Sync rejected with a
// permanent error. Thin wrapper over recordSyncDeadLetter for the
// "permanent" kind.
func (db *DB) recordSyncPermanentError(ctx context.Context, id string, index uint64, syncErr error) {
	db.recordSyncDeadLetter(ctx, id, index, "permanent", syncErr)
}

// recordSyncTransientError emits the transient-error metric. Each
// occurrence counts, so a worker stuck retrying a transient failure
// shows a rising counter and a fresh last-error timestamp.
func (db *DB) recordSyncTransientError(id string) {
	if db.metrics != nil {
		db.metrics.SyncError(id, "transient")
	}
}

// stuckTracker manages a worker's replicated "blocked on index N" status —
// the node-agnostic half of the manual dead-letter flow. It debounces the
// flag (publishing only after the worker has been wedged for
// db.syncStuckThreshold), publishes/clears the SyncableStuck record through
// Raft so every node can see the stall, drives the stuck gauge, and reads the
// operator's SyncableSkipRequest. One per worker goroutine; all of its Raft
// proposes are best-effort (a failure just defers visibility to the next
// retry, never the worker's control flow).
type stuckTracker struct {
	db        *DB
	id        string
	since     time.Time // when the worker first wedged on `index`; zero = not stuck
	index     uint64
	published bool
}

// wedged is called on every transient failure of the proposal at `index`. It
// (re)starts the debounce when the wedged proposal changes and publishes the
// replicated stuck record once the worker has been blocked past the threshold.
func (t *stuckTracker) wedged(ctx context.Context, index uint64, lastErr error) {
	if t.since.IsZero() || t.index != index {
		t.since = time.Now()
		t.index = index
		t.published = false
	}
	if t.published || time.Since(t.since) < t.db.syncStuckThreshold {
		return
	}
	msg := ""
	if lastErr != nil {
		msg = truncateDeadLetterMessage(lastErr.Error())
	}
	s := &cluster.SyncableStuck{ID: t.id, Index: index, SinceUnixNano: t.since.UnixNano(), Message: msg}
	if err := t.db.proposeSyncableStuck(ctx, s); err != nil {
		t.db.logger.Warn("publish stuck status failed (will retry)",
			zap.String("id", t.id), zap.Uint64("index", index), zap.Error(err))
		return
	}
	t.published = true
	if t.db.metrics != nil {
		t.db.metrics.SetSyncStuck(t.id, true)
	}
}

// skipRequested reports whether an operator has asked the worker to skip the
// proposal at `index` (matching the stuck record it published). A request for
// a different index is stale (the worker moved on) and is dropped. Returns
// false until a stuck record has been published, since a request can only
// target a published index.
func (t *stuckTracker) skipRequested(ctx context.Context, index uint64) bool {
	if !t.published {
		return false
	}
	req, ok, err := t.db.storage.SyncableSkipRequest(t.id)
	if err != nil || !ok {
		return false
	}
	if req.Index != index {
		_ = t.db.proposeDeleteSyncableSkipRequest(ctx, t.id)
		return false
	}
	return true
}

// cleared is called when the worker makes progress, unsticks, or stops. It
// clears the published stuck record (no-op if nothing was published) and
// resets the debounce.
func (t *stuckTracker) cleared(ctx context.Context) {
	if t.published {
		if err := t.db.proposeDeleteSyncableStuck(ctx, t.id); err != nil {
			t.db.logger.Warn("clear stuck status failed", zap.String("id", t.id), zap.Error(err))
		}
		if t.db.metrics != nil {
			t.db.metrics.SetSyncStuck(t.id, false)
		}
	}
	t.since = time.Time{}
	t.index = 0
	t.published = false
}

// honored is called after the worker skips a proposal in response to a skip
// request: it clears the request as well as the stuck record.
func (t *stuckTracker) honored(ctx context.Context) {
	_ = t.db.proposeDeleteSyncableSkipRequest(ctx, t.id)
	t.cleared(ctx)
}

func (db *DB) syncSingle(ctx context.Context, id string, s cluster.Syncable) error {
	isNode := false
	var r ProposalReader
	backoff := syncBackoffMin

	// retryIndex and retryProposal hold a proposal that failed with a
	// transient error. On the next iteration the worker retries the same
	// proposal instead of reading a new one from the log — transient
	// errors retry forever, so the worker stalls (visibly) rather than
	// losing data. retryErr is the most recent transient error, recorded
	// as the dead-letter message if an operator skips the wedged proposal.
	// All three are cleared on success, permanent error, manual skip, or
	// leadership transition.
	var retryIndex uint64
	var retryProposal *cluster.Proposal
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
			retryProposal = nil
			tracker.cleared(ctx)
			progressed = true
		case !isNode && db.isNode(id):
			db.logger.Info("starting sync", zap.String("id", id))
			r = db.storage.Reader(id)
			isNode = true
			retryProposal = nil
			tracker.cleared(ctx)
			progressed = true
		case isNode && db.isNode(id):
			var i uint64
			var p *cluster.Proposal
			var readErr error

			if retryProposal != nil {
				i, p = retryIndex, retryProposal
				// The worker is blocked retrying this proposal. If an
				// operator asked to dead-letter what it's stuck on, honor
				// it now: record the skip (kind "manual", carrying the last
				// transient error) and advance past it instead of syncing.
				if tracker.skipRequested(ctx, i) {
					db.logger.Warn("operator dead-letter: skipping wedged proposal",
						zap.String("id", id), zap.Uint64("index", i), zap.Error(retryErr))
					db.recordSyncDeadLetter(ctx, id, i, "manual", retryErr)
					retryProposal = nil
					retryErr = nil
					tracker.honored(ctx)
					progressed = true
					break
				}
			} else {
				i, p, readErr = r.Read()
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
					retryProposal = nil
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
				shouldSnapshot, syncErr := s.Sync(ctx, p)
				if db.metrics != nil {
					db.metrics.SyncCompleted(id, time.Since(syncStart))
				}
				if syncErr != nil {
					if errors.Is(syncErr, cluster.ErrPermanent) {
						db.logger.Error("permanent sync error, skipping proposal",
							zap.String("id", id), zap.Uint64("index", i), zap.Error(syncErr))
						db.recordSyncPermanentError(ctx, id, i, syncErr)
						retryProposal = nil
						retryErr = nil
						tracker.cleared(ctx)
					} else {
						db.logger.Warn("transient sync error, will retry",
							zap.String("id", id), zap.Uint64("index", i), zap.Error(syncErr))
						db.recordSyncTransientError(id)
						retryIndex = i
						retryProposal = p
						retryErr = syncErr
						// Publish (after the debounce) the index the worker is
						// blocked on so any node can report it and an operator
						// can skip it. Don't set progressed — the backoff slows
						// the retry loop.
						tracker.wedged(ctx, i, syncErr)
						break
					}
				} else {
					retryProposal = nil
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
						retryIndex = i
						retryProposal = p
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

// batchEntry pairs a proposal with its WAL index so the batch flush
// can advance SyncableIndex to the last index in the batch.
type batchEntry struct {
	index    uint64
	proposal *cluster.Proposal
}

func (db *DB) syncBatch(ctx context.Context, id string, s cluster.Syncable, bs cluster.BatchSyncable) error {
	isNode := false
	var r ProposalReader
	backoff := syncBackoffMin

	var batch []batchEntry
	var batchStart time.Time
	// retryBatch is set when a flush fails with a transient error. The
	// next iteration retries the flush instead of reading more proposals.
	retryBatch := false
	tracker := &stuckTracker{db: db, id: id}

	flush := func() bool {
		if len(batch) == 0 {
			return true
		}

		ps := make([]*cluster.Proposal, len(batch))
		for i, e := range batch {
			ps[i] = e.proposal
		}

		syncStart := time.Now()
		shouldSnapshot, syncErr := bs.SyncBatch(ctx, ps)
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
			tracker.wedged(ctx, batch[0].index, syncErr)
			return false
		}

		lastIndex := batch[len(batch)-1].index
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
				if len(batch) > 0 && tracker.skipRequested(ctx, batch[0].index) {
					db.logger.Warn("operator dead-letter: isolating wedged batch",
						zap.String("id", id), zap.Int("batch_size", len(batch)), zap.Uint64("head_index", batch[0].index))
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

			i, p, readErr := r.Read()
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
				batch = append(batch, batchEntry{index: i, proposal: p})

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
func (db *DB) syncBatchFallback(ctx context.Context, id string, s cluster.Syncable, entries []batchEntry, transientSkipKind string) bool {
	for _, e := range entries {
		syncStart := time.Now()
		shouldSnapshot, syncErr := s.Sync(ctx, e.proposal)
		if db.metrics != nil {
			db.metrics.SyncCompleted(id, time.Since(syncStart))
		}
		if syncErr != nil {
			if errors.Is(syncErr, cluster.ErrPermanent) {
				db.logger.Error("permanent sync error, skipping proposal",
					zap.String("id", id), zap.Uint64("index", e.index), zap.Error(syncErr))
				db.recordSyncPermanentError(ctx, id, e.index, syncErr)
				continue
			}
			if transientSkipKind != "" {
				// Operator-requested isolation: skip the still-failing
				// proposal rather than re-blocking on it.
				db.logger.Warn("operator dead-letter: skipping proposal that still fails in isolation",
					zap.String("id", id), zap.Uint64("index", e.index), zap.Error(syncErr))
				db.recordSyncDeadLetter(ctx, id, e.index, transientSkipKind, syncErr)
				continue
			}
			// Transient error in fallback — stop here. The caller
			// should not retry this batch (the successful prefix
			// was already committed via Sync's idempotent upsert).
			db.logger.Warn("transient sync error in fallback, stopping",
				zap.String("id", id), zap.Uint64("index", e.index), zap.Error(syncErr))
			db.recordSyncTransientError(id)
			return false
		}
		if shouldSnapshot {
			if err := db.proposeSyncableIndex(ctx, &cluster.SyncableIndex{ID: id, Index: e.index}); err != nil {
				// The bump did not durably apply. Stop the fallback and
				// leave the remaining batch for the caller to retry rather
				// than advancing past an unconfirmed index. The successful
				// prefix was already pushed downstream via the idempotent
				// Sync, so re-processing on retry is safe.
				db.logger.Warn("proposeSyncableIndex error in fallback, stopping",
					zap.String("id", id), zap.Uint64("index", e.index), zap.Error(err))
				return false
			}
		}
	}
	return true
}
