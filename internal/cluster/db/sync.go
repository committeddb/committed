package db

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/philborlin/committed/internal/cluster"
	"go.uber.org/zap"
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

	workerCtx, cancel := context.WithCancel(db.ctx)
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
	var r ProposalReader
	backoff := syncBackoffMin

	// retryIndex and retryProposal hold a proposal that failed with a
	// transient error. On the next iteration the worker retries the same
	// proposal instead of reading a new one from the log. Cleared on
	// success, permanent error, or leadership transition.
	var retryIndex uint64
	var retryProposal *cluster.Proposal

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
			progressed = true
		case !isNode && db.isNode(id):
			db.logger.Info("starting sync", zap.String("id", id))
			r = db.storage.Reader(id)
			isNode = true
			retryProposal = nil
			progressed = true
		case isNode && db.isNode(id):
			var i uint64
			var p *cluster.Proposal
			var readErr error

			if retryProposal != nil {
				i, p = retryIndex, retryProposal
			} else {
				i, p, readErr = r.Read()
			}

			switch {
			case readErr == io.EOF:
				// caught up; idle, will sleep below
			case readErr != nil:
				db.logger.Warn("sync read error", zap.String("id", id), zap.Error(readErr))
			default:
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
						retryProposal = nil
					} else {
						db.logger.Warn("transient sync error, will retry",
							zap.String("id", id), zap.Uint64("index", i), zap.Error(syncErr))
						retryIndex = i
						retryProposal = p
						// Don't set progressed — the backoff will
						// slow the retry loop.
						break
					}
				} else {
					retryProposal = nil
				}
				if shouldSnapshot {
					err := db.proposeSyncableIndex(ctx, &cluster.SyncableIndex{ID: id, Index: i})
					if err != nil {
						db.logger.Warn("proposeSyncableIndex error", zap.String("id", id), zap.Error(err))
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
				ok := db.syncBatchFallback(ctx, id, s, batch)
				if ok {
					batch = batch[:0]
					retryBatch = false
				}
				return ok
			}
			// Transient error — don't clear the batch so it will be
			// retried on the next iteration.
			db.logger.Warn("transient batch sync error, will retry",
				zap.String("id", id), zap.Int("batch_size", len(batch)), zap.Error(syncErr))
			retryBatch = true
			return false
		}

		lastIndex := batch[len(batch)-1].index
		if shouldSnapshot {
			err := db.proposeSyncableIndex(ctx, &cluster.SyncableIndex{ID: id, Index: lastIndex})
			if err != nil {
				db.logger.Warn("proposeSyncableIndex error", zap.String("id", id), zap.Error(err))
			}
		}

		batch = batch[:0]
		retryBatch = false
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
			progressed = true
		case !isNode && db.isNode(id):
			db.logger.Info("starting sync", zap.String("id", id))
			r = db.storage.Reader(id)
			isNode = true
			batch = batch[:0]
			retryBatch = false
			progressed = true
		case isNode && db.isNode(id):
			// If a previous flush failed with a transient error, retry
			// the flush before reading more proposals.
			if retryBatch {
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

// syncBatchFallback processes a failed batch one proposal at a time
// using the per-proposal Sync method. Permanent errors skip the
// offending proposal; transient errors stop the fallback and leave the
// remaining proposals in the batch for the caller to retry.
func (db *DB) syncBatchFallback(ctx context.Context, id string, s cluster.Syncable, entries []batchEntry) bool {
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
				continue
			}
			// Transient error in fallback — stop here. The caller
			// should not retry this batch (the successful prefix
			// was already committed via Sync's idempotent upsert).
			db.logger.Warn("transient sync error in fallback, stopping",
				zap.String("id", id), zap.Uint64("index", e.index), zap.Error(syncErr))
			return false
		}
		if shouldSnapshot {
			err := db.proposeSyncableIndex(ctx, &cluster.SyncableIndex{ID: id, Index: e.index})
			if err != nil {
				db.logger.Warn("proposeSyncableIndex error", zap.String("id", id), zap.Error(err))
			}
		}
	}
	return true
}
