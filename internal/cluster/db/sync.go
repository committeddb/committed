package db

import (
	"context"
	"fmt"
	"io"
	"time"

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

// Sync registers a Syncable to run as a worker for the given ID. See
// db.Ingest for the registry semantics — Sync is the syncable-side
// counterpart and behaves identically: a duplicate call for the same
// ID cancels and replaces the existing worker, the worker context is
// derived from db.ctx (not the caller's ctx), and db.Close drains
// every registered worker before tearing the raft layer down.
func (db *DB) Sync(_ context.Context, id string, s cluster.Syncable) error {
	db.workersMu.Lock()
	if db.closed {
		db.workersMu.Unlock()
		return ErrClosed
	}
	// See db.Ingest for the rationale behind the loop and the
	// re-check of db.closed after each wait.
	for {
		existing, ok := db.syncWorkers[id]
		if !ok {
			break
		}
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

	workerCtx, cancel := context.WithCancel(db.ctx)
	handle := &workerHandle{cancel: cancel, done: make(chan struct{})}
	db.syncWorkers[id] = handle
	db.workersMu.Unlock()

	go func() {
		defer close(handle.done)
		_ = db.sync(workerCtx, id, s)
	}()

	return nil
}

func (db *DB) sync(ctx context.Context, id string, s cluster.Syncable) error {
	isNode := false
	var r ProposalReader
	backoff := syncBackoffMin

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
			fmt.Println("Stopping Sync...")
			r = nil
			isNode = false
			progressed = true
		case !isNode && db.isNode(id):
			fmt.Printf("[db] Syncing %v\n", id)
			r = db.storage.Reader(id)
			isNode = true
			progressed = true
		case isNode && db.isNode(id):
			i, p, err := r.Read()
			switch {
			case err == io.EOF:
				// caught up; idle, will sleep below
			case err != nil:
				// TODO Handle error properly. For now log and idle —
				// treating a Read error as "no progress" lets the
				// backoff slow the retry loop instead of hammering a
				// broken reader.
				fmt.Printf("[db.DB] read: %v\n", err)
			default:
				// Pass the worker's ctx (not db.ctx) so a replace or
				// Close-driven cancellation propagates into the user's
				// Sync implementation. Without this, a slow Sync keeps
				// the worker alive past the registry replace, leaving
				// the new worker waiting on the old one's done channel.
				// Sync operations are expected to be idempotent (the
				// SQL dialect uses upsert), so the replacement worker
				// re-syncing the same proposal after a cancel is safe.
				shouldSnapshot, err := s.Sync(ctx, p)
				if err != nil {
					// TODO Handle error
					fmt.Printf("[db.DB] sync: %v\n", err)
				}
				if shouldSnapshot {
					err = db.proposeSyncableIndex(ctx, &cluster.SyncableIndex{ID: id, Index: i})
					if err != nil {
						// TODO Handle error
						fmt.Printf("[db.DB] proposeSyncableIndex: %v\n", err)
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
