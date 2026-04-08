package db

import (
	"context"
	"fmt"
	"io"

	"github.com/philborlin/committed/internal/cluster"
)

// Sync registers a Syncable to run as a worker for the given ID. See
// db.Ingest for the registry semantics — Sync is the syncable-side
// counterpart and behaves identically: a duplicate call for the same
// ID cancels and replaces the existing worker, the worker context is
// derived from db.ctx (not the caller's ctx), and db.Close drains
// every registered worker before tearing the raft layer down.
func (db *DB) Sync(_ context.Context, id string, s cluster.Syncable) error {
	db.workersMu.Lock()
	// See db.Ingest for the rationale behind the loop. tl;dr: a naive
	// check-cancel-wait-install races against concurrent replaces of
	// the same id and orphans workers. The loop re-checks the slot
	// after each wait, walking through any new entries that slipped in.
	for {
		existing, ok := db.syncWorkers[id]
		if !ok {
			break
		}
		existing.cancel()
		db.workersMu.Unlock()
		<-existing.done
		db.workersMu.Lock()
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

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if isNode && !db.isNode(id) {
				fmt.Println("Stopping Sync...")
				// leader -> not-leader - stop syncing
				r = nil
				isNode = false
			} else if !isNode && db.isNode(id) {
				fmt.Printf("[db] Syncing %v\n", id)
				// not-leader -> leader - start syncing
				r = db.storage.Reader(id)
				isNode = true
			} else if isNode && db.isNode(id) {
				// leader -> leader - keep syncing
				i, p, err := r.Read()
				if err == io.EOF {
					// TODO Figure out what to do - maybe do an exponential backoff to a certain point - maybe nothing?
					continue
				} else if err != nil {
					// TODO Handle error
					fmt.Printf("[db.DB] read: %v\n", err)
				}

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
			}
		}
	}
}
