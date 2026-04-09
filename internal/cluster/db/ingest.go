package db

import (
	"context"
	"fmt"
	"sync"

	"github.com/philborlin/committed/internal/cluster"
)

// Ingest registers an Ingestable to run as a worker for the given ID.
// If a worker is already running for that ID, the existing one is
// canceled and Ingest waits for it to fully exit before starting the
// replacement. This makes Ingest idempotent on duplicate calls and
// makes config-replace semantics deterministic: a fresh propose for
// the same ID always wins, and the old worker is gone before the new
// one touches the same Reader / Position / proposeC slot.
//
// The worker context is derived from db.ctx, NOT from the ctx passed
// in by the caller. The caller's ctx is typically a per-request HTTP
// context that completes long before the worker should — wiring it
// in would tear the worker down as soon as the propose handler
// returned. db.ctx is the database lifecycle context, so workers run
// until either Replace (which cancels via the registry) or Close
// (which cancels via cancelSyncs / the per-handle cancel).
func (db *DB) Ingest(_ context.Context, id string, i cluster.Ingestable) error {
	db.workersMu.Lock()
	if db.closed {
		db.workersMu.Unlock()
		return ErrClosed
	}
	// Loop until the slot is empty before installing. The naive
	// "if existing { cancel; wait; install }" shape races when two
	// callers replace the same id concurrently: both observe the
	// original entry, both wait on it, then both install — and the
	// loser's worker is orphaned (running but unreferenced from the
	// map, so no future replace can find it).
	//
	// The loop fixes this by re-checking the slot after each wait.
	// If the slot still points to the entry we just waited on, we
	// clear it and break out. If a concurrent caller slipped a new
	// entry in while we waited, we cancel+wait that one too. This
	// converges in normal use (one extra cycle per pre-empting caller)
	// and the lock is dropped only during the wait, so the exiting
	// worker is never blocked by us.
	//
	// Re-check db.closed after each wait too: a concurrent db.Close
	// may have flipped the flag while we were dropped. Without the
	// re-check, we'd install a worker that escapes the Close drain.
	for {
		existing, ok := db.ingestWorkers[id]
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
		if db.ingestWorkers[id] == existing {
			delete(db.ingestWorkers, id)
		}
	}

	workerCtx, cancel := context.WithCancel(db.ctx)
	handle := &workerHandle{cancel: cancel, done: make(chan struct{})}
	db.ingestWorkers[id] = handle
	db.workersMu.Unlock()

	go func() {
		defer close(handle.done)
		_ = db.ingest(workerCtx, id, i)
	}()

	return nil
}

func (db *DB) ingest(ctx context.Context, id string, i cluster.Ingestable) error {
	isNode := false

	proposalChan := make(chan *cluster.Proposal)
	positionChan := make(chan cluster.Position)
	var (
		ingressCancel context.CancelFunc
		ingressWG     sync.WaitGroup
	)

	// On exit, cancel the inner Ingest goroutine (if any) and wait for
	// it to actually return. The worker handle's done channel only
	// closes after this function returns, so by the time the registry
	// observes done the user-supplied Ingest is fully torn down. Without
	// this, replace and Close would race against a still-running inner
	// Ingest still holding the proposalChan/positionChan endpoints.
	defer func() {
		if ingressCancel != nil {
			ingressCancel()
		}
		ingressWG.Wait()
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case proposal := <-proposalChan:
			if db.isNode(id) {
				// Ingest worker proposes user data; we use the worker
				// context (ctx) so cancel-on-stop interrupts the wait.
				err := db.Propose(ctx, proposal)
				if err != nil {
					// TODO Handle error
					fmt.Printf("[db.DB] propose: %v\n", err)
				}
			}
		case position := <-positionChan:
			if db.isNode(id) {
				err := db.proposeIngestablePosition(ctx, &cluster.IngestablePosition{ID: id, Position: position})
				if err != nil {
					// TODO Handle error
					fmt.Printf("[db.DB] proposeIngestablePosition: %v\n", err)
				}
			}
		default:
			if isNode && !db.isNode(id) {
				fmt.Println("Stopping ingestion...")
				// leader -> not-leader - stop ingesting
				isNode = false
				ingressCancel()
				ingressWG.Wait()
				ingressCancel = nil
			} else if !isNode && db.isNode(id) {
				fmt.Println("Starting to ingest...")
				// not-leader -> leader - start ingesting
				isNode = true

				// Parent the inner Ingest's context on the worker ctx
				// so PR3's replace / Close cancellation propagates
				// directly through to the user-supplied Ingest, instead
				// of going via the deferred cancel above. The deferred
				// cancel is still the safety net for the leader-state
				// transition (leader → not-leader cancels the inner
				// goroutine without exiting the worker loop).
				var ingressCtx context.Context
				ingressCtx, ingressCancel = context.WithCancel(ctx)

				p := db.storage.Position(id)
				ingressWG.Go(func() {
					err := i.Ingest(ingressCtx, p, proposalChan, positionChan)
					if err != nil {
						// TODO Handle error
						fmt.Printf("[db.DB] ingest: %v\n", err)
					}
				})
			}
		}
	}
}
