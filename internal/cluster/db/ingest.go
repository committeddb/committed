package db

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/philborlin/committed/internal/cluster"
)

// ingestBackoff{Min,Max} bound the interval at which db.ingest's
// state-machine wakes to check for leader transitions when no
// proposal or position is in flight. The worker reacts to its
// channels immediately (the select still has the channel cases), so
// active workloads pay no latency; the backoff only governs how
// often an idle worker polls db.isNode for a leadership change.
//
// Without this, the loop's `default` branch ran on every iteration
// and burned ~one CPU core per worker between proposals — atomic
// load + branch + loop, ~10M iter/sec. Same trade-off as
// syncBackoff: idle workers cap at Max polling latency for leader
// transitions, active workers stay at Min.
const (
	ingestBackoffMin = 1 * time.Millisecond
	ingestBackoffMax = 500 * time.Millisecond
)

// ingressLifecycle owns the inner Ingest goroutine plus the channels
// the user-supplied Ingestable writes to. db.ingest creates one and
// re-uses it across leader transitions: start() spawns a fresh
// inner Ingest with a child context derived from the worker ctx;
// stop() cancels that context, waits for the inner goroutine to
// exit, and clears the cancel func so the lifecycle can be re-used.
//
// Why this struct exists at all: the cancel func has to outlive a
// single iteration of db.ingest's for-loop (it's reused across
// repeated leader gain/loss transitions), so it can't just be a
// `defer cancel()` after a `WithCancel`. As a function-local
// variable in db.ingest, gopls's `lostcancel` analyzer flagged it
// as "may not be called on all paths" because the analyzer doesn't
// trace cancel funcs through deferred closures or repeated
// reassignment. Moving it onto a struct field puts it outside the
// analyzer's tracking scope (lostcancel ignores escaped cancel
// funcs by design — escaped means "stored in a struct, returned,
// sent on a channel, etc.: someone else's responsibility"). The
// behavior is unchanged; the warnings go away.
type ingressLifecycle struct {
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	proposalChan chan *cluster.Proposal
	positionChan chan cluster.Position
}

func newIngressLifecycle() *ingressLifecycle {
	return &ingressLifecycle{
		proposalChan: make(chan *cluster.Proposal),
		positionChan: make(chan cluster.Position),
	}
}

// start spawns the inner Ingest with a context derived from parent.
// The caller must have called stop() (or never started) before
// calling start() again — start() unconditionally overwrites the
// stored cancel func.
func (l *ingressLifecycle) start(parent context.Context, i cluster.Ingestable, pos cluster.Position) {
	ictx, cancel := context.WithCancel(parent)
	l.cancel = cancel
	l.wg.Go(func() {
		err := i.Ingest(ictx, pos, l.proposalChan, l.positionChan)
		if err != nil {
			zap.L().Warn("ingest error", zap.Error(err))
		}
	})
}

// stop cancels the inner Ingest's context and waits for the
// goroutine to exit. Safe to call when no inner Ingest is running
// (it's a no-op when cancel is nil). Idempotent: a second call
// after the first sees cancel == nil and just runs Wait, which
// returns immediately because the WaitGroup is already drained.
func (l *ingressLifecycle) stop() {
	if l.cancel != nil {
		l.cancel()
		l.cancel = nil
	}
	l.wg.Wait()
}

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
	replaced := false
	for {
		existing, ok := db.ingestWorkers[id]
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
		if db.ingestWorkers[id] == existing {
			delete(db.ingestWorkers, id)
		}
	}

	if replaced && db.metrics != nil {
		db.metrics.WorkerReplaced("ingest", id)
	}

	// cancel ownership passes to the workerHandle. It is invoked by
	// db.Close (see db.go:~390) and by the replace path above when a
	// duplicate Ingest call supersedes this worker, so the cancel is
	// not leaked. gosec can't see through the handle indirection.
	workerCtx, cancel := context.WithCancel(db.ctx) //nolint:gosec // G118: cancel owned by workerHandle
	handle := &workerHandle{cancel: cancel, done: make(chan struct{})}
	db.ingestWorkers[id] = handle
	db.workersMu.Unlock()

	if db.metrics != nil {
		db.metrics.SetWorkerRunning("ingest", id, true)
	}

	go func() {
		defer func() {
			if db.metrics != nil {
				db.metrics.SetWorkerRunning("ingest", id, false)
			}
			close(handle.done)
		}()
		_ = db.ingest(workerCtx, id, i)
	}()

	return nil
}

func (db *DB) ingest(ctx context.Context, id string, i cluster.Ingestable) error {
	isNode := false

	// The ingressLifecycle owns the inner Ingest goroutine and the
	// channels it writes to. Holding it as a struct (not as loose
	// function-local variables) keeps the cancel func outside the
	// gopls/vet `lostcancel` analyzer's scope — see the type's doc
	// comment for the full reasoning.
	ingress := newIngressLifecycle()

	// On exit, cancel the inner Ingest goroutine (if any) and wait
	// for it to actually return. The worker handle's done channel
	// only closes after this function returns, so by the time the
	// registry observes done the user-supplied Ingest is fully torn
	// down. Without this, replace and Close would race against a
	// still-running inner Ingest still holding the channel endpoints.
	defer ingress.stop()

	backoff := ingestBackoffMin

	for {
		select {
		case <-ctx.Done():
			return nil
		case proposal := <-ingress.proposalChan:
			if db.isNode(id) {
				// Ingest worker proposes user data; we use the worker
				// context (ctx) so cancel-on-stop interrupts the wait.
				err := db.Propose(ctx, proposal)
				if err != nil {
					db.logger.Warn("ingest propose error", zap.String("id", id), zap.Error(err))
				}
			}
			backoff = ingestBackoffMin
		case position := <-ingress.positionChan:
			if db.isNode(id) {
				err := db.proposeIngestablePosition(ctx, &cluster.IngestablePosition{ID: id, Position: position})
				if err != nil {
					db.logger.Warn("proposeIngestablePosition error", zap.String("id", id), zap.Error(err))
				}
			}
			backoff = ingestBackoffMin
		case <-time.After(backoff):
			// Periodic wakeup to check for leader transitions. Replaces
			// the previous default-branch spin which polled db.isNode
			// at ~10M iter/sec between proposals. The state-machine
			// logic is unchanged; only the cadence is.
			progressed := false
			if isNode && !db.isNode(id) {
				db.logger.Info("stopping ingestion", zap.String("id", id))
				// leader -> not-leader - stop ingesting
				isNode = false
				ingress.stop()
				progressed = true
			} else if !isNode && db.isNode(id) {
				db.logger.Info("starting ingestion", zap.String("id", id))
				// not-leader -> leader - start ingesting
				isNode = true
				// Parent the inner Ingest's context on the worker ctx
				// (passed into start as `parent`) so PR3's replace /
				// Close cancellation propagates directly through to
				// the user-supplied Ingest. The defer above is the
				// safety net for the leader-state transition (a
				// leader → not-leader stop() inside this branch
				// doesn't exit the worker loop).
				ingress.start(ctx, i, db.storage.Position(id))
				progressed = true
			}
			if progressed {
				backoff = ingestBackoffMin
			} else {
				backoff *= 2
				if backoff > ingestBackoffMax {
					backoff = ingestBackoffMax
				}
			}
		}
	}
}
