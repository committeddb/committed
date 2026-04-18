package db

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/philborlin/committed/internal/cluster"
)

// ingestExitReason classifies why db.ingest returned. The worker-launch
// goroutine inspects it to decide whether to spawn the supervisor:
// ingestExitFreeze means "parked in the ErrProposalUnknown branch,
// operator-style restart needed"; ingestExitShutdown means the worker
// exited because its ctx was canceled for another reason (db.Close or
// registry replace) and the normal teardown already handles recovery.
type ingestExitReason int

const (
	ingestExitShutdown ingestExitReason = iota
	ingestExitFreeze
)

// inflightBumps tracks the set of position-bump waiters this worker
// has submitted but not yet observed resolve. Key is the RequestID,
// value is the ack channel returned by proposeIngestablePositionAsync.
//
// Why this is worker-local and not on db.DB: only the worker that
// submitted a bump cares about its outcome, and only on its own
// freeze path. Keeping the map in the goroutine-local scope
// eliminates locking (single-goroutine access) and scopes the drain
// cleanly to "this worker's in-flight bumps".
//
// Why track at all: before the unification, position bumps went
// through a fire-and-forget primitive with no waiter — a bump
// truncated during a leader flap silently left storage.Position
// stale, and the supervisor's post-freeze restart would then
// replay rows already committed under the old leader, producing
// duplicate raft log entries. Draining before freeze forces the
// supervisor to read Position after every bump has either applied
// (advancing it correctly) or been ErrProposalUnknown-signaled
// (leaving it at the last known-good value, so replay is
// idempotent).
type inflightBumps struct {
	acks map[uint64]<-chan error
}

func newInflightBumps() *inflightBumps {
	return &inflightBumps{acks: make(map[uint64]<-chan error)}
}

func (b *inflightBumps) add(rid uint64, ack <-chan error) {
	b.acks[rid] = ack
}

// reap drops already-resolved bumps without waiting, so the map
// stays bounded during steady-state ingestion. Called from the
// worker's idle branch (the time.After wake) where we have time to
// do housekeeping. Correctness of the freeze path doesn't depend
// on reap ever running — drain cleans up everything at freeze time
// regardless — but reaping is a cheap way to avoid O(ever-seen)
// growth on long-lived workers.
func (b *inflightBumps) reap(db *DB) {
	for rid, ack := range b.acks {
		select {
		case <-ack:
			db.unregisterWaiter(rid)
			delete(b.acks, rid)
		default:
		}
	}
}

// drain waits for every remaining in-flight bump to resolve, bounded
// by timeout. After return, every waiter previously tracked here is
// unregistered (either its ack was consumed or we gave up and
// cleaned it up as a leak-prevention step). Emits the drain-timeout
// metric if the deadline fired with unresolved acks.
//
// Callers MUST be the sole consumer of these ack channels (workers
// don't share bumps across goroutines), so the <-ack read here is
// guaranteed to be the single receiver — no race with another
// drainer.
func (b *inflightBumps) drain(ctx context.Context, db *DB, timeout time.Duration, logger *zap.Logger, id string) {
	if len(b.acks) == 0 {
		return
	}

	start := time.Now()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	// Snapshot rids so map mutation during iteration is safe and
	// iteration order is stable for the drained-count accounting.
	rids := make([]uint64, 0, len(b.acks))
	for rid := range b.acks {
		rids = append(rids, rid)
	}

	timedOut := false
	drained := 0
DRAIN:
	for _, rid := range rids {
		ack := b.acks[rid]
		select {
		case <-ack:
			drained++
		case <-deadline.C:
			timedOut = true
			break DRAIN
		case <-ctx.Done():
			break DRAIN
		}
	}

	undrained := len(b.acks) - drained
	for rid := range b.acks {
		db.unregisterWaiter(rid)
	}
	b.acks = make(map[uint64]<-chan error)

	if timedOut && undrained > 0 {
		logger.Warn("ingest freeze drain timeout; supervisor may read stale position",
			zap.String("id", id),
			zap.Int("undrained", undrained),
			zap.Duration("elapsed", time.Since(start)))
		if db.metrics != nil {
			db.metrics.IngestFreezeDrainTimeout(id)
		}
	}
}

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

	db.spawnIngestWorkerLocked(id, i)
	db.workersMu.Unlock()

	if db.metrics != nil {
		db.metrics.SetWorkerRunning("ingest", id, true)
	}

	return nil
}

// spawnIngestWorkerLocked installs a fresh ingest worker handle for id
// in the registry and starts the worker goroutine. Caller MUST hold
// db.workersMu and MUST have already ensured the registry slot for id
// is empty and db.closed is false. Shared between db.Ingest (which
// drains any prior entry via the replace loop first) and
// superviseRestartIngest (which drops the frozen handle directly
// under the same lock, then calls this to install the replacement).
//
// Centralizing the handle/goroutine/supervisor-wiring here is load-
// bearing for the supervisor-vs-user-replace race: holding the lock
// across the frozen-drop + fresh-install keeps a concurrent user
// replace from slipping a handle in between. Both the supervisor's
// preflight check and its install run without releasing the lock, so
// the registry transitions from {frozen} → {supervisor-installed}
// atomically. A user replace that arrives after the unlock still wins
// the final state (its db.Ingest replaces the supervisor-installed
// handle via the normal replacement loop).
func (db *DB) spawnIngestWorkerLocked(id string, i cluster.Ingestable) *workerHandle {
	// cancel ownership passes to the workerHandle. It is invoked by
	// db.Close and by the replace loop in db.Ingest when a duplicate
	// supersedes this worker, so the cancel is not leaked. gosec can't
	// see through the handle indirection.
	workerCtx, cancel := context.WithCancel(db.ctx) //nolint:gosec // G118: cancel owned by workerHandle
	handle := &workerHandle{cancel: cancel, done: make(chan struct{})}
	db.ingestWorkers[id] = handle

	go func() {
		reason := db.ingest(workerCtx, id, i)
		if db.metrics != nil {
			db.metrics.SetWorkerRunning("ingest", id, false)
		}
		// Close done BEFORE spawning the supervisor. Replace-path
		// callers and the supervisor's preflight both observe
		// handle.done to gate subsequent work; closing here keeps
		// them unblocked uniformly.
		close(handle.done)
		if reason == ingestExitFreeze {
			go db.superviseRestartIngest(id, i, handle)
		}
	}()

	return handle
}

func (db *DB) ingest(ctx context.Context, id string, i cluster.Ingestable) ingestExitReason {
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

	// Track in-flight position-bump acks. Drained synchronously on
	// the freeze exit path so the supervisor's subsequent
	// storage.Position read reflects every bump's true outcome; on
	// any OTHER exit (shutdown / replace) the deferred cleanup
	// below unregisters leftover waiters so the waiters map doesn't
	// leak. See inflightBumps doc for the correctness argument.
	bumps := newInflightBumps()
	defer func() {
		for rid := range bumps.acks {
			db.unregisterWaiter(rid)
		}
	}()

	backoff := ingestBackoffMin

	for {
		select {
		case <-ctx.Done():
			return ingestExitShutdown
		case proposal := <-ingress.proposalChan:
			if db.isNode(id) {
				// Ingest worker proposes user data; we use the worker
				// context (ctx) so cancel-on-stop interrupts the wait.
				err := db.Propose(ctx, proposal)
				if err != nil {
					db.logger.Warn("ingest propose error", zap.String("id", id), zap.Error(err))
					if errors.Is(err, ErrProposalUnknown) {
						// Freeze: we don't know if this proposal
						// committed. Before exiting, drain our
						// in-flight position bumps so the
						// supervisor's post-restart storage.Position
						// read reflects every bump's outcome (applied
						// or known-lost). Without this, a bump that
						// was truncated during the same flap would
						// leave Position stale, and the restart would
						// replay rows already committed under the old
						// leader — duplicate raft log entries.
						//
						// ingress.stop (from the defer above) cancels
						// the inner Ingest goroutine cleanly on
						// return, unblocking its pending
						// positionChan send. The position value is
						// discarded (we must not advance past the
						// unknown proposal), which is the whole
						// point of freezing here.
						if db.metrics != nil {
							db.metrics.IngestFrozen(id, true)
						}
						bumps.drain(ctx, db, db.ingestFreezeDrainTimeout, db.logger, id)
						return ingestExitFreeze
					}
				}
			}
			backoff = ingestBackoffMin
		case position := <-ingress.positionChan:
			if db.isNode(id) {
				rid, ack, err := db.proposeIngestablePositionAsync(ctx, &cluster.IngestablePosition{ID: id, Position: position})
				if err != nil {
					db.logger.Warn("proposeIngestablePositionAsync error", zap.String("id", id), zap.Error(err))
				} else {
					bumps.add(rid, ack)
				}
			}
			backoff = ingestBackoffMin
		case <-time.After(backoff):
			// Periodic wakeup. In addition to the leader-transition
			// polling below, this is where we reap already-resolved
			// bumps so the in-flight map stays bounded during
			// steady-state ingestion.
			bumps.reap(db)

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
