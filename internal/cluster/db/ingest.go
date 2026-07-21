package db

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
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
		// Backstop: a panic in a dialect's Ingest (e.g. a decode panic on a
		// malformed CDC frame that escapes the dialect's own recover) must not
		// unwind this goroutine and crash the whole node. Recover it into a loud
		// log; this ingestable stops (the outer loop idles on its timer) but the
		// node — and every other syncable/ingestable — stays up, pending an
		// operator re-POST. The Postgres dialect additionally recovers in-stream
		// so a transient bad frame self-heals via reconnect (see stream).
		defer func() {
			if r := recover(); r != nil {
				zap.L().Error("ingest worker panicked; ingestable stopped, node stays up — investigate and re-POST it",
					zap.Any("panic", r), zap.Stack("stack"))
			}
		}()
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
		if !waitDone(existing.done, db.workerDrainTimeout) {
			db.logger.Warn("ingest replace: prior worker did not exit in time; abandoning it (wedged on its source?) and proceeding",
				zap.String("id", id), zap.Duration("timeout", db.workerDrainTimeout))
		}
		// Release the superseded ingestable's source resources (only if it drained,
		// so we don't race a wedged worker) — otherwise every re-POST leaks them.
		db.closeDrainedIngestable(existing, id)
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

// deleteIngest cancels the ingest worker for id and, on the owner node only,
// tears down the source-side replication resources (drops the Postgres slot +
// publication). It is the ingest analogue of deleteSync, run on apply of an
// ingestable config delete.
//
// Two planes, as the delete design requires:
//   - Worker cancel is node-local and idempotent: every node that built the
//     config has a worker, so each stops its own goroutine; a node with none is
//     a no-op. The worker clears its own running metric on exit.
//   - The source teardown is the destructive side effect (drop the slot), so it
//     is gated on db.isNode(id) and run live only — never reconstructed from
//     replay. By the time this runs the config is already deleted, so isNode
//     resolves to "this node is the leader"; the leader tears down using its own
//     already-built ingestable handle.
//
// Best-effort: the logical deletion already succeeded via consensus, so a
// teardown failure only leaves an orphaned slot an operator can drop — it must
// never fail or panic.
func (db *DB) deleteIngest(id string) {
	db.workersMu.Lock()
	handle, ok := db.ingestWorkers[id]
	if ok {
		handle.cancel()
		db.workersMu.Unlock()
		if !waitDone(handle.done, db.workerDrainTimeout) {
			db.logger.Warn("delete ingest: worker did not exit in time; abandoning it (wedged on its source?) and proceeding",
				zap.String("id", id), zap.Duration("timeout", db.workerDrainTimeout))
		}
		db.workersMu.Lock()
		if db.ingestWorkers[id] == handle {
			delete(db.ingestWorkers, id)
		}
	}
	db.workersMu.Unlock()

	if !ok || handle.ingestable == nil {
		return // no worker built on this node — nothing to tear down
	}

	// Release the deleted ingestable's source resources (Close). Node-local
	// cleanup — done on every node that built a worker (if it drained), before and
	// independent of the owner-only slot teardown below.
	db.closeDrainedIngestable(handle, id)

	if !db.isNode(id) {
		return // this node isn't the owner
	}

	teardownable, ok := handle.ingestable.(cluster.IngestableTeardownable)
	if !ok {
		return // ingestable owns no source-side replication resource
	}
	// Bounded (runBounded): this runs on the single-threaded config listener.
	// The SQL implementation ctx-bounds itself (TeardownSource), but the guard
	// must hold for any implementation.
	if err, completed := runBounded(db.workerDrainTimeout, teardownable.Teardown); !completed {
		db.logger.Error("ingestable deleted but source teardown did not return in time (unreachable source?); abandoning it (an orphaned replication slot may pin the source's WAL; drop it manually)",
			zap.String("id", id), zap.Duration("timeout", db.workerDrainTimeout))
	} else if err != nil {
		// Best-effort: the logical delete already committed. Log loudly and move
		// on — the worst case is an orphaned slot pinning the source's WAL.
		db.logger.Error("ingestable deleted but source teardown failed (an orphaned replication slot may pin the source's WAL; drop it manually)",
			zap.String("id", id), zap.Error(err))
	}
}

// IngestableStatus reports an ingestable worker's operational status — snapshot
// vs. streaming phase, per-table snapshot progress, the CDC position, source
// lag, and caught-up. It reads the worker's persisted checkpoint position
// (replicated apply state, consistent on any node behind the HTTP linearize
// barrier) and asks the parsed Ingestable to decode it and, where the dialect
// supports it, query the source for lag.
//
// Every node builds and registers the worker when it applies the config
// (isNode only gates which node actually streams), so the local handle is
// present on any node that has the config; a missing handle means no ingestable
// of that id is configured here, surfaced as ErrIngestableNotRunning (404).
// The Ingestable's Status is called without holding workersMu — it makes a
// source query and must not block the worker registry.
func (db *DB) IngestableStatus(ctx context.Context, id string) (cluster.IngestableStatus, error) {
	db.workersMu.Lock()
	handle, ok := db.ingestWorkers[id]
	var ing cluster.Ingestable
	if ok {
		ing = handle.ingestable
	}
	db.workersMu.Unlock()

	if ing == nil {
		return cluster.IngestableStatus{}, cluster.ErrIngestableNotRunning
	}

	return ing.Status(ctx, db.storage.Position(id))
}

// TopicRefreshEpoch exposes the storage's delete-surviving per-topic
// refresh-epoch highwater to the SQL ingest worker (wired as the
// IngestableParser's EpochFloor), so a same-topic recreate resumes its
// generation above the rows still on the sink rather than resetting to epoch 1.
// See wal.Storage.TopicRefreshEpoch. Satisfies ingestablesql.TopicEpochReader.
func (db *DB) TopicRefreshEpoch(topic string) uint64 {
	return db.storage.TopicRefreshEpoch(topic)
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
	handle := &workerHandle{cancel: cancel, done: make(chan struct{}), ingestable: i}
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

	backoff := ingestBackoffMin

	// dataProposeFailed is the POSITION BARRIER: armed the moment any data
	// proposal fails, before the error is even classified. A standalone
	// position checkpoint summarizes "all data through here committed", so
	// once a data proposal has failed, proposing any later position would
	// checkpoint past a hole. Every failure branch below freezes the worker
	// anyway — the barrier is the structural enforcement that survives a
	// future mishandled error branch (the oversized-proposal bug: TooLarge
	// was warn-and-continue, and the next checkpoint silently committed the
	// resume position past the dropped batch).
	dataProposeFailed := false

	for {
		select {
		case <-ctx.Done():
			return ingestExitShutdown
		case proposal := <-ingress.proposalChan:
			if db.isNode(id) {
				// Stamp the ingestable id so the apply path advances this
				// ingestable's source-seq highwater when the proposal
				// applies.
				proposal.IngestableID = id

				// Effectively-once dedup. After a crash/flap the dialect
				// resumes from a checkpoint that may trail the last
				// committed proposal and re-emits proposals already in the
				// log. We drop those here — BEFORE raft — by comparing the
				// dialect's monotonic SourceSeq against the durable
				// highwater. Dropping pre-raft keeps the duplicate out of
				// the committed event log entirely, so syncables never see
				// it. SourceSeq==0 (snapshot rows / non-CDC / legacy) is
				// never deduped.
				if proposal.SourceSeq > 0 && proposal.SourceSeq <= db.storage.IngestSourceSeqHighwater(id) {
					db.logger.Debug("ingest dedup skip",
						zap.String("id", id), zap.Uint64("sourceSeq", proposal.SourceSeq))
					if db.metrics != nil {
						db.metrics.IngestDedupSkipped(id)
					}
					backoff = ingestBackoffMin
					continue
				}

				// Hold a refresh-boundary marker until every member can apply
				// it (semantic version-skew gate): a pre-mechanism sink has no
				// IsRefreshBoundary branch and would dead-letter it permanently.
				// The worker blocks on its pr send until we accept the marker,
				// so this backpressure holds its position checkpoint too — the
				// marker commits before the position advances. A false return
				// (shutdown / leadership loss) drops the un-advanced marker; a
				// restart re-runs the refresh and re-emits it. See
				// awaitRefreshBoundaryEnabled.
				if containsRefreshBoundary(proposal) && !db.awaitRefreshBoundaryEnabled(ctx, id) {
					backoff = ingestBackoffMin
					continue
				}

				// Ingest worker proposes user data; we use the worker
				// context (ctx) so cancel-on-stop interrupts the wait.
				// proposeIngestData retries (not drops) on disk-pressure
				// rejection so a full disk pauses ingestion cleanly without
				// advancing the position past uncommitted data.
				err := db.proposeIngestData(ctx, proposal)
				if err != nil {
					// Arm the position barrier BEFORE classifying the error:
					// no later standalone checkpoint may pass this hole,
					// whatever the branches below decide.
					dataProposeFailed = true
					db.logger.Warn("ingest propose error", zap.String("id", id), zap.Error(err))
					// Count real failures only — a ctx cancellation here is
					// the worker shutting down (replace/Close), not an
					// ingest error.
					if db.metrics != nil && ctx.Err() == nil {
						db.metrics.IngestError(id, "propose")
					}
					if ctx.Err() == nil {
						// Freeze on EVERY propose failure — the conservative
						// default. ErrProposalUnknown (status unknown after a
						// leader change) and ErrProposalLost (entry truncated
						// before commit) are the classic leader-flap orphan
						// signals: we don't know (or know it didn't) commit,
						// and continuing would let the next checkpoint advance
						// the position past data that never committed. The
						// same reasoning holds for ANY other failure — a
						// deterministic rejection like ErrProposalTooLarge or
						// an error this code has never seen. The worker never
						// rewrites a proposal to make it fit: a proposal is an
						// opaque atomic unit whose composition (grouping AND
						// sizing) belongs to its emitter — the dialects
						// byte-budget their snapshot batches under the cap
						// (sql.ChunkEntitiesByBytes), and an emitter that
						// exceeds it anyway freezes here loudly. The
						// pre-barrier bug was exactly a branch that chose to
						// continue; the default must hold the line, and the
						// supervisor's restart-from-durable-position is the
						// one recovery that is always correct. Because
						// position bumps are blocking (one in flight at a
						// time, fully resolved before the next channel
						// event), there are no outstanding bumps to drain —
						// storage.Position is already definitive, so the
						// supervisor's post-restart read is correct without
						// any drain step.
						//
						// ingress.stop (from the defer above) cancels the
						// inner Ingest goroutine cleanly on return,
						// unblocking its pending positionChan send. The
						// position value is discarded (we must not advance
						// past the failed proposal), which is the whole
						// point of freezing here.
						if db.metrics != nil {
							db.metrics.IngestFrozen(id, true)
						}
						return ingestExitFreeze
					}
					// ctx canceled mid-propose: worker shutdown, not an
					// ingest failure — the next select observes ctx.Done.
				}
			}
			backoff = ingestBackoffMin
		case position := <-ingress.positionChan:
			if dataProposeFailed {
				// POSITION BARRIER (see declaration above): a data proposal
				// failed and some branch continued instead of freezing — a
				// path that should not exist. Never checkpoint past the
				// hole; freeze now.
				db.logger.Error("ingest position barrier: refusing checkpoint after a failed data proposal", zap.String("id", id))
				if db.metrics != nil {
					db.metrics.IngestFrozen(id, true)
				}
				return ingestExitFreeze
			}
			if db.isNode(id) {
				// Block until the position is durably applied. The
				// unbuffered positionChan means the inner Ingest only
				// sends a checkpoint AFTER the proposals it covers have
				// been handed off (and those Proposes already blocked
				// to apply), so a durable position implies durable
				// proposals through it. On ErrProposalUnknown OR
				// ErrProposalLost we freeze exactly like an orphaned user
				// proposal: the supervisor restarts the ingestable from the
				// un-advanced (persisted) position. Both leader-flap orphan
				// signals must freeze — Lost is the one that wins on the old
				// leader where this worker runs.
				err := db.proposeIngestablePosition(ctx, &cluster.IngestablePosition{ID: id, Position: position})
				if err != nil {
					db.logger.Warn("proposeIngestablePosition error", zap.String("id", id), zap.Error(err))
					if db.metrics != nil && ctx.Err() == nil {
						db.metrics.IngestError(id, "position")
					}
					if errors.Is(err, ErrProposalUnknown) || errors.Is(err, ErrProposalLost) {
						if db.metrics != nil {
							db.metrics.IngestFrozen(id, true)
						}
						return ingestExitFreeze
					}
				}
			}
			backoff = ingestBackoffMin
		case <-time.After(backoff):
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

// proposeIngestData proposes one ingest data entry, retrying with backoff while
// the node rejects it for disk pressure (cluster.ErrInsufficientStorage). It
// deliberately does NOT drop the proposal on pressure: holding it keeps the
// inner Ingest backpressured on the unbuffered proposalChan (so upstream
// reading pauses) and — crucially — guarantees the ingestable's position never
// advances past an uncommitted proposal. The position checkpoint is a separate
// channel event processed only after this returns; if we dropped the data and
// returned, the following checkpoint would commit and silently skip the dropped
// data. Retrying instead pauses ingestion cleanly and resumes the moment disk
// recovers.
//
// Returns nil on durable apply, ctx.Err() when the worker ctx is canceled
// (replace/Close), or any non-pressure Propose error (e.g. ErrProposalUnknown)
// unchanged for the caller's existing handling.
func (db *DB) proposeIngestData(ctx context.Context, p *cluster.Proposal) error {
	backoff := ingestBackoffMin
	for {
		err := db.Propose(ctx, p)
		if !errors.Is(err, cluster.ErrInsufficientStorage) {
			return err
		}
		db.logger.Warn("ingest paused: insufficient disk space, will retry",
			zap.String("id", p.IngestableID),
			zap.Uint64("sourceSeq", p.SourceSeq),
			zap.Duration("backoff", backoff))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > ingestBackoffMax {
			backoff = ingestBackoffMax
		}
	}
}
