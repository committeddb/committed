package db

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// ingestExitReason classifies why db.ingest returned. The worker-launch
// goroutine inspects it to decide whether to spawn the supervisor:
// ingestExitFreeze means the worker needs an operator-style supervised
// restart — either it parked in the ErrProposalUnknown branch, or its inner
// Ingest exited unexpectedly (a fatal source/config error or a panic) while
// the node still owned the ingestable. ingestExitShutdown means the worker
// exited because its ctx was canceled for another reason (db.Close or
// registry replace) and the normal teardown already handles recovery.
type ingestExitReason int

const (
	ingestExitShutdown ingestExitReason = iota
	ingestExitFreeze
)

// featureLevelTxnScopedDedup gates the transaction-scoped ingest dedup
// regime: an ingestable's dedup record first takes the transaction-carrying
// shape (which a pre-level-2 binary reads as "nothing seen") only once every
// member announces version.FeatureLevel >= 2. Enforced where the worker
// receives a dialect's proposals (db.ingest), by clearing the dialect's
// TxnScopedDedup stamp before the proposal is marshaled.
const featureLevelTxnScopedDedup uint64 = 2

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

	// ingestPipelineDepth bounds the snapshot pipeline's in-flight window:
	// bare snapshot rows submitted without waiting for apply, so raft
	// coalesces their entries into shared Ready fsyncs. Measured on the
	// BenchmarkIngestShape 512-row window (256B rows, real fsync): depth 64
	// → 462ms, 256 → 150ms, 1024 → 89ms vs 29ms for the old counterfeit
	// batch — each Ready cycle has a ~25ms fsync floor, so the window must
	// hold several cycles' worth of rows. Freeze abandonment is bounded by
	// this depth and restart re-emission by the dialect read window (10k),
	// both idempotent for seq-0 rows.
	ingestPipelineDepth = 1024

	// ingestPipelineBytes bounds the in-flight window's estimated size, well
	// under raft's uncommitted-entries limit (MaxUncommittedEntriesSize, 1
	// GiB), past which raft drops proposals.
	ingestPipelineBytes = 64 << 20
)

// featureLevelIngestOrder gates the ingest ordering check (see
// cluster.Proposal.IngestSeq): a worker numbers and pipelines its ordered
// proposals only once every member announces version.FeatureLevel >= 3, so
// no member applies a sequenced proposal without the check.
const featureLevelIngestOrder uint64 = 3

// newIngestSession returns a random, non-zero ordering session id.
func newIngestSession() uint64 {
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		// Same posture as randomRequestIDBase: crypto/rand does not fail on a
		// supported platform.
		panic("db: crypto/rand failed generating an ingest session: " + err.Error())
	}
	return binary.LittleEndian.Uint64(b[:]) | 1
}

// proposalBytes estimates a proposal's marshaled size for the in-flight
// window's byte bound.
func proposalBytes(p *cluster.Proposal) int {
	n := len(p.Position)
	for _, e := range p.Entities {
		n += len(e.Key) + len(e.Data) + 64
	}
	return n
}

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
	// exited signals an UNEXPECTED inner-Ingest exit — a fatal source/config
	// error or a panic while the session was still meant to be running — so
	// db.ingest's loop can freeze and hand the worker to the supervisor instead
	// of idling forever with a healthy-looking gauge. A clean, ctx-driven
	// teardown (stop/loseOwnership/shutdown) never sends on it. nil between
	// sessions: start() makes a fresh buffered channel, stop() clears it.
	exited chan error
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
	exited := make(chan error, 1)
	l.exited = exited
	l.wg.Go(func() {
		var err error
		// Backstop: a panic in a dialect's Ingest (e.g. a decode panic on a
		// malformed CDC frame that escapes the dialect's own recover) must not
		// unwind this goroutine and crash the whole node. Recover it into a loud
		// log and treat it as an unexpected exit so the worker freezes and the
		// supervisor restarts it — the node, and every other syncable/ingestable,
		// stays up. (The Postgres dialect additionally recovers in-stream so a
		// transient bad frame self-heals via reconnect.)
		defer func() {
			if r := recover(); r != nil {
				zap.L().Error("ingest worker panicked; freezing for supervised restart",
					zap.Any("panic", r), zap.Stack("stack"))
				err = fmt.Errorf("ingest worker panicked: %v", r)
			}
			// Signal ONLY an unexpected exit: a non-nil error (or panic) while
			// this session's ctx is still live. A clean teardown cancels ictx
			// (stop/loseOwnership/db.Close), on which Ingest returns nil or a ctx
			// error — expected, so stay silent and let the normal path run.
			// Buffered channel: the send never blocks and needs no reader.
			if err != nil && ictx.Err() == nil {
				exited <- err
			}
		}()
		err = i.Ingest(ictx, pos, l.proposalChan, l.positionChan)
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
	// Drop this session's exit channel: a freeze signal buffered just before an
	// intentional teardown (a fatal error racing loseOwnership/replace) must not
	// spuriously freeze the next session — the teardown wins. start() makes a
	// fresh one. Safe: same goroutine as the loop's select, and the worker has
	// already exited (wg.Wait above).
	l.exited = nil
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
	if db.safeMode {
		// Safe mode (WithSafeMode): the config is stored and visible, but no
		// worker starts. Release what the parse built (a source pool, a binlog
		// syncer) so a held ingestable doesn't leak on every apply/reconcile.
		db.logger.Warn("SAFE MODE: ingest worker held; delete or fix the config over the API, then restart without COMMITTED_SAFE_MODE to resume",
			zap.String("id", id))
		if i != nil {
			if err := i.Close(); err != nil {
				db.logger.Warn("held ingestable close failed", zap.String("id", id), zap.Error(err))
			}
		}
		return nil
	}
	err := db.workers.replace(workerKindIngest, id,
		func(replaced bool) *workerHandle {
			if replaced {
				// A re-POST replaced the worker — a new worker generation. Drop the
				// supervisor give-up state so the fresh worker starts with a full restart
				// budget instead of inheriting a prior give-up (this is the "raise the cap
				// and re-POST" recovery). Works for a byte-identical re-POST too, which
				// does not bump the config version but still replaces the worker here.
				db.ingestSupervisor.prune(id)
				if db.metrics != nil {
					db.metrics.WorkerReplaced("ingest", id)
				}
			}
			return db.newIngestWorker(id, i)
		},
		func(prev *workerHandle, drained bool) {
			if !drained {
				db.logger.Warn("ingest replace: prior worker did not exit in time; abandoning it (wedged on its source?) and proceeding",
					zap.String("id", id), zap.Duration("timeout", db.workers.drainTimeout))
			}
			// Release the superseded ingestable's source resources (only if it drained,
			// so we don't race a wedged worker) — otherwise every re-POST leaks them.
			db.closeDrainedIngestable(prev, id)
		})
	if err != nil {
		return err
	}
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
// cancelIngestWorker cancels and deregisters id's worker and releases its
// node-local source resources — the shared front half of deleteIngest and the
// reconcile absent-worker cancel (which must NOT run the owner-only source
// teardown: reconcile never touches sources). Returns the drained handle (nil
// if no worker was built here).
func (db *DB) cancelIngestWorker(id string) *workerHandle {
	// Condemned + cancelled under one hold, drained outside it, deregistered
	// on relock — the registry's remove. beforeCancelIngestRelockForTest is
	// the test-only poise point inside that drain window. See
	// workerHandle.condemned and superviseRestartIngest.
	handle, drained := db.workers.remove(workerKindIngest, id, abandonOnWedge, db.beforeCancelIngestRelockForTest)
	if handle != nil && !drained {
		db.logger.Warn("delete ingest: worker did not exit in time; abandoning it (wedged on its source?) and proceeding",
			zap.String("id", id), zap.Duration("timeout", db.workers.drainTimeout))
	}

	// cancelIngestWorker's only callers are delete and the reconcile
	// absent-cancel — both mean this id's config is GONE. Drop its supervisor
	// give-up state so a later recreate of the same id starts with a fresh
	// restart budget (and the state map stays bounded).
	db.ingestSupervisor.prune(id)

	if handle == nil || handle.ingestable == nil {
		return nil // no worker built on this node — nothing to clean up
	}

	// Release the ingestable's source resources (Close). Node-local cleanup —
	// done on every node that built a worker (if it drained), before and
	// independent of any owner-only slot teardown the caller may run.
	db.closeDrainedIngestable(handle, id)
	return handle
}

// reconcileIngestWorkers is the ingest twin of reconcileSyncWorkers; see it
// for the serialization and cancel-only rationale.
func (db *DB) reconcileIngestWorkers(list func() ([]*IngestableWithID, error)) {
	backoff := reconcileRetryMin
	var parsed []*IngestableWithID
	for {
		var err error
		parsed, err = list()
		if err == nil {
			break
		}
		db.logger.Error("ingest reconcile: listing configs failed; ingest workers not reconciled (data plane degraded), retrying",
			zap.Error(err))
		select {
		case <-db.ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, reconcileRetryMax)
	}
	present := make(map[string]struct{}, len(parsed))
	installed, degraded := 0, 0
	for _, iw := range parsed {
		present[iw.ID] = struct{}{}
		if iw.Ingestable == nil {
			degraded++
			continue // existing-but-degraded: keep the running worker
		}
		if err := db.Ingest(context.Background(), iw.ID, iw.Ingestable); err != nil {
			return // ErrClosed: db shutting down
		}
		installed++
	}
	ids := db.workers.ids(workerKindIngest)
	cancelled := 0
	for _, id := range ids {
		if _, ok := present[id]; !ok {
			db.logger.Warn("ingest reconcile: cancelling worker for a config that no longer exists (deleted, incl. via snapshot)",
				zap.String("id", id))
			db.cancelIngestWorker(id)
			cancelled++
		}
	}
	db.logger.Info("ingest reconcile complete",
		zap.Int("installed", installed), zap.Int("degraded", degraded), zap.Int("cancelled", cancelled))
}

func (db *DB) deleteIngest(id string) {
	handle := db.cancelIngestWorker(id)
	if handle == nil {
		// No worker built on this node. In safe mode that is EVERY delete,
		// and on a single-node cluster nobody else will tear down either —
		// warn loudly: an orphaned replication slot pins the source's WAL.
		if db.safeMode {
			db.logger.Warn("safe mode: ingestable deleted; source-side teardown skipped (no worker was built) — a replication slot/publication may remain at the source and pin its WAL; drop it manually (see docs/ingestables.md)",
				zap.String("id", id))
		}
		return
	}

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
	if err, completed := runBounded(db.workers.drainTimeout, teardownable.Teardown); !completed {
		db.logger.Error("ingestable deleted but source teardown did not return in time (unreachable source?); abandoning it (an orphaned replication slot may pin the source's WAL; drop it manually)",
			zap.String("id", id), zap.Duration("timeout", db.workers.drainTimeout))
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
// The Ingestable's Status is called without holding workers.mu — it makes a
// source query and must not block the worker registry.
func (db *DB) IngestableStatus(ctx context.Context, id string) (cluster.IngestableStatus, error) {
	// Parked is a REPLICATED terminal state — report it from ANY node, even one
	// with no local worker handle (a follower, or the owner after the supervisor
	// gave up and stopped the worker). Checked before the local-handle lookup so a
	// follower does not 404 a parked worker; this is what makes a dead ingest worker
	// visible cluster-wide instead of only where its worker last ran.
	if _, parked, err := db.storage.IngestableStuck(id); err == nil && parked {
		return cluster.IngestableStatus{WorkerState: cluster.WorkerStateParked}, nil
	}

	handle := db.workers.lookup(workerKindIngest, id)
	ok := handle != nil
	var ing cluster.Ingestable
	if ok {
		ing = handle.ingestable
	}

	if ing == nil {
		return cluster.IngestableStatus{}, cluster.ErrIngestableNotRunning
	}

	st, err := ing.Status(ctx, db.storage.Position(id))
	if err != nil {
		return st, err
	}
	// A live worker is either running or — if the supervisor is restarting it after
	// a freeze — recovering, a node-local, self-healing state (contrast the
	// replicated terminal parked state above). The phase/position still reflect
	// where it wedged; workerState is the health headline.
	st.WorkerState = cluster.WorkerStateRunning
	if db.ingestSupervisor.isFrozen(id) {
		st.WorkerState = cluster.WorkerStateRecovering
	}
	return st, nil
}

// TopicRefreshEpoch exposes the storage's delete-surviving per-topic
// refresh-epoch highwater to the SQL ingest worker (wired as the
// IngestableParser's EpochFloor), so a same-topic recreate resumes its
// generation above the rows still on the sink rather than resetting to epoch 1.
// See wal.Storage.TopicRefreshEpoch. Satisfies ingestablesql.TopicEpochReader.
func (db *DB) TopicRefreshEpoch(topic string) uint64 {
	return db.storage.TopicRefreshEpoch(topic)
}

// newIngestWorker builds a fresh ingest worker handle for id and starts
// the worker goroutine; the CALLER registers the returned handle, holding
// db.workers.mu with the slot for id already empty and the registry not
// closed. Shared between the registry's replace (on behalf of db.Ingest,
// which drains any prior entry first) and superviseRestartIngest (which
// drops the frozen handle directly under the same lock, then registers the
// replacement).
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
func (db *DB) newIngestWorker(id string, i cluster.Ingestable) *workerHandle {
	// cancel ownership passes to the workerHandle. It is invoked by
	// db.Close and by the replace loop in db.Ingest when a duplicate
	// supersedes this worker, so the cancel is not leaked. gosec can't
	// see through the handle indirection.
	workerCtx, cancel := context.WithCancel(db.ctx) //nolint:gosec // G118: cancel owned by workerHandle
	handle := &workerHandle{cancel: cancel, ctx: workerCtx, done: make(chan struct{}), ingestable: i}

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
			// Drain apply up to the current commit index BEFORE the supervisor
			// restarts this worker from its durable position. Any freeze path can
			// leave pipelined rows or a just-failed proposal COMMITTED but not yet
			// APPLIED (acks fire at apply, which can lag commit under a busy Ready
			// loop). The durable position advances only at apply, so restarting on a
			// stale position would re-read behind those committed batches and re-emit
			// them — duplicating them in the permanent event log, which is a raw
			// mirror of the committed raft log and cannot drop a duplicate at apply
			// (P_local==R_local). Every index <= commit is committed and therefore
			// guaranteed to apply (or the node fatals), so this returns once the
			// position reflects all committed data — restoring the effectively-once
			// ingest contract at the one point it
			// can be: before the re-read. It sits here, at the shared freeze exit,
			// because every ingestExitFreeze path has the same hazard. Fix #1 keeps
			// the wait short (apply is no longer scrub-starved); a longer wait is the
			// intended backpressure. workerCtx is still live on a freeze (a normal
			// return, not a cancel); if a concurrent replace/Close cancels it mid-
			// drain, waitForApplied returns its error and we restart anyway — the
			// supervisor's preflight resolves that race.
			if db.raft != nil {
				drainCtx := workerCtx
				if db.ingestDrainTimeoutForTest > 0 {
					var cancel context.CancelFunc
					drainCtx, cancel = context.WithTimeout(workerCtx, db.ingestDrainTimeoutForTest)
					defer cancel()
				}
				if derr := db.waitForApplied(drainCtx, db.raft.CommitIndex()); derr != nil {
					db.logger.Debug("ingest freeze: apply-drain interrupted; restarting without full drain",
						zap.String("id", id), zap.Error(derr))
				}
			}
			// The worker froze and the supervisor is about to restart it — mark it
			// recovering (node-local) so the status endpoint stops reporting a live
			// "streaming" phase for a worker that is actually wedged and retrying.
			db.ingestSupervisor.setFrozen(id, true)
			go db.superviseRestartIngest(id, i, handle)
		}
	}()

	return handle
}

// clearIngestFrozen marks id un-frozen after the worker makes durable progress
// (a checkpoint advanced past a freeze), and clears the matching gauge — the
// flag/gauge pairing is the one composition the supervisor's setFrozen doesn't
// own. The supervisor no longer clears the gauge on restart — a restart is not
// recovery — so this progress signal is what un-sets it. It fires only on a
// committed checkpoint, which a poison proposal blocks, so it never flaps
// against the freeze that set the gauge.
func (db *DB) clearIngestFrozen(id string) {
	db.ingestSupervisor.setFrozen(id, false)
	if db.metrics != nil {
		db.metrics.IngestFrozen(id, false)
	}
}

// proposalTopic returns the topic/type id of a proposal's first entity for
// freeze diagnostics (which topic an ingest proposal targets), or "" when
// unknown. The source table itself is not on the proposal — the dialect has
// already mapped it to this type.
func proposalTopic(p *cluster.Proposal) string {
	if p == nil || len(p.Entities) == 0 || p.Entities[0].Type == nil {
		return ""
	}
	return p.Entities[0].Type.ID
}

func (db *DB) ingest(ctx context.Context, id string, i cluster.Ingestable) ingestExitReason {
	isNode := false
	// lineageRideNoted dedups the once-per-run failover ride-through Info in
	// the dedup branch (the dialect stamps LineageRegressed on every proposal
	// of the regressed lineage; the note only needs saying once).
	lineageRideNoted := false
	// dedupFlipHeldNoted likewise dedups the once-per-run Info that the
	// transaction-scoped dedup regime is waiting on the cluster feature level.
	dedupFlipHeldNoted := false

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

	// The JSON shape census (see census.go): folds snapshot-phase rows,
	// publishes replicated census state. nil (all methods no-op) when the
	// operator opted out or no config is stored.
	census := db.newCensusRecorder(id)

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

	// The pipeline: proposals submitted WITHOUT waiting for apply, up to
	// ingestPipelineDepth (and ingestPipelineBytes) in flight, so raft
	// coalesces their entries into shared Ready fsyncs. Two kinds ride it:
	//
	//   - Bare snapshot rows (SourceSeq 0, no bundled Position, no refresh
	//     marker): independent single-row transactions whose relative log
	//     order is meaningless (unique keys within a snapshot), so a
	//     leader-flap hole re-filled after restart cannot reorder anything
	//     observable.
	//   - In an ordering session (feature level 3), every ORDERED proposal:
	//     CDC transactions (source order must survive into the immutable
	//     log), bundled-position rows and standalone checkpoints (a
	//     checkpoint must trail every row it covers), refresh markers (the
	//     sweep must follow the rows it stamps). Each is numbered
	//     (cluster.Proposal.IngestSeq) and apply rejects one that does not
	//     directly follow the last accepted, so a proposal lost before commit
	//     can never be skipped by a later one. An ordered proposal still waits
	//     for in-flight bare rows, which its checkpoint may cover.
	//
	// Below the feature level an ordered proposal first DRAINS the pipeline
	// to success and then proposes synchronously. Any submit or ack failure
	// arms the position barrier and freezes; the supervisor's
	// restart-from-durable-position re-emits what never applied (bare rows
	// as upsert-idempotent duplicates, today's crash semantics).
	type inflight struct {
		rid uint64
		ack <-chan error
		at  time.Time
		// bytes is the proposal's estimated size, against
		// ingestPipelineBytes.
		bytes int
		// sequenced marks an ordered proposal stamped with IngestSeq; the
		// rest are bare snapshot rows.
		sequenced bool
		// done runs once the proposal applied, in submission order (OnDurable,
		// clearing the frozen gauge).
		done func()
		// For the freeze log line.
		sourceSeq, ingestSeq uint64
		topic                string
	}
	pipeline := make([]inflight, 0, ingestPipelineDepth)
	pipelineBytes, unsequencedInFlight := 0, 0
	defer func() {
		// Whatever path exits the worker, no waiter may leak.
		for _, f := range pipeline {
			db.unregisterWaiter(f.rid)
		}
	}()
	// popOldest removes the front in-flight proposal.
	popOldest := func() inflight {
		f := pipeline[0]
		pipeline = pipeline[1:]
		pipelineBytes -= f.bytes
		if !f.sequenced {
			unsequencedInFlight--
		}
		db.unregisterWaiter(f.rid)
		return f
	}
	// awaitOldest blocks on the front in-flight ack and, on success, runs
	// its done. ctx cancellation resolves to ctx.Err() (worker shutdown).
	awaitOldest := func() (inflight, error) {
		f := pipeline[0]
		// Backstop timer, mirroring db.Propose: a proposal raft DROPS without a
		// leader change (uncommitted-entries buffer full, an expired transfer, or a
		// leaderless-window submit the Old==0 fail-fast skip doesn't cover) is
		// absorbed by forwardProposeErr, so this ack would never fire and the
		// worker would hang here forever — silently, with a healthy-looking
		// gauge. Surface it as ErrProposalUnknown so the worker freezes visibly and
		// the supervisor restarts from the durable position. NewTimer+Stop, not
		// time.After, so a fast ack doesn't leave a timer pinned per drained row.
		timer := time.NewTimer(max(time.Until(f.at.Add(db.proposeTimeout)), 0))
		defer timer.Stop()
		var err error
		select {
		case err = <-f.ack:
		case <-timer.C:
			err = ErrProposalUnknown
		case <-ctx.Done():
			err = ctx.Err()
		}
		popOldest()
		if err == nil && f.done != nil {
			f.done()
		}
		return f, err
	}
	// drainPipeline awaits every in-flight ack in FIFO order; the first
	// failure abandons (unregisters) the rest — the worker is about to
	// freeze, and their outcomes no longer matter.
	drainPipeline := func() (inflight, error) {
		for len(pipeline) > 0 {
			if f, err := awaitOldest(); err != nil {
				return f, err
			}
		}
		return inflight{}, nil
	}
	// submitPipelined submits without waiting, mirroring
	// proposeIngestData's disk-pressure pause, and applies backpressure by
	// awaiting the oldest ack once the window is full. A failed ack it
	// awaits is returned with the proposal it belonged to.
	submitPipelined := func(p *cluster.Proposal, sequenced bool, done func()) (inflight, error) {
		// The validation tripwire runs in db.Propose, which this pipelined
		// lane bypasses (proposeAsync directly) — so run it here: divergent
		// rows announce too. A signal, never a gate.
		db.announceDivergences(ctx, p)
		f := inflight{
			at: time.Now(), bytes: proposalBytes(p), sequenced: sequenced, done: done,
			sourceSeq: p.SourceSeq, ingestSeq: p.IngestSeq, topic: proposalTopic(p),
		}
		pauseBackoff := ingestBackoffMin
		for {
			rid, ack, err := db.proposeAsync(ctx, p)
			if err == nil {
				f.rid, f.ack = rid, ack
				pipeline = append(pipeline, f)
				pipelineBytes += f.bytes
				if !sequenced {
					unsequencedInFlight++
				}
				break
			}
			if !errors.Is(err, cluster.ErrInsufficientStorage) {
				return f, err
			}
			db.logger.Warn("ingest paused: insufficient disk space, will retry",
				zap.String("id", p.IngestableID),
				zap.Duration("backoff", pauseBackoff))
			select {
			case <-ctx.Done():
				return f, ctx.Err()
			case <-time.After(pauseBackoff):
			}
			pauseBackoff = min(pauseBackoff*2, ingestBackoffMax)
		}
		for len(pipeline) >= ingestPipelineDepth || pipelineBytes > ingestPipelineBytes {
			if g, err := awaitOldest(); err != nil {
				return g, err
			}
		}
		return inflight{}, nil
	}
	// awaitDurable queues done behind the in-flight proposals, so a
	// dropped duplicate reports durable in order; with nothing in flight it
	// runs now.
	awaitDurable := func(done func()) {
		if len(pipeline) == 0 {
			done()
			return
		}
		ack := make(chan error, 1)
		ack <- nil
		pipeline = append(pipeline, inflight{ack: ack, at: time.Now(), sequenced: true, done: done})
	}

	// The ordering session (see cluster.Proposal.IngestSeq): once every
	// member can apply the ordering check, each run of the inner Ingest is a
	// session whose ordered proposals are numbered and pipelined; below the
	// level the ordered lane stays serial.
	var order struct {
		on      bool
		next    uint64
		session uint64
		start   bool
		// dedupTxn and dedupSeq are the dedup record as it will stand once
		// everything this session submitted applies: the applied record at
		// session start, folded with each submitted proposal. A dialect that
		// reconnects inside a session re-emits from its last handed-off
		// position, which may be a part still in flight; the applied record
		// alone would not see it. Exact, because a session's proposals apply
		// in order or the session ends (any failure freezes, and the next
		// session reseeds from the applied record).
		dedupTxn string
		dedupSeq uint64
	}
	startSession := func() {
		order.on = db.featureEnabled(featureLevelIngestOrder)
		if !order.on {
			return
		}
		last, _ := db.storage.IngestOrder(id)
		order.next, order.session, order.start = last+1, newIngestSession(), true
		order.dedupTxn, order.dedupSeq = db.storage.IngestSourceDedup(id)
	}
	stamp := func(p *cluster.Proposal) {
		p.IngestableID = id
		p.IngestSeq, p.IngestSession, p.IngestSessionStart = order.next, order.session, order.start
		order.next++
		order.start = false
		if p.SourceSeq > 0 {
			txn := ""
			if p.TxnScopedDedup {
				txn = p.SourceTxnID
			}
			order.dedupTxn, order.dedupSeq = cluster.FoldSourceDedup(order.dedupTxn, order.dedupSeq, txn, p.SourceSeq)
		}
	}
	// proposeFailed logs a failed data proposal and reports whether the
	// worker must freeze: every failure freezes, except a ctx cancellation
	// (the worker shutting down).
	proposeFailed := func(err error, f inflight, entities int) bool {
		// Arm the position barrier BEFORE classifying the error: no later
		// standalone checkpoint may pass this hole, whatever the caller
		// decides.
		dataProposeFailed = true
		db.logger.Warn("ingest propose failed; freezing the worker (the supervisor restarts it from the durable resume position). If this is ErrProposalTooLarge, one row or transaction exceeds COMMITTED_MAX_PROPOSAL_BYTES — raise the cap and restart, or split the source write",
			zap.String("id", id),
			zap.Uint64("sourceSeq", f.sourceSeq),
			zap.Uint64("ingestSeq", f.ingestSeq),
			zap.String("topic", f.topic),
			zap.Int("entities", entities),
			zap.Int("inFlight", len(pipeline)),
			zap.Error(err))
		if ctx.Err() != nil {
			return false
		}
		if db.metrics != nil {
			db.metrics.IngestError(id, "propose")
			db.metrics.IngestFrozen(id, true)
		}
		return true
	}

	// loseOwnership tears the running ingress session down the moment this node
	// stops owning the ingestable (a leadership change on a leader-based
	// ingestable). It cancels the inner Ingest — releasing the source's
	// replication slot / read cursor — flips the local ownership flag, and
	// ABANDONS the in-flight pipeline without reporting anything durable.
	// Bare snapshot rows (SourceSeq 0) never advanced the durable position, so
	// restart-from-position re-reads them idempotently. A sequenced proposal
	// may still commit after this: the ordering check then accepts it only
	// ahead of the next session's first proposal, which it rejects (and the
	// next session restarts past it), or rejects it behind that proposal.
	// Abandoning rather than draining keeps a deposed session's residue from
	// contaminating the next session's pipeline (a stale flap-lost ack would
	// otherwise spuriously freeze the fresh worker). Called from the data
	// cases (prompt teardown while the source is active) and the timer branch
	// (the idle path). Idempotent.
	loseOwnership := func() {
		db.logger.Info("stopping ingestion: node no longer owns this ingestable", zap.String("id", id))
		isNode = false
		ingress.stop()
		for _, f := range pipeline {
			db.unregisterWaiter(f.rid)
		}
		pipeline = pipeline[:0]
		pipelineBytes, unsequencedInFlight = 0, 0
	}
	// front is the oldest in-flight ack, or nil (blocks forever in a
	// select) with nothing in flight.
	front := func() <-chan error {
		if len(pipeline) == 0 {
			return nil
		}
		return pipeline[0].ack
	}

	for {
		select {
		case <-ctx.Done():
			return ingestExitShutdown
		case err := <-ingress.exited:
			// The inner Ingest returned on its own while we still owned the
			// ingestable and weren't tearing it down — a fatal source/config
			// error or a panic (a portless MySQL URL that passes the lenient
			// admission DSN but binlogSyncerConfig rejects, an unrecoverable
			// stream fault, etc.). Freeze so the supervisor restarts it and the
			// parked state is observable, instead of idling forever reporting a
			// live "streaming" phase — a silently-dead CDC worker whose mirror
			// diverges. A racing shutdown is just a normal teardown.
			if ctx.Err() != nil {
				return ingestExitShutdown
			}
			db.logger.Error("ingest worker exited unexpectedly; freezing for supervised restart",
				zap.String("id", id), zap.Error(err))
			if db.metrics != nil {
				db.metrics.IngestFrozen(id, true)
			}
			return ingestExitFreeze
		case err := <-front():
			// The oldest in-flight proposal resolved: report it durable, in
			// submission order, or freeze.
			f := popOldest()
			if err != nil {
				if proposeFailed(err, f, 0) {
					return ingestExitFreeze
				}
				continue
			}
			if f.done != nil {
				f.done()
			}
		case proposal := <-ingress.proposalChan:
			if db.isNode(id) {
				// Stamp the ingestable id so the apply path advances this
				// ingestable's source-seq highwater when the proposal
				// applies.
				proposal.IngestableID = id

				// Effectively-once dedup, TRANSACTION-scoped. After a
				// crash/flap the dialect resumes from its durable watermark
				// and can re-emit only the single in-flight transaction (the
				// commit flush bundles the position, so anything older is in
				// the resume set). A re-emitted part is a duplicate only if
				// it belongs to the SAME source transaction as the dedup
				// record with a seq at or below its highwater; a proposal
				// from a different transaction is never compared against
				// another transaction's coordinates — post-failover
				// low-numbered lineages and cross-transaction coordinate
				// inflation are harmless by construction. The legacy scalar
				// regime (stored record has no transaction identity —
				// pre-upgrade state, or a dialect that stamps none) keeps
				// the old global comparison. Dropping pre-raft keeps the
				// duplicate out of the committed event log entirely, so
				// syncables never see it. SourceSeq==0 (snapshot rows /
				// non-CDC / legacy) is never deduped.
				if proposal.SourceSeq > 0 {
					storedTxn, storedSeq := db.storage.IngestSourceDedup(id)
					if order.on {
						// In flight is not yet applied: compare against
						// what this session has submitted (see stamp).
						storedTxn, storedSeq = order.dedupTxn, order.dedupSeq
					}
					scalarRegime := storedTxn == ""
					// The regime FLIP (scalar → transaction-scoped) waits on
					// the cluster feature level: the record the flip writes
					// reads as "nothing seen" to an older binary, and an
					// older owner would re-ingest the resume window. Until
					// every member can read it, the stamp is cleared here —
					// before the proposal is marshaled — and the record keeps
					// the legacy scalar shape. Once flipped, the stamp is
					// honored regardless of the minimum: a member still
					// announcing (a joining learner reads as level 0) would
					// otherwise regress the record to scalar and re-open the
					// cross-transaction collapse this regime exists to close.
					if proposal.TxnScopedDedup && scalarRegime && !db.featureEnabled(featureLevelTxnScopedDedup) {
						proposal.TxnScopedDedup = false
						if !dedupFlipHeldNoted {
							dedupFlipHeldNoted = true
							db.logger.Info("ingest dedup: holding the transaction-scoped dedup regime until the cluster is fully upgraded; deduping by the legacy global highwater meanwhile",
								zap.String("id", id),
								zap.Uint64("required", featureLevelTxnScopedDedup),
								zap.Uint64("clusterMin", db.clusterMinFeatureLevel()))
						}
					}
					flipsRecord := proposal.TxnScopedDedup && scalarRegime
					dedupTxn := ""
					if proposal.TxnScopedDedup {
						dedupTxn = proposal.SourceTxnID
					}
					dup := proposal.SourceSeq <= storedSeq &&
						(scalarRegime || dedupTxn == storedTxn)
					switch {
					case dup && (proposal.DedupUnsafe || (proposal.LineageRegressed && scalarRegime)):
						// The dialect cannot vouch for this drop: a
						// same-coordinate multi-part replay under changed
						// chunking (DedupUnsafe — a WITHIN-transaction risk
						// scoping can't help), or a lineage regression while
						// the stored record is still the legacy scalar
						// regime, where a scalar comparison could silently
						// drop the new lineage's data (the upgrade window:
						// the first transaction-stamped apply flips the
						// regime and retires this freeze). FREEZE loudly —
						// the fail-safe the system-of-record contract
						// requires.
						db.logger.Error("ingest dedup: proposal at or below the dedup highwater is flagged unsafe to drop (re-chunk under changed config/binary, or lineage regression against a legacy scalar record); freezing to avoid silent data loss — operator intervention required",
							zap.String("id", id),
							zap.Uint64("sourceSeq", proposal.SourceSeq),
							zap.Uint64("highwater", storedSeq),
							zap.String("storedTxn", storedTxn))
						if db.metrics != nil {
							db.metrics.IngestFrozen(id, true)
						}
						return ingestExitFreeze
					case dup:
						db.logger.Debug("ingest dedup skip",
							zap.String("id", id), zap.Uint64("sourceSeq", proposal.SourceSeq))
						if db.metrics != nil {
							db.metrics.IngestDedupSkipped(id)
						}
						// The applied copy is durable, so this one is too.
						awaitDurable(proposal.Durable)
						backoff = ingestBackoffMin
						continue
					case proposal.LineageRegressed && !lineageRideNoted:
						// Transaction-scoped record + regressed coordinates =
						// the failover ride-through working as designed; note
						// it once per worker run so the operator can see the
						// promotion was absorbed without a freeze.
						lineageRideNoted = true
						db.logger.Info("ingest: source coordinate lineage regressed (failover/promotion) — riding through under the transaction-scoped watermark, no operator action needed",
							zap.String("id", id))
					}
					if flipsRecord && dedupFlipHeldNoted {
						// The hold this run announced is over, and this
						// proposal is neither a replayed part nor frozen:
						// its apply flips the record.
						dedupFlipHeldNoted = false
						db.logger.Info("ingest dedup: the cluster is fully upgraded; the transaction-scoped dedup regime takes effect with this transaction",
							zap.String("id", id))
					}
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

				// Census: fold snapshot rows (a refresh-boundary marker forces
				// the pass's census to publish before the boundary commits).
				// Never gates — see census.go.
				census.observe(ctx, proposal)

				// Lane split. Bare snapshot rows pipeline unsequenced. In an
				// ordering session, every ordered proposal (CDC, bundled-position
				// rows, markers) is numbered and pipelined too: the apply-side
				// check keeps the log in source order with none skipped. It
				// waits only for in-flight bare rows, which its checkpoint may
				// cover. Below the feature level, ordered proposals drain the
				// pipeline to success and then propose synchronously.
				// proposeIngestData and submitPipelined retry (not drop) on
				// disk-pressure rejection, so a full disk pauses ingestion
				// cleanly without advancing the position past uncommitted data.
				bare := proposal.SourceSeq == 0 && len(proposal.Position) == 0 && !containsRefreshBoundary(proposal)
				var f inflight
				var err error
				switch {
				case bare:
					f, err = submitPipelined(proposal, false, nil)
				case order.on:
					if unsequencedInFlight > 0 {
						f, err = drainPipeline()
					}
					if err == nil {
						stamp(proposal)
						p := proposal
						f, err = submitPipelined(p, true, func() {
							p.Durable()
							if len(p.Position) > 0 {
								// A committed bundled checkpoint advanced the
								// durable position — real progress. Clear the
								// frozen gauge (the supervisor no longer clears
								// it on restart).
								db.clearIngestFrozen(id)
							}
						})
					}
				default:
					f, err = drainPipeline()
					if err == nil {
						f = inflight{sourceSeq: proposal.SourceSeq, topic: proposalTopic(proposal)}
						err = db.proposeIngestData(ctx, proposal)
					}
					if err == nil {
						// Propose returns once the proposal is applied.
						proposal.Durable()
						if len(proposal.Position) > 0 {
							// A committed bundled checkpoint advanced the
							// durable position — real progress. A poison
							// proposal never reaches here, so this never flaps
							// against a freeze.
							db.clearIngestFrozen(id)
						}
					}
				}
				if err != nil {
					if f.topic == "" && f.sourceSeq == 0 {
						f = inflight{sourceSeq: proposal.SourceSeq, ingestSeq: proposal.IngestSeq, topic: proposalTopic(proposal)}
					}
					// Freeze on EVERY propose failure — the conservative
					// default. ErrProposalUnknown (status unknown after a
					// leader change) and ErrProposalLost (entry truncated
					// before commit) are the classic leader-flap orphan
					// signals: we don't know (or know it didn't) commit,
					// and continuing would let the next checkpoint advance
					// the position past data that never committed.
					// ErrIngestOutOfOrder means an earlier proposal of this
					// session never committed. The same reasoning holds for
					// ANY other failure — a deterministic rejection like
					// ErrProposalTooLarge or an error this code has never
					// seen. The worker never rewrites a proposal to make it
					// fit: a proposal is an opaque atomic unit whose
					// composition (grouping AND sizing) belongs to its
					// emitter — snapshot dialects emit one row per proposal,
					// so only a single row larger than the cap (or a giant
					// CDC transaction) lands here, and it freezes loudly. The
					// pre-barrier bug was exactly a branch that chose to
					// continue; the default must hold the line, and the
					// supervisor's restart-from-durable-position is the one
					// recovery that is always correct — once the durable
					// position it re-reads is CURRENT. It may lag here: the
					// pipelined proposals and this failed one can be
					// committed-but-unapplied (acks fire at apply, which can
					// lag commit under a busy Ready loop). The freeze-exit
					// handler (newIngestWorker) drains apply to the commit
					// index before the supervisor re-reads the position, so a
					// committed batch is never re-emitted into the permanent
					// event log — the effectively-once contract. The drain is
					// central there because every freeze path shares this
					// hazard.
					//
					// ingress.stop (from the defer above) cancels the inner
					// Ingest goroutine cleanly on return, unblocking its
					// pending positionChan send. The position value is
					// discarded (we must not advance past the failed
					// proposal), which is the whole point of freezing here.
					if proposeFailed(err, f, len(proposal.Entities)) {
						return ingestExitFreeze
					}
					// ctx canceled mid-propose: worker shutdown, not an
					// ingest failure — the next select observes ctx.Done.
				}
			} else {
				// Lost leadership mid-stream: the row we just received belongs
				// to a source this node no longer owns. Drop it and tear the
				// session down — the durable position never advanced past it,
				// so restart-from-position re-reads it. A clean handoff, never
				// a "propose failed".
				loseOwnership()
			}
			backoff = ingestBackoffMin
		case position := <-ingress.positionChan:
			if !db.isNode(id) {
				// Lost leadership: don't drain or checkpoint. Draining here
				// could await a flap-lost in-flight ack and spuriously freeze a
				// clean handoff; instead tear down and abandon the pipeline —
				// restart-from-position re-reads. The position is discarded (we
				// must never advance it while not the owner).
				loseOwnership()
				backoff = ingestBackoffMin
				continue
			}
			// A standalone checkpoint summarizes "all data through here
			// committed". Every bare row ahead of it must succeed before it
			// may propose; in an ordering session, sequenced proposals ahead
			// of it need not, since the apply check rejects it behind a gap.
			if !order.on || unsequencedInFlight > 0 {
				if _, err := drainPipeline(); err != nil {
					dataProposeFailed = true
					db.logger.Warn("ingest pipeline drain failed before checkpoint", zap.String("id", id), zap.Error(err))
					if db.metrics != nil && ctx.Err() == nil {
						db.metrics.IngestError(id, "propose")
					}
					if ctx.Err() == nil {
						if db.metrics != nil {
							db.metrics.IngestFrozen(id, true)
						}
						return ingestExitFreeze
					}
					continue
				}
			}
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
			if order.on {
				// Numbered and pipelined like any ordered proposal; any
				// failure freezes.
				entity, err := cluster.NewUpsertIngestablePositionEntity(&cluster.IngestablePosition{ID: id, Position: position})
				var f inflight
				if err == nil {
					p := &cluster.Proposal{Entities: []*cluster.Entity{entity}}
					stamp(p)
					f, err = submitPipelined(p, true, func() { db.clearIngestFrozen(id) })
				}
				if err != nil && proposeFailed(err, f, 0) {
					return ingestExitFreeze
				}
			} else if db.isNode(id) {
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
				} else {
					// A standalone checkpoint committed — the durable position
					// advanced, so the worker is making progress. Clear the frozen
					// gauge (see clearIngestFrozen); a poison proposal blocks this
					// path, so it never flaps against a freeze.
					db.clearIngestFrozen(id)
				}
			}
			backoff = ingestBackoffMin
		case <-time.After(backoff):
			progressed := false
			if len(pipeline) > 0 && time.Since(pipeline[0].at) > db.proposeTimeout {
				// The oldest in-flight proposal is unresolved past the
				// propose timeout (see awaitOldest's backstop).
				if proposeFailed(ErrProposalUnknown, popOldest(), 0) {
					return ingestExitFreeze
				}
			}
			if isNode && !db.isNode(id) {
				// leader -> not-leader - stop ingesting. This is the idle path:
				// the data cases above tear down promptly while the source is
				// active (when this timer is starved), so this fires only when
				// the source has gone quiet.
				loseOwnership()
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
				startSession()
				if order.on && db.raft != nil {
					// Read the resume state only once this node has applied
					// everything committed: a new leader may still be
					// applying its predecessor's last entries. A session
					// started behind them would number from a stale record
					// and be rejected at its first proposal.
					if err := db.waitForApplied(ctx, db.raft.CommitIndex()); err != nil {
						return ingestExitShutdown
					}
					startSession()
				}
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
