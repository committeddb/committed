package db

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// The ingest supervisor auto-restarts an ingest worker that parks in the
// ErrProposalUnknown freeze branch (see ingest.go's worker loop). Its state
// — the per-id give-up state machine and the node-local frozen/recovering
// flag set — lives on the ingestSupervisor struct below, a pure bookkeeping
// subsystem with no engine views. The restart ORCHESTRATION
// (superviseRestartIngest) stays a DB method: it coordinates the worker
// registry, storage, and propose paths that this state machine advises.

// ingestSupervisor* constants govern the auto-restart behavior applied
// when an ingest worker parks in the ErrProposalUnknown freeze branch.
// Options (WithIngestSupervisor*) let callers override; zero values in
// options resolve to these defaults. See the ingest-worker-supervisor
// ticket for the motivation — in short, a cluster that flaps under
// load would otherwise leave one or more ingestables offline after
// each flap until an operator intervened.
const (
	defaultIngestSupervisorInitialBackoff = 100 * time.Millisecond
	defaultIngestSupervisorMaxBackoff     = 30 * time.Second
	defaultIngestSupervisorMaxAttempts    = 20
)

// ingestSupervisorState tracks consecutive freeze-restart cycles for a
// single ingestable id. Consecutiveness is keyed on the durable resume
// POSITION at freeze time, not wall-clock: a freeze at the same position is
// the same poison proposal re-read (a >cap row, a persistently-orphaned
// transaction) and counts consecutive no matter how long the re-read took;
// a freeze at an advanced position means the worker made real progress and
// resets the run. This is the sync breaker's distinct-entry keying inverted
// (there, distinct entries are the systematic signal; here, the SAME position
// is). Keying on wall-clock instead let a poison row whose restart cycle
// exceeded the healthy window reset every time and churn forever without ever
// reaching give-up.
type ingestSupervisorState struct {
	lastFreezePosition cluster.Position
	consecutiveFreezes int
	backoff            time.Duration
}

// ingestSupervisor is the freeze/restart bookkeeping subsystem: the per-id
// give-up state machine (consecutive-freeze runs keyed on resume position,
// exponential backoff, the give-up cap) and the node-local
// frozen/recovering flag set the status endpoint reports. Pure state — no
// engine views, no I/O — so it is unit-testable directly.
type ingestSupervisor struct {
	// mu guards states. Deliberately separate from workers.mu so the
	// supervisor's bookkeeping doesn't contend with the hot
	// worker-registry path.
	mu     sync.Mutex
	states map[string]*ingestSupervisorState

	initialBackoff time.Duration
	maxBackoff     time.Duration
	maxAttempts    int

	// frozenMu guards frozen, the node-local set of ingestables whose
	// worker is currently FROZEN and being restarted (the "recovering"
	// state). It drives the workerState the status endpoint reports for a
	// live worker on this node, so it must be tracked independently of the
	// committed.ingest.frozen gauge (which is a no-op when no metrics
	// endpoint is wired — the common beta case). Set at freeze-exit,
	// cleared on durable progress or worker teardown.
	frozenMu sync.Mutex
	frozen   map[string]bool
}

// newIngestSupervisor builds the subsystem with production defaults applied
// to any zero-valued tuning knob.
func newIngestSupervisor(initialBackoff, maxBackoff time.Duration, maxAttempts int) *ingestSupervisor {
	if initialBackoff == 0 {
		initialBackoff = defaultIngestSupervisorInitialBackoff
	}
	if maxBackoff == 0 {
		maxBackoff = defaultIngestSupervisorMaxBackoff
	}
	if maxAttempts == 0 {
		maxAttempts = defaultIngestSupervisorMaxAttempts
	}
	return &ingestSupervisor{
		states:         make(map[string]*ingestSupervisorState),
		frozen:         make(map[string]bool),
		initialBackoff: initialBackoff,
		maxBackoff:     maxBackoff,
		maxAttempts:    maxAttempts,
	}
}

// superviseRestartIngest re-registers an ingestable whose worker
// parked in the ErrProposalUnknown freeze branch. It runs as a
// detached goroutine spawned from the freeze-exit branch of the
// worker-launch goroutine in spawnIngestWorkerLocked.
//
// Behavior:
//
//   - Records the freeze in the per-id state map, keyed on the durable
//     resume position; resets the consecutive counter only when that
//     position has advanced since the last freeze (real progress — this
//     flap is "new", not the same poison proposal re-read).
//   - Gives up + emits IngestSupervisorGiveup once the consecutive
//     count exceeds the supervisor's maxAttempts. The worker stays
//     parked; operator intervention is required.
//   - Otherwise waits a jittered backoff (exponential, capped at the
//     supervisor's maxBackoff) before re-registering.
//   - Preflight AND install under a single workers.mu hold so a
//     concurrent user replace can't slip in between (see the race
//     analysis in spawnIngestWorkerLocked's doc comment): if the
//     frozen handle is no longer the registered one when we acquire
//     the lock, we bail; otherwise we delete it and install the
//     supervisor's replacement without releasing the lock. A user
//     replace that arrives after the unlock still wins the final
//     state via db.Ingest's own replacement loop.
//   - On successful install, bumps the IngestRestart counter. It does
//     NOT clear the frozen gauge — a restart is not recovery; the worker
//     clears it only once it makes real progress past the freeze position.
func (db *DB) superviseRestartIngest(id string, i cluster.Ingestable, frozen *workerHandle) {
	if db.afterIngestSupervisorAttemptForTest != nil {
		defer db.afterIngestSupervisorAttemptForTest()
	}
	// The durable resume position at freeze time keys consecutiveness and locates
	// the wedge for an operator. The frozen worker never advanced past it, so it
	// is the same across re-reads of a poison proposal.
	pos := db.storage.Position(id)
	backoff, consecutive, giveup := db.ingestSupervisor.recordFreezeAndNextBackoff(id, pos)
	if giveup {
		db.logger.Error("ingest supervisor giving up after repeated freezes at the same resume position — the worker is wedged on a proposal it cannot commit (most often a single row or transaction over COMMITTED_MAX_PROPOSAL_BYTES; see the freeze warnings above for its SourceSeq/coordinate). It stays parked until an operator intervenes: raise the cap and restart, or fix the source",
			zap.String("id", id),
			zap.Int("consecutive_freezes", consecutive),
			zap.Binary("stuck_position", pos))
		if db.metrics != nil {
			db.metrics.IngestSupervisorGiveup(id)
		}
		// Publish the replicated terminal parked record so the give-up is visible
		// from any node (status + the sustained worker.parked gauge), not just a log
		// line on the owner. db.ctx (not the frozen worker's ctx, cancelled below).
		db.publishIngestableParked(db.ctx, id, consecutive)
		// Cancel the frozen worker's context, same as the restart path below. The
		// goroutine exited via ingestExitFreeze (a normal return, NOT a ctx cancel),
		// so workerCtx is still an un-cancelled child of the long-lived db.ctx; on
		// this terminal branch it is never restarted, so without this the context
		// node leaks until db.Close (the un-fixed sibling of the restart-path leak).
		// The handle stays registered so an operator re-POST/delete still finds and
		// fully tears it down; cancelling an already-exited worker is a harmless
		// no-op that just releases the node.
		frozen.cancel()
		return
	}

	db.logger.Info("ingest supervisor scheduled restart",
		zap.String("id", id),
		zap.Int("consecutive_freezes", consecutive),
		zap.Duration("backoff", backoff))

	// Jitter is drawn from [0, backoff/2]. Keeps concurrent freezes
	// across multiple ids from all trying to restart in lockstep.
	// math/rand/v2 is deliberate — this is scheduling jitter, not a
	// security primitive, and crypto/rand would add failure modes
	// (syscall error handling) for no benefit.
	jitter := time.Duration(0)
	if backoff/2 > 0 {
		jitter = time.Duration(rand.Int64N(int64(backoff / 2))) //nolint:gosec // G404: non-security-sensitive jitter
	}
	select {
	case <-time.After(backoff + jitter):
	case <-db.ctx.Done():
		return
	}

	if db.beforeIngestSupervisorRelockForTest != nil {
		db.beforeIngestSupervisorRelockForTest()
	}

	db.workers.mu.Lock()
	if db.workers.closed {
		db.workers.mu.Unlock()
		return
	}
	if db.workers.ingest[id] != frozen || frozen.condemned {
		// Either a user-initiated replace already installed a fresh handle while
		// we were waiting (!= frozen), or a delete/reconcile has condemned this
		// handle and is mid-teardown — it set condemned under workers.mu before
		// dropping the lock to drain, and we reacquired the lock inside that
		// window (the map entry is deleted only after its relock). Resurrecting a
		// condemned handle would build a fresh worker on the same Ingestable
		// instance the teardown is about to Close. Bail in both cases; a user
		// replace that arrives later still wins via db.Ingest's own loop.
		db.workers.mu.Unlock()
		db.logger.Debug("ingest supervisor skipping restart; handle replaced or condemned",
			zap.String("id", id))
		return
	}
	// Drop the frozen entry directly. Its goroutine exited already
	// (we're downstream of that exit) and its handle.done is closed,
	// so no drain step is needed — unlike db.Ingest's public replace
	// loop, which must assume the existing worker is still running.
	//
	// Cancel the frozen worker's context before dropping the handle. The
	// goroutine returned via ingestExitFreeze — a normal return, NOT a ctx
	// cancel — so workerCtx is still an un-cancelled child of db.ctx; without
	// this, each restart leaks one context node on the long-lived db.ctx (and
	// pins the handle) until db.Close. Cancelling an already-exited worker is a
	// harmless no-op that just releases the node.
	frozen.cancel()
	delete(db.workers.ingest, id)
	db.spawnIngestWorkerLocked(id, i)
	db.workers.mu.Unlock()

	if db.afterIngestSupervisorRestartForTest != nil {
		db.afterIngestSupervisorRestartForTest(frozen.ctx.Err())
	}

	// Do NOT clear the frozen gauge here. A restart is not recovery — the worker
	// re-reads to the same poison proposal and freezes again, so clearing on
	// restart made the gauge flap 1→0→1 and defeated any sustained-1 alert. The
	// worker clears it only once it makes real progress past the freeze position
	// (see db.ingest's position-advance clear). SetWorkerRunning is fine — the
	// goroutine really is running again.
	if db.metrics != nil {
		db.metrics.SetWorkerRunning("ingest", id, true)
		db.metrics.IngestRestart(id)
	}
}

// prune drops the give-up bookkeeping for id. It is called whenever the
// worker is torn down for an operator recovery — a re-POST (via db.Ingest's
// replace loop) or a delete (via cancelIngestWorker) — because the restart
// budget's lifetime is tied to the worker GENERATION: a fresh worker (any
// re-POST, even a byte-identical one that does not bump the config version)
// must start with a full budget, not inherit a prior give-up. It also bounds
// the state map (a deleted/recreated id can't accumulate). Idempotent.
func (s *ingestSupervisor) prune(id string) {
	s.mu.Lock()
	delete(s.states, id)
	s.mu.Unlock()
	// A worker teardown (re-POST or delete) ends any recovering state, so a fresh
	// worker starts reported as running rather than inheriting a stale frozen flag.
	s.setFrozen(id, false)
}

// setFrozen sets or clears id's node-local frozen flag — the "recovering"
// state the status endpoint reports for a live worker on this node.
// Node-local by design: recovering is a LIVE state tied to the owner running
// the worker (a follower has no worker to recover), so it rides ingest's
// node-local live-status model rather than the replicated terminal parked
// record.
func (s *ingestSupervisor) setFrozen(id string, frozen bool) {
	s.frozenMu.Lock()
	defer s.frozenMu.Unlock()
	if frozen {
		s.frozen[id] = true
		return
	}
	delete(s.frozen, id)
}

// isFrozen reports whether id's worker is currently frozen/recovering on
// this node (the supervisor is restarting it).
func (s *ingestSupervisor) isFrozen(id string) bool {
	s.frozenMu.Lock()
	defer s.frozenMu.Unlock()
	return s.frozen[id]
}

// recordFreezeAndNextBackoff bumps the consecutive-freeze counter for id and
// returns the backoff to apply before the next restart. pos is the durable
// resume position at freeze time (db.storage.Position(id)): a freeze at the
// SAME position as the previous is the same poison proposal re-read and grows
// the run; a freeze at a DIFFERENT (advanced) position means the worker made
// real progress and resets the run to a fresh episode. Returns giveup = true
// when the (post-increment) counter exceeds maxAttempts; callers surface the
// giveup metric and skip the restart.
func (s *ingestSupervisor) recordFreezeAndNextBackoff(id string, pos cluster.Position) (backoff time.Duration, consecutive int, giveup bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	st, ok := s.states[id]
	if !ok {
		st = &ingestSupervisorState{backoff: s.initialBackoff}
		s.states[id] = st
	}
	// Reset the run only on genuine progress — an advanced resume position. A
	// slow re-read to the SAME poison position is NOT progress (resetting on
	// wall-clock let the run churn forever). An operator recovery (re-POST or
	// delete) resets the run a different way: it tears down the worker, and the
	// budget's lifetime is tied to the worker generation — the teardown paths
	// prune this state, so the fresh worker starts clean. See prune (called
	// from db.Ingest's replace loop and cancelIngestWorker).
	if st.consecutiveFreezes > 0 && !bytes.Equal(pos, st.lastFreezePosition) {
		st.consecutiveFreezes = 0
		st.backoff = s.initialBackoff
	}
	st.consecutiveFreezes++
	st.lastFreezePosition = pos

	if st.consecutiveFreezes > s.maxAttempts {
		return 0, st.consecutiveFreezes, true
	}

	backoff = st.backoff
	st.backoff *= 2
	if st.backoff > s.maxBackoff {
		st.backoff = s.maxBackoff
	}
	return backoff, st.consecutiveFreezes, false
}

// publishIngestableParked writes the replicated, TERMINAL IngestableStuck record
// for an ingestable whose freeze/restart supervisor gave up, so the parked state is
// queryable from any node (GET /ingestable/{id}/status) and drives the sustained
// committed.worker.parked{kind:ingest} gauge on every node. The record outlives the
// worker and clears only on an operator fix (a new config version) or a delete. The
// give-up carries no user error (the freeze cause, with its SourceSeq, was logged at
// each freeze), so the replicated message is a generic, PII-free remedy hint.
func (db *DB) publishIngestableParked(ctx context.Context, id string, consecutiveFreezes int) {
	msg := fmt.Sprintf("freeze/restart supervisor gave up after %d consecutive freezes at the same resume position — the worker is wedged on a proposal it cannot commit (most often a row or transaction over COMMITTED_MAX_PROPOSAL_BYTES); raise the cap and re-POST the config, or fix the source", consecutiveFreezes)
	s := &cluster.IngestableStuck{ID: id, SinceUnixNano: time.Now().UnixNano(), Message: msg}
	if err := db.proposeIngestableStuck(ctx, s); err != nil {
		db.logger.Warn("publish ingestable parked status failed (worker stays parked regardless; status not visible until republished)",
			zap.String("id", id), zap.Error(err))
	}
}

// proposeIngestableStuck publishes an ingestable's terminal parked record through
// Raft so every node applies it.
func (db *DB) proposeIngestableStuck(ctx context.Context, s *cluster.IngestableStuck) error {
	e, err := cluster.NewUpsertIngestableStuckEntity(s)
	if err != nil {
		return err
	}
	return db.Propose(ctx, &cluster.Proposal{Entities: []*cluster.Entity{e}})
}
