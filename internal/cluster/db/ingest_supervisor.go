package db

import (
	"bytes"
	"math/rand/v2"
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// The ingest supervisor auto-restarts an ingest worker that parks in the
// ErrProposalUnknown freeze branch (see ingest.go's worker loop). It lives
// here, next to the worker it supervises, rather than in db.go: the
// freeze→restart lifecycle is one cohesive subsystem. The supervisor's *state*
// (the per-id map and tuning knobs) is on the DB struct in db.go because Go
// keeps a struct's fields in one place; only the behaviour lives here.

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
//     count exceeds ingestSupervisorMaxAttempts. The worker stays
//     parked; operator intervention is required.
//   - Otherwise waits a jittered backoff (exponential, capped at
//     ingestSupervisorMaxBackoff) before re-registering.
//   - Preflight AND install under a single workersMu hold so a
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
	backoff, consecutive, giveup := db.recordFreezeAndNextBackoff(id, pos)
	if giveup {
		db.logger.Error("ingest supervisor giving up after repeated freezes at the same resume position — the worker is wedged on a proposal it cannot commit (most often a single row or transaction over COMMITTED_MAX_PROPOSAL_BYTES; see the freeze warnings above for its SourceSeq/coordinate). It stays parked until an operator intervenes: raise the cap and restart, or fix the source",
			zap.String("id", id),
			zap.Int("consecutive_freezes", consecutive),
			zap.Binary("stuck_position", pos))
		if db.metrics != nil {
			db.metrics.IngestSupervisorGiveup(id)
		}
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

	db.workersMu.Lock()
	if db.closed {
		db.workersMu.Unlock()
		return
	}
	if db.ingestWorkers[id] != frozen || frozen.condemned {
		// Either a user-initiated replace already installed a fresh handle while
		// we were waiting (!= frozen), or a delete/reconcile has condemned this
		// handle and is mid-teardown — it set condemned under workersMu before
		// dropping the lock to drain, and we reacquired the lock inside that
		// window (the map entry is deleted only after its relock). Resurrecting a
		// condemned handle would build a fresh worker on the same Ingestable
		// instance the teardown is about to Close. Bail in both cases; a user
		// replace that arrives later still wins via db.Ingest's own loop.
		db.workersMu.Unlock()
		db.logger.Debug("ingest supervisor skipping restart; handle replaced or condemned",
			zap.String("id", id))
		return
	}
	// Drop the frozen entry directly. Its goroutine exited already
	// (we're downstream of that exit) and its handle.done is closed,
	// so no drain step is needed — unlike db.Ingest's public replace
	// loop, which must assume the existing worker is still running.
	delete(db.ingestWorkers, id)
	db.spawnIngestWorkerLocked(id, i)
	db.workersMu.Unlock()

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

// pruneIngestSupervisorState drops the give-up bookkeeping for id. It is called
// whenever the worker is torn down for an operator recovery — a re-POST (via
// db.Ingest's replace loop) or a delete (via cancelIngestWorker) — because the
// restart budget's lifetime is tied to the worker GENERATION: a fresh worker
// (any re-POST, even a byte-identical one that does not bump the config version)
// must start with a full budget, not inherit a prior give-up. It also bounds the
// state map (a deleted/recreated id can't accumulate). Idempotent.
func (db *DB) pruneIngestSupervisorState(id string) {
	db.ingestSupervisorMu.Lock()
	delete(db.ingestSupervisorStates, id)
	db.ingestSupervisorMu.Unlock()
}

// recordFreezeAndNextBackoff bumps the consecutive-freeze counter for id and
// returns the backoff to apply before the next restart. pos is the durable
// resume position at freeze time (db.storage.Position(id)): a freeze at the
// SAME position as the previous is the same poison proposal re-read and grows
// the run; a freeze at a DIFFERENT (advanced) position means the worker made
// real progress and resets the run to a fresh episode. Returns giveup = true
// when the (post-increment) counter exceeds ingestSupervisorMaxAttempts;
// callers surface the giveup metric and skip the restart.
func (db *DB) recordFreezeAndNextBackoff(id string, pos cluster.Position) (backoff time.Duration, consecutive int, giveup bool) {
	db.ingestSupervisorMu.Lock()
	defer db.ingestSupervisorMu.Unlock()

	st, ok := db.ingestSupervisorStates[id]
	if !ok {
		st = &ingestSupervisorState{backoff: db.ingestSupervisorInitialBackoff}
		db.ingestSupervisorStates[id] = st
	}
	// Reset the run only on genuine progress — an advanced resume position. A
	// slow re-read to the SAME poison position is NOT progress (resetting on
	// wall-clock let the run churn forever). An operator recovery (re-POST or
	// delete) resets the run a different way: it tears down the worker, and the
	// budget's lifetime is tied to the worker generation — the teardown paths
	// prune this state, so the fresh worker starts clean. See
	// pruneIngestSupervisorState (called from db.Ingest's replace loop and
	// cancelIngestWorker).
	if st.consecutiveFreezes > 0 && !bytes.Equal(pos, st.lastFreezePosition) {
		st.consecutiveFreezes = 0
		st.backoff = db.ingestSupervisorInitialBackoff
	}
	st.consecutiveFreezes++
	st.lastFreezePosition = pos

	if st.consecutiveFreezes > db.ingestSupervisorMaxAttempts {
		return 0, st.consecutiveFreezes, true
	}

	backoff = st.backoff
	st.backoff *= 2
	if st.backoff > db.ingestSupervisorMaxBackoff {
		st.backoff = db.ingestSupervisorMaxBackoff
	}
	return backoff, st.consecutiveFreezes, false
}
