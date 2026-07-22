package db

import (
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
	defaultIngestSupervisorHealthyWindow  = 60 * time.Second
)

// ingestSupervisorState tracks consecutive freeze-restart cycles for a
// single ingestable id. A freeze observed within
// ingestSupervisorHealthyWindow of the previous one counts as
// consecutive and grows the backoff; a longer gap means the restarted
// worker ran healthy long enough to reset the counter. giveup is set
// once the supervisor has declared the id unrecoverable so subsequent
// freezes (e.g., if an operator replaces the config later) don't
// silently carry forward old state forever.
type ingestSupervisorState struct {
	lastFreezeAt       time.Time
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
//   - Records the freeze in the per-id state map; resets the
//     consecutive counter if the gap since the last freeze exceeds
//     ingestSupervisorHealthyWindow (the worker ran cleanly long enough
//     that this flap is "new", not a continuation).
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
//   - On successful install, clears the frozen gauge and bumps the
//     IngestRestart counter.
func (db *DB) superviseRestartIngest(id string, i cluster.Ingestable, frozen *workerHandle) {
	if db.afterIngestSupervisorAttemptForTest != nil {
		defer db.afterIngestSupervisorAttemptForTest()
	}
	backoff, consecutive, giveup := db.recordFreezeAndNextBackoff(id)
	if giveup {
		db.logger.Error("ingest supervisor giving up after repeated freezes",
			zap.String("id", id),
			zap.Int("consecutive_freezes", consecutive))
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

	if db.metrics != nil {
		db.metrics.SetWorkerRunning("ingest", id, true)
		db.metrics.IngestFrozen(id, false)
		db.metrics.IngestRestart(id)
	}
}

// recordFreezeAndNextBackoff bumps the consecutive-freeze counter for
// id and returns the backoff to apply before the next restart. If the
// gap since the previous freeze exceeds ingestSupervisorHealthyWindow
// the counter is reset first — the worker ran cleanly long enough that
// this flap is a fresh episode, not a continuation. Returns giveup =
// true when the (post-increment) counter exceeds
// ingestSupervisorMaxAttempts; callers surface the giveup metric and
// skip the restart.
func (db *DB) recordFreezeAndNextBackoff(id string) (backoff time.Duration, consecutive int, giveup bool) {
	db.ingestSupervisorMu.Lock()
	defer db.ingestSupervisorMu.Unlock()

	st, ok := db.ingestSupervisorStates[id]
	if !ok {
		st = &ingestSupervisorState{backoff: db.ingestSupervisorInitialBackoff}
		db.ingestSupervisorStates[id] = st
	}
	now := time.Now()
	if !st.lastFreezeAt.IsZero() && now.Sub(st.lastFreezeAt) > db.ingestSupervisorHealthyWindow {
		st.consecutiveFreezes = 0
		st.backoff = db.ingestSupervisorInitialBackoff
	}
	st.consecutiveFreezes++
	st.lastFreezeAt = now

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
