package db

import (
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// This file holds the machinery that resolves a blocking Propose's waiter —
// the two ways an in-flight proposal's outcome becomes known:
//
//   - notifyApplied: the raft Ready loop calls it after each ApplyCommitted,
//     signalling the waiter that its proposal landed.
//   - watchLeaderTransitions + signalAfterGrace: on a leader change, signal
//     at-risk waiters with ErrProposalUnknown after a grace period, so a
//     Propose whose old leader may have orphaned it fails fast instead of
//     hanging until ctx.
//
// The waiter type and the db.waiters / waitersMu / leaderChangeGrace state
// live on the DB struct (db.go) alongside Propose, which creates the waiters
// these functions resolve; only the resolution behaviour lives here.

// watchLeaderTransitions runs for the lifetime of the DB. On every
// LeaderTransition emitted by LeaderState, it snapshots the current
// waiter map, identifies every waiter stamped under a leader that
// isn't the new leader, and schedules a grace-delayed sweep that
// signals those waiters with ErrProposalUnknown unless they've been
// cleaned up by the apply path in the interim.
//
// The grace period is critical to reducing false positives: a leader
// hand-off where the new leader inherits and commits the entry fast
// will see notifyApplied fire before the grace timer, which deletes
// the waiter out from under the sweep. Only waiters that are still
// registered when the timer fires get ErrProposalUnknown.
//
// The watcher exits on db.ctx.Done (Close path) — no further sweeps
// are scheduled. Any in-flight sweep goroutine also observes db.ctx
// and exits without signaling (Propose returns db.ctx.Err on its own
// select branch during shutdown).
func (db *DB) watchLeaderTransitions(transitions <-chan LeaderTransition) {
	for {
		select {
		case t, ok := <-transitions:
			if !ok {
				return
			}
			db.logger.Debug("observed leader transition",
				zap.Uint64("old", t.Old),
				zap.Uint64("new", t.New))
			if db.metrics != nil {
				db.metrics.LeaderTransitionObserved()
			}

			// A transition whose old value is 0 is the initial
			// establishment of a leader (pre-election → elected),
			// not a leader change. Waiters stamped with 0 submitted
			// before a leader was known; raft internally held them
			// pending election and submits them to the new leader
			// when it emerges — there is no prior leader whose
			// acceptance got orphaned, so marking them at-risk here
			// would be a spurious false positive that fires every
			// cold start. Any subsequent flap (X→Y where X > 0)
			// still marks stamp=0 waiters at risk via the normal
			// leaderID != t.New check below.
			if t.Old == 0 {
				continue
			}

			// Snapshot waiters-at-risk under the lock so a Propose
			// currently mid-registration can't be observed in a
			// half-installed state. The snapshot captures pointers;
			// the grace-timer goroutine compares by pointer identity
			// at fire time to detect races where the waiter's slot
			// was reused for a new request with the same RequestID
			// (not currently possible — RequestID is monotonic via
			// atomic — but the check is cheap and defensive).
			db.waitersMu.Lock()
			atRisk := make(map[uint64]*waiter, len(db.waiters))
			for id, w := range db.waiters {
				if w.leaderID != t.New {
					atRisk[id] = w
				}
			}
			db.waitersMu.Unlock()

			if len(atRisk) == 0 {
				continue
			}
			go db.signalAfterGrace(atRisk)
		case <-db.ctx.Done():
			return
		}
	}
}

// signalAfterGrace waits leaderChangeGrace and then signals every
// still-registered waiter in atRisk with ErrProposalUnknown. A waiter
// that's been removed from the registry (Propose's defer ran after
// notifyApplied signaled it) is skipped naturally.
//
// The send to w.ack is non-blocking: notifyApplied may have won the
// race and filled the buffered chan with nil, in which case we hit
// the default branch and drop the ErrProposalUnknown so the caller
// sees "applied" instead of "unknown" (the preferred outcome when
// both signals race).
func (db *DB) signalAfterGrace(atRisk map[uint64]*waiter) {
	timer := time.NewTimer(db.leaderChangeGrace)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-db.ctx.Done():
		return
	}

	db.waitersMu.Lock()
	defer db.waitersMu.Unlock()
	for id, w := range atRisk {
		cur, ok := db.waiters[id]
		if !ok || cur != w {
			continue
		}
		select {
		case w.ack <- ErrProposalUnknown:
			db.logger.Debug("propose fail-fast",
				zap.Uint64("requestID", id),
				zap.Uint64("stampedLeader", w.leaderID))
			if db.metrics != nil {
				db.metrics.ProposeFailFastUnknown()
			}
		default:
		}
	}
}

// notifyApplied is invoked from the raft Ready loop after each successful
// ApplyCommitted. It unmarshals the entry, looks up any blocking
// db.Propose call by RequestID, and signals the waiter. Entries with
// RequestID == 0 are system-internal proposals (or pre-PR2 entries) that
// have no waiter; for those this is a no-op.
func (db *DB) notifyApplied(data []byte) {
	if data == nil {
		return
	}
	p := &cluster.Proposal{}
	if err := p.Unmarshal(data, db.storage); err != nil {
		// Defense-in-depth: ApplyCommitted also unmarshals the entry
		// and fatal-exits on failure, so reaching here with an
		// unmarshal error means the apply path succeeded on one
		// decode and this one failed — pathological, not expected.
		// Log and skip; Propose's caller will time out via ctx if
		// its waiter is never signaled.
		db.logger.Warn("notifyApplied unmarshal failed", zap.Error(err))
		return
	}
	if p.RequestID == 0 {
		return
	}
	db.waitersMu.Lock()
	w, ok := db.waiters[p.RequestID]
	db.waitersMu.Unlock()
	if !ok {
		return
	}
	// Buffered channel of capacity 1 set up by Propose, so this never
	// blocks. We don't delete the waiter here — Propose's defer cleans
	// it up after the receive. If the leader-change watcher has already
	// queued an ErrProposalUnknown on this slot, the default branch
	// fires and we leave the watcher's signal in place: a caller
	// treating ErrProposalUnknown as "unknown outcome, must confirm"
	// will just re-read and find the entry applied, which is safe.
	select {
	case w.ack <- nil:
	default:
	}
}
