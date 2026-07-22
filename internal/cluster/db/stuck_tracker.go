package db

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// stuckTracker manages a worker's replicated "blocked on index N" status —
// the node-agnostic half of the manual dead-letter flow. It debounces the
// flag (publishing only after the worker has been wedged for
// db.syncStuckThreshold), publishes/clears the SyncableStuck record through
// Raft so every node can see the stall, drives the stuck gauge, and reads the
// operator's SyncableSkipRequest. One per worker goroutine; all of its Raft
// proposes are best-effort (a failure just defers visibility to the next
// retry, never the worker's control flow).
//
// The proposeSyncableStuck / proposeDeleteSyncableStuck /
// proposeDeleteSyncableSkipRequest helpers it calls live in
// manual_dead_letter.go alongside the operator-facing entry points that
// share them; only the worker-side debounce state machine lives here.
type stuckTracker struct {
	db        *DB
	id        string
	since     time.Time // when the worker first wedged on `index`; zero = not stuck
	index     uint64
	published bool
}

// newStuckTracker builds a worker's stuck tracker, ADOPTING any replicated
// SyncableStuck record already present for id (via resync). Without adoption a
// replacement worker (after a config re-POST — the natural fix for a rotated
// webhook token, now that auth failures are transient — or a restart) starts
// with published=false, so its first successful sync's cleared() would skip
// deleting the replicated record, leaving stuck:true latched forever on a
// healthy syncable (and the manual dead-letter endpoint acting on a stale
// record). Adoption keys the clear on the REPLICATED record, not this
// instance's memory (the companion determinism rule).
func (db *DB) newStuckTracker(id string) *stuckTracker {
	t := &stuckTracker{db: db, id: id}
	t.resync()
	return t
}

// resync re-derives the tracker's local debounce memory (published/index/since)
// from the applied SyncableStuck record — the companion-determinism rule made
// repeatable. It runs at construction AND on every leadership GAIN, because the
// sync worker goroutine OUTLIVES a raft leadership flap (it toggles isNode; it
// does not exit and rebuild the tracker). Across a flap the worker's in-memory
// published/index can go stale against the replicated record: another node that
// held leadership may have durably CLEARED the record (it made progress past the
// wedge), or published it at a DIFFERENT index. Trusting the stale memory let
// wedged() early-return on published and never re-publish a genuine re-wedge —
// stranding the syncable stuck-but-invisible (no record, gauge 0, and the
// operator's skip/dead-letter lever 409ing). Re-deriving from applied state on
// every gain fixes that: record present ⇒ adopt it (a re-wedge at the same index
// is then idempotent — no flap), absent ⇒ reset (a re-wedge re-publishes). A
// read error resets too, so a failed probe re-publishes rather than latching
// stale. The gauge is NOT toggled here — it is derived from the applied record
// on every node by handleSyncableStuck, so an adopting worker (and every
// follower) already reflects it; toggling it here is what latched it at 1 on
// followers.
func (t *stuckTracker) resync() {
	if rec, ok, err := t.db.storage.SyncableStuck(t.id); err == nil && ok {
		t.published = true
		t.index = rec.Index
		t.since = time.Unix(0, rec.SinceUnixNano)
		return
	}
	t.published = false
	t.index = 0
	t.since = time.Time{}
}

// wedged is called on every transient failure of the proposal at `index`. It
// (re)starts the debounce when the wedged proposal changes and publishes the
// replicated stuck record once the worker has been blocked past the threshold.
func (t *stuckTracker) wedged(ctx context.Context, index uint64, lastErr error) {
	if t.since.IsZero() || t.index != index {
		t.since = time.Now()
		t.index = index
		t.published = false
	}
	if t.published || time.Since(t.since) < t.db.syncStuckThreshold {
		return
	}
	msg := ""
	if lastErr != nil {
		// SyncableStuck is proposed through Raft (replicated, permanent) and
		// exposed over the status API, so redact like dead letters do: a
		// RedactedError (e.g. a transient execError) may echo entity PII in its
		// full text. Keep the full detail node-local, replicate the classifier.
		safe, redacted := safeDeadLetterMessage(lastErr)
		if redacted {
			t.db.logger.Warn("stuck syncable: full retry-error detail kept node-local, replicated status redacted",
				zap.String("id", t.id), zap.Uint64("index", index), zap.Error(lastErr))
		}
		msg = truncateDeadLetterMessage(safe)
	}
	s := &cluster.SyncableStuck{ID: t.id, Index: index, SinceUnixNano: t.since.UnixNano(), Message: msg}
	if err := t.db.proposeSyncableStuck(ctx, s); err != nil {
		t.db.logger.Warn("publish stuck status failed (will retry)",
			zap.String("id", t.id), zap.Uint64("index", index), zap.Error(err))
		return
	}
	t.published = true
	// Gauge derived from the applied record (handleSyncableStuck), not toggled
	// here — the proposeSyncableStuck above applies on every node and sets it.
}

// skipRequested reports whether an operator has asked the worker to skip the
// proposal at `index` (matching the stuck record it published). A request for
// a different index is stale (the worker moved on) and is dropped. Returns
// false until a stuck record has been published, since a request can only
// target a published index.
func (t *stuckTracker) skipRequested(ctx context.Context, index uint64) bool {
	if !t.published {
		return false
	}
	req, ok, err := t.db.storage.SyncableSkipRequest(t.id)
	if err != nil || !ok {
		return false
	}
	if req.Index != index {
		_ = t.db.proposeDeleteSyncableSkipRequest(ctx, t.id)
		return false
	}
	return true
}

// cleared is called when the worker makes progress, unsticks, or stops. It
// clears the published stuck record (no-op if nothing was published) and
// resets the debounce.
func (t *stuckTracker) cleared(ctx context.Context) {
	if t.published {
		if err := t.db.proposeDeleteSyncableStuck(ctx, t.id); err != nil {
			t.db.logger.Warn("clear stuck status failed", zap.String("id", t.id), zap.Error(err))
		}
		// Gauge derived from the applied record (handleSyncableStuck): the delete
		// above applies on every node and clears it. Not toggled here.
	}
	t.since = time.Time{}
	t.index = 0
	t.published = false
}

// honored is called after the worker skips a proposal in response to a skip
// request: it clears the request as well as the stuck record.
func (t *stuckTracker) honored(ctx context.Context) {
	_ = t.db.proposeDeleteSyncableSkipRequest(ctx, t.id)
	t.cleared(ctx)
}
