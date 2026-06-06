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
		msg = truncateDeadLetterMessage(lastErr.Error())
	}
	s := &cluster.SyncableStuck{ID: t.id, Index: index, SinceUnixNano: t.since.UnixNano(), Message: msg}
	if err := t.db.proposeSyncableStuck(ctx, s); err != nil {
		t.db.logger.Warn("publish stuck status failed (will retry)",
			zap.String("id", t.id), zap.Uint64("index", index), zap.Error(err))
		return
	}
	t.published = true
	if t.db.metrics != nil {
		t.db.metrics.SetSyncStuck(t.id, true)
	}
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
		if t.db.metrics != nil {
			t.db.metrics.SetSyncStuck(t.id, false)
		}
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
