package db

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// This file holds the dead-letter side of the sync worker: classifying a
// failed Sync into permanent / transient / manual, recording the durable
// dead-letter entity through Raft, and reading those records back out. The
// worker state machines in sync.go call recordSync* on failure; everything
// here is observability layered on top of the skip decision the worker has
// already made, never control flow (see proposeSyncableDeadLetter).

// recordSyncDeadLetter emits the error metric for `kind` and durably
// dead-letters the skipped proposal so an operator can later query (and,
// once replay lands, re-drive) it. Best-effort on the dead-letter write:
// the proposal is skipped regardless, so a failed record propose (ctx
// canceled by replace/Close, or a leader change) is logged, not fatal.
// `kind` is "permanent" (Sync returned ErrPermanent) or "manual" (an
// operator skipped a syncable wedged on a transient error). ctx is the
// sync worker's context.
func (db *DB) recordSyncDeadLetter(ctx context.Context, id string, index uint64, kind string, syncErr error) {
	if db.metrics != nil {
		db.metrics.SyncError(id, kind)
	}
	msg := "operator dead-letter"
	if syncErr != nil {
		msg = syncErr.Error()
	}
	dl := &cluster.SyncableDeadLetter{
		ID:                id,
		Index:             index,
		TimestampUnixNano: time.Now().UnixNano(),
		Kind:              kind,
		Message:           truncateDeadLetterMessage(msg),
	}
	if err := db.proposeSyncableDeadLetter(ctx, dl); err != nil {
		db.logger.Warn("dead-letter record not persisted (best-effort; proposal still skipped)",
			zap.String("id", id), zap.Uint64("index", index), zap.String("kind", kind), zap.Error(err))
	}
}

// recordSyncPermanentError dead-letters a proposal that Sync rejected with a
// permanent error. Thin wrapper over recordSyncDeadLetter for the
// "permanent" kind.
func (db *DB) recordSyncPermanentError(ctx context.Context, id string, index uint64, syncErr error) {
	db.recordSyncDeadLetter(ctx, id, index, "permanent", syncErr)
}

// recordSyncTransientError emits the transient-error metric. Each
// occurrence counts, so a worker stuck retrying a transient failure
// shows a rising counter and a fresh last-error timestamp.
func (db *DB) recordSyncTransientError(id string) {
	if db.metrics != nil {
		db.metrics.SyncError(id, "transient")
	}
}

// maxDeadLetterMessageBytes bounds the error string stored in a
// dead-letter record. The record is replicated through Raft and lives in
// the permanent event log, so an unbounded driver error message (some
// SQL drivers echo the whole offending statement) would bloat every
// replica's log. 1 KiB keeps the diagnostic useful while capping growth.
const maxDeadLetterMessageBytes = 1024

// proposeSyncableDeadLetter records that a syncable permanently skipped
// the proposal at index i. It proposes a SyncableDeadLetter entity and
// BLOCKS until it durably applies, so the record is replicated to every
// node and queryable from any of them — not stranded on whichever node
// was leader at failure time.
//
// Unlike proposeSyncableIndex, a failure here does NOT change control
// flow: a permanent error skips the proposal regardless, and the dead
// letter is best-effort observability layered on top (the metric counter
// and the structured ERROR log already fired). A ctx cancellation
// (replace/Close) or ErrProposalUnknown (leader change) just means this
// one record didn't land; the caller logs and moves on rather than
// freezing the worker. ctx is the sync worker's context.
func (db *DB) proposeSyncableDeadLetter(ctx context.Context, d *cluster.SyncableDeadLetter) error {
	entity, err := cluster.NewUpsertSyncableDeadLetterEntity(d)
	if err != nil {
		return err
	}
	return db.Propose(ctx, &cluster.Proposal{Entities: []*cluster.Entity{entity}})
}

// truncateDeadLetterMessage clamps s to maxDeadLetterMessageBytes,
// appending an ellipsis marker when it had to cut. Operates on bytes
// (not runes) for a hard size bound; a multi-byte rune split at the
// boundary is acceptable for a diagnostic string.
func truncateDeadLetterMessage(s string) string {
	if len(s) <= maxDeadLetterMessageBytes {
		return s
	}
	return s[:maxDeadLetterMessageBytes] + "…(truncated)"
}

// SyncableDeadLetters returns the proposals a syncable permanently
// skipped, in ascending raft-index order. `since` is an exclusive
// raft-index cursor; `limit` bounds the page. Delegates to storage,
// where the records were written from the (replicated) apply path, so
// the answer is consistent on every node.
func (db *DB) SyncableDeadLetters(id string, since uint64, limit int) ([]cluster.SyncableDeadLetter, error) {
	return db.storage.SyncableDeadLetters(id, since, limit)
}
