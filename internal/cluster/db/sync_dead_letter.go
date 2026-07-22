package db

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/migration"
)

// This file holds the dead-letter side of the sync worker: classifying a
// failed Sync into permanent / transient / manual, recording the durable
// dead-letter entity through Raft, and reading those records back out. The
// worker state machines in sync.go call recordSync* on failure. The record is
// observability ABOUT a skip the worker has already decided — it never changes
// WHETHER a proposal is skipped — but its durability IS load-bearing on control
// flow: the worker must not advance its consumed head past a skip whose record
// has not landed (the EOF checkpoint advance and the restart-time
// HasSyncableDeadLetter re-exclusion both assume every skipped entry has a
// record), so a failed record propose holds position and re-records rather than
// advancing (see recordSyncDeadLetter and proposeSyncableDeadLetter).

// recordSyncDeadLetter emits the error metric for `kind` and durably
// dead-letters the skipped proposal so an operator can later query (and,
// once replay lands, re-drive) it. Best-effort on the dead-letter write:
// the proposal is skipped regardless, so a failed record propose (ctx
// canceled by replace/Close, or a leader change) is logged, not fatal.
// `kind` is "permanent" (Sync returned ErrPermanent) or "manual" (an
// operator skipped a syncable wedged on a transient error). ctx is the
// sync worker's context.
// safeDeadLetterMessage returns the message to persist into the permanent,
// replicated dead-letter record, and whether it was redacted. An error that
// implements cluster.RedactedError — a driver error that may echo a bound value
// (an entity key or data) — yields its PII-free RedactedMessage; the caller logs
// the full Error() node-locally instead. committed's own error text is already
// PII-safe and used verbatim.
func safeDeadLetterMessage(syncErr error) (string, bool) {
	if syncErr == nil {
		return "operator dead-letter", false
	}
	return cluster.RedactedMessage(syncErr)
}

// recordSyncDeadLetter durably records that the syncable skipped index, and
// reports whether the record LANDED. The return value is load-bearing: the
// worker's consumed head may only advance past a skipped entry once its
// dead-letter is durable — the EOF checkpoint advance and the restart-time
// HasSyncableDeadLetter re-exclusion both assume every skipped entry has a
// record. A false (the propose was orphaned by a leader flap, or ctx ended)
// means the caller must HOLD POSITION and re-run the decide+record on its next
// iteration, exactly like an orphaned checkpoint bump; advancing anyway would
// leave a skip with no durable record and no replayability.
func (db *DB) recordSyncDeadLetter(ctx context.Context, id string, index uint64, kind string, syncErr error) bool {
	if db.metrics != nil {
		db.metrics.SyncError(id, kind)
	}
	msg, redacted := safeDeadLetterMessage(syncErr)
	if redacted {
		// The full detail can carry PII a driver echoed from a bound value (the
		// deleted subject key, upserted row data); keep it in this node's logs and
		// replicate only the PII-free classifier into the permanent record.
		db.logger.Warn("sync dead-letter: full detail kept node-local, replicated record redacted",
			zap.String("id", id), zap.Uint64("index", index), zap.Error(syncErr))
	}
	dl := &cluster.SyncableDeadLetter{
		ID:                id,
		Index:             index,
		TimestampUnixNano: time.Now().UnixNano(),
		Kind:              kind,
		Message:           truncateDeadLetterMessage(msg),
	}
	if err := db.proposeSyncableDeadLetter(ctx, dl); err != nil {
		db.logger.Error("dead-letter record not persisted; the worker holds position and will re-record rather than advance past an unrecorded skip",
			zap.String("id", id), zap.Uint64("index", index), zap.String("kind", kind), zap.Error(err))
		return false
	}

	// A failure inside the type-migration chain is attributed to the type as
	// well: the syncable record above says "syncable S skipped index N", the
	// type-keyed twin says which type's migration program broke it, so an
	// operator can enumerate (and retry) failures per type. Best-effort — the
	// syncable-keyed record above is the durable source of truth for the skip.
	// See type_migration_dead_letter.go.
	if merr, ok := errors.AsType[*migration.Error](syncErr); ok {
		db.recordTypeMigrationDeadLetter(ctx, index, merr)
	}
	return true
}

// recordSyncPermanentError dead-letters a proposal that Sync rejected with a
// permanent error, reporting whether the record landed (see
// recordSyncDeadLetter). Thin wrapper for the "permanent" kind.
func (db *DB) recordSyncPermanentError(ctx context.Context, id string, index uint64, syncErr error) bool {
	return db.recordSyncDeadLetter(ctx, id, index, "permanent", syncErr)
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
// A failure here DOES change control flow (see recordSyncDeadLetter): the
// consumed head must never advance past a skip whose record is not durable, so
// an orphaned propose (ErrProposalUnknown after a leader flap, ctx ended) makes
// the worker hold position and re-run the decide+record — exactly the posture
// an orphaned checkpoint bump already takes. ctx is the sync worker's context.
func (db *DB) proposeSyncableDeadLetter(ctx context.Context, d *cluster.SyncableDeadLetter) error {
	if db.deadLetterProposeHookForTest != nil {
		if err := db.deadLetterProposeHookForTest(d); err != nil {
			return err
		}
	}
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
