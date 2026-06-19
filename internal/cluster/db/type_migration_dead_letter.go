package db

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/migration"
)

// This file holds the type-keyed side of dead-lettering: when a sync skip
// was caused by a type-migration program failing at runtime, the failure is
// also recorded against the type (which program broke, on which proposal),
// counted in committed.type.migration.errors, and exposed for query and
// retry. Like the syncable records in sync_dead_letter.go, everything here
// is observability layered on top of the worker's skip decision — never
// control flow.

// recordTypeMigrationDeadLetter emits the migration-error metric and durably
// records the failure against the type. Called from recordSyncDeadLetter
// when the failed Sync unwraps to a *migration.Error. Best-effort like its
// syncable twin: a failed record propose is logged, never fatal. ctx is the
// sync worker's context.
func (db *DB) recordTypeMigrationDeadLetter(ctx context.Context, index uint64, merr *migration.Error) {
	if db.metrics != nil {
		db.metrics.MigrationError(merr.TypeID, merr.FromVersion, merr.ToVersion)
	}
	db.logger.Error("type migration failed at runtime",
		zap.String("type_id", merr.TypeID),
		zap.Int("from_version", merr.FromVersion),
		zap.Int("to_version", merr.ToVersion),
		zap.Uint64("index", index),
		zap.Error(merr.Err))

	dl := &cluster.TypeMigrationDeadLetter{
		TypeID:            merr.TypeID,
		Index:             index,
		TimestampUnixNano: time.Now().UnixNano(),
		FromVersion:       merr.FromVersion,
		ToVersion:         merr.ToVersion,
		Message:           truncateDeadLetterMessage(merr.Err.Error()),
	}
	entity, err := cluster.NewUpsertTypeMigrationDeadLetterEntity(dl)
	if err == nil {
		err = db.Propose(ctx, &cluster.Proposal{Entities: []*cluster.Entity{entity}})
	}
	if err != nil {
		db.logger.Warn("type-migration dead-letter record not persisted (best-effort; proposal still skipped)",
			zap.String("type_id", merr.TypeID), zap.Uint64("index", index), zap.Error(err))
	}
}

// TypeMigrationDeadLetters returns the proposals whose entities failed a
// type's migration program at runtime, in ascending raft-index order.
// `since` is an exclusive raft-index cursor; `limit` bounds the page.
// Delegates to storage, where the records were written from the
// (replicated) apply path, so the answer is consistent on every node.
func (db *DB) TypeMigrationDeadLetters(typeID string, since uint64, limit int) ([]cluster.TypeMigrationDeadLetter, error) {
	return db.storage.TypeMigrationDeadLetters(typeID, since, limit)
}

// ReplayTypeMigrationDeadLetter re-runs the (presumably fixed) migration
// chain for the dead-lettered proposal at `index` and, on success, clears
// the type-keyed record. It validates the fix against the exact payload
// that broke the old program — it does NOT deliver the result anywhere.
// Delivery is the syncable's job: the proposal is still dead-lettered for
// every syncable that skipped it, and POST /syncable/{id}/replay/{index}
// re-drives it through the same (now-working) chain to the downstream.
//
// Node-agnostic: the type history, the proposal (the permanent event log),
// and the dead-letter store are all replicated.
//
// Errors: cluster.ErrNotDeadLettered (the index isn't a migration dead
// letter for this type → 404); cluster.ErrReplayMigrationFailed (the chain
// still fails → 502, the record is left in place); anything else is
// internal (→ 500).
func (db *DB) ReplayTypeMigrationDeadLetter(ctx context.Context, typeID string, index uint64) error {
	has, err := db.storage.HasTypeMigrationDeadLetter(typeID, index)
	if err != nil {
		return err
	}
	if !has {
		return cluster.ErrNotDeadLettered
	}

	a, err := db.storage.ActualAt(index)
	if err != nil {
		return fmt.Errorf("read committed entry at index %d: %w", index, err)
	}

	// Re-run the chain for every entity of this type in the proposal. An
	// entity that has since been erased (RTBF delete) or a type whose
	// current version no longer needs migrating both fall through to
	// success — there is nothing left to fail, so the record clears.
	for _, e := range a.Entities {
		if e.ID != typeID || cluster.IsInternal(e.ID) || e.IsDelete() {
			continue
		}
		latest, err := db.storage.ResolveType(cluster.LatestTypeRef(typeID))
		if err != nil {
			return fmt.Errorf("resolve latest type %s: %w", typeID, err)
		}
		if _, chainErr := migration.Chain(db.storage, typeID, e.Version, latest.Version, e.Data); chainErr != nil {
			return fmt.Errorf("%w: %v", cluster.ErrReplayMigrationFailed, chainErr)
		}
	}

	// The chain succeeded — clear the record through Raft so it is gone on
	// every node.
	entity := cluster.NewDeleteTypeMigrationDeadLetterEntity(typeID, index)
	return db.Propose(ctx, &cluster.Proposal{Entities: []*cluster.Entity{entity}})
}
