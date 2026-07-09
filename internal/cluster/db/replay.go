package db

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/migration"
)

// ReplaySyncableDeadLetter re-drives a single dead-lettered proposal: it
// rebuilds the syncable from its current config, re-runs Sync once for the
// proposal at `index`, and on success clears the dead-letter record. Use it
// after fixing the downstream that caused the original skip.
//
// Node-agnostic: config, the proposal (the permanent event log), and the
// dead-letter store are all replicated, so any node can rebuild + re-sync +
// clear. Sync is idempotent (the SQL dialect upserts), so a replay racing the
// live worker is safe at the sink — and a dead-lettered proposal is one the
// worker has already skipped, so it isn't being processed concurrently.
//
// Errors: cluster.ErrNotDeadLettered (the index isn't a dead letter → 404);
// cluster.ErrReplaySyncFailed (re-Sync failed again → 502, the dead letter is
// left in place); anything else is internal (→ 500).
func (db *DB) ReplaySyncableDeadLetter(ctx context.Context, id string, index uint64) error {
	has, err := db.storage.HasSyncableDeadLetter(id, index)
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

	syncable, err := db.buildSyncable(id)
	if err != nil {
		return err
	}
	defer func() { _ = syncable.Close() }()

	if _, syncErr := syncable.Sync(ctx, a); syncErr != nil {
		// Keep the full detail node-local (it may echo entity PII via a driver
		// error), and chain syncErr with %w — not %v — so the HTTP layer can reach
		// a cluster.RedactedError in the chain and surface only its classifier.
		db.logger.Warn("syncable replay re-failed",
			zap.String("id", id), zap.Uint64("index", index), zap.Error(syncErr))
		return fmt.Errorf("%w: %w", cluster.ErrReplaySyncFailed, syncErr)
	}

	// Re-sync succeeded — clear the dead letter through Raft so the record is
	// gone on every node and the proposal is no longer excluded by the worker.
	return db.proposeDeleteSyncableDeadLetter(ctx, id, index)
}

// buildSyncable constructs a fresh syncable instance from its current
// persisted config, applying the always-current migration wrapper exactly as
// the worker-registration path does (see wal.saveSyncable). The instance owns
// its own downstream connection; callers must Close it.
func (db *DB) buildSyncable(id string) (cluster.Syncable, error) {
	cfgs, err := db.storage.Syncables()
	if err != nil {
		return nil, err
	}
	var cfg *cluster.Configuration
	for _, c := range cfgs {
		if c.ID == id {
			cfg = c
			break
		}
	}
	if cfg == nil {
		return nil, fmt.Errorf("syncable %q has no config to rebuild from", id)
	}

	_, syncable, mode, err := db.parser.ParseSyncable(cfg.MimeType, cfg.Data, db.storage)
	if err != nil {
		return nil, err
	}
	if mode == cluster.ModeAlwaysCurrent {
		syncable = migration.Wrap(syncable, db.storage, db.metrics)
	}
	return syncable, nil
}

// proposeDeleteSyncableDeadLetter clears the dead-letter record at index for a
// syncable through Raft (applied deterministically on every node).
func (db *DB) proposeDeleteSyncableDeadLetter(ctx context.Context, id string, index uint64) error {
	e := cluster.NewDeleteSyncableDeadLetterEntity(id, index)
	return db.Propose(ctx, &cluster.Proposal{Entities: []*cluster.Entity{e}})
}
