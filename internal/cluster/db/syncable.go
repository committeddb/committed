package db

import (
	"context"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

type SyncableWithID struct {
	ID       string
	Syncable cluster.Syncable
	// Delete signals that the syncable with ID was removed from the log
	// (deleteSyncable on the apply path), rather than upserted. The DB-layer
	// consumer (listenForSyncables) cancels the worker and, on the owner, tears
	// down the syncable's destination. Syncable is nil for a delete.
	Delete bool
}

func (db *DB) AddSyncableParser(name string, p cluster.SyncableParser) {
	db.parser.AddSyncableParser(name, p)
}

func (db *DB) ProposeSyncable(ctx context.Context, c *cluster.Configuration) error {
	name, syncable, _, err := db.ParseSyncable(c.MimeType, c.Data, db.storage)
	if err != nil {
		return cluster.NewConfigError(err)
	}
	c.Name = name

	// Guard: a re-POST some destinations can't absorb in place (e.g. a SQL
	// projection whose CREATE-only DDL never ALTERs the table) is rejected and
	// steered to the rebuild verb, rather than silently no-op'd. The syncable
	// itself decides (ConfigChangeValidator); this layer stays destination-
	// agnostic. Returns a cluster.RebuildRequiredError the HTTP layer renders as
	// 409. Best-effort/fail-open — see the helper.
	if err := db.guardSyncableConfigChange(c.ID, syncable); err != nil {
		return err
	}

	e, err := cluster.NewUpsertSyncableEntity(c)
	if err != nil {
		return err
	}

	p := &cluster.Proposal{Entities: []*cluster.Entity{e}}
	return db.Propose(ctx, p)
}

// guardSyncableConfigChange asks the newly-parsed syncable whether replacing
// the currently-persisted config with it is safe to apply in place. The
// syncable owns the decision (cluster.ConfigChangeValidator) — this layer never
// inspects the destination shape — and a SQL syncable answers by comparing
// config-derived schemas, so no schema query hits the destination DB.
//
// Fail-open: if the new syncable doesn't validate replacements (no
// ConfigChangeValidator), there is no prior config, or the prior config can't
// be rebuilt on this node, the guard allows the POST. It is a signpost toward
// the rebuild verb, not a correctness gate — blocking a deploy on an
// un-rebuildable old config would be worse than the silent no-op it guards
// against, which only recurs in that already-degraded case.
func (db *DB) guardSyncableConfigChange(id string, next cluster.Syncable) error {
	validator, ok := next.(cluster.ConfigChangeValidator)
	if !ok {
		return nil // syncable absorbs any config change in place — nothing to guard
	}

	prior := db.currentSyncableConfig(id)
	if prior == nil {
		return nil // first POST for this id — nothing to compare against
	}

	_, priorSyncable, _, err := db.ParseSyncable(prior.MimeType, prior.Data, db.storage)
	if err != nil {
		return nil // prior config not buildable on this node — fail open
	}
	defer func() { _ = priorSyncable.Close() }() // releases the prepared stmts (not the shared pool)

	return validator.ValidateReplace(priorSyncable)
}

// RebuildSyncable re-materializes a syncable's destination in place from index
// 0, keeping the same config (and therefore the same schema — schema changes go
// through DELETE + re-POST). It is the recovery primitive for a drifted or
// corrupted projection: any rows the replay does not reproduce are gone,
// because the destination is torn down and recreated empty first. (For a SQL
// syncable the teardown/re-init is a DROP + CREATE of its table.)
//
// Sequence (the config stays on the log throughout):
//  1. Consensus: reset the checkpoint to 0 (delete the SyncableIndex entity).
//     Replicated and deterministic — every node's Reader will seed from 0.
//  2. Owner, live-only: stop the worker and tear the destination down (clean
//     slate).
//  3. Re-apply the unchanged config: the normal apply path re-initializes the
//     destination (Init) and restarts the worker, which reads from the reset
//     checkpoint (0) and replays — reusing all of saveSyncable's machinery
//     (migration wrap, worker replace) for free.
//
// Re-runnable by construction (idempotent teardown + Init + replay from 0), so
// a leadership flap mid-rebuild just means the operator runs it again.
// Leader-pinned at the HTTP layer; since Storage.Node is 0 today the leader is
// the owner, so step 2 runs where step 3's re-init will.
func (db *DB) RebuildSyncable(ctx context.Context, id string) error {
	cfg := db.currentSyncableConfig(id)
	if cfg == nil {
		return cluster.ErrResourceNotFound
	}

	// 1. Reset the checkpoint to 0 (consensus). Blocks until applied.
	reset := &cluster.Proposal{Entities: []*cluster.Entity{cluster.NewDeleteSyncableIndexEntity(id)}}
	if err := db.Propose(ctx, reset); err != nil {
		return err
	}

	// 2. Owner clean-slate: stop the worker, then tear the destination down.
	db.rebuildTeardownLocal(id)

	// 3. Re-apply the unchanged config: re-initializes the destination and
	//    restarts the worker reading from index 0. The config is identical, so
	//    the in-place-change guard sees no change and allows it.
	return db.ProposeSyncable(ctx, cfg)
}

// rebuildTeardownLocal stops the local worker and, on the owner, tears the
// syncable's destination down so the re-apply that follows recreates it empty.
// It is the owner-gated, best-effort, live-only half of a rebuild — a failed
// teardown is logged and the rebuild continues (replay then writes over the
// existing destination, a degraded-but-not-fatal rebuild).
func (db *DB) rebuildTeardownLocal(id string) {
	db.workersMu.Lock()
	handle, ok := db.syncWorkers[id]
	if ok {
		handle.cancel()
		db.workersMu.Unlock()
		<-handle.done
		db.workersMu.Lock()
		if db.syncWorkers[id] == handle {
			delete(db.syncWorkers, id)
		}
	}
	db.workersMu.Unlock()

	if !ok || handle.syncable == nil || !db.isNode(id) {
		return
	}
	teardownable, ok := handle.syncable.(cluster.Teardownable)
	if !ok {
		return
	}
	if err := teardownable.Teardown(); err != nil {
		db.logger.Error("rebuild: destination teardown failed; replay will write over the existing destination (rebuild not clean)",
			zap.String("id", id), zap.Error(err))
	}
}

// currentSyncableConfig returns the currently-persisted syncable configuration
// for id, or nil if there is none (or it can't be read). nil means "no prior
// version", which the schema guard treats as "nothing to compare".
func (db *DB) currentSyncableConfig(id string) *cluster.Configuration {
	cfgs, err := db.storage.Syncables()
	if err != nil {
		return nil
	}
	for _, c := range cfgs {
		if c.ID == id {
			return c
		}
	}
	return nil
}

func (db *DB) ParseSyncable(mimeType string, data []byte, s cluster.DatabaseStorage) (string, cluster.Syncable, cluster.SyncableMode, error) {
	return db.parser.ParseSyncable(mimeType, data, s)
}

// DeleteSyncable removes a syncable: it proposes the two delete tombstones
// (config + checkpoint index) as one atomic Actual. On apply, every node
// cancels the local worker and the owner tears down the syncable's destination
// (best-effort) — unless keepData is set, which preserves it (e.g. another
// consumer reads a SQL syncable's table, or this node has DML-only grants).
//
// The logical deletion is the authoritative act: once the Actual commits the
// syncable is gone from the log, so a restart can't re-Init the old config and
// a later same-named create starts fresh from index 0. keepData is recorded
// node-locally (this node is the leader the write proxied to, and the owner
// that performs the teardown), then consumed by deleteSync on apply.
func (db *DB) DeleteSyncable(ctx context.Context, id string, keepData bool) error {
	if keepData {
		db.workersMu.Lock()
		db.syncDeleteKeep[id] = true
		db.workersMu.Unlock()
	}

	p := &cluster.Proposal{Entities: cluster.NewDeleteSyncableEntities(id)}
	if err := db.Propose(ctx, p); err != nil {
		// Propose failed → the delete never applies, so drop the stale intent.
		if keepData {
			db.workersMu.Lock()
			delete(db.syncDeleteKeep, id)
			db.workersMu.Unlock()
		}
		return err
	}
	return nil
}

func (db *DB) Syncables() ([]*cluster.Configuration, error) {
	return db.storage.Syncables()
}

func (db *DB) SyncableVersions(id string) ([]cluster.VersionInfo, error) {
	return db.storage.SyncableVersions(id)
}

func (db *DB) SyncableVersion(id string, version uint64) (*cluster.Configuration, error) {
	return db.storage.SyncableVersion(id, version)
}
