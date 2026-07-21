package db

import (
	"context"
	"fmt"

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
	// KeepData carries the delete tombstone's entity-borne preserve-the-
	// destination intent (see cluster.Entity.KeepData): the owner skips the
	// destination teardown. Deterministic on every node — no node-local state.
	KeepData bool
	// ReconcileList, when non-nil, makes this message a RECONCILE REQUEST
	// instead of a worker event: the listener executes the closure at
	// dequeue time — serialized with the apply path's own events — to get
	// the parsed CURRENT config set, then converges the running workers to
	// it (install every listed id, cancel every unlisted one). Executing at
	// dequeue is what makes a stale read structurally impossible: the old
	// RestoreSyncableWorkers listed configs on its caller's goroutine and
	// could resurrect a deleted worker or roll one back to a superseded
	// version. All other fields are ignored on a reconcile request.
	ReconcileList func() ([]*SyncableWithID, error)
}

func (db *DB) AddSyncableParser(name string, p cluster.SyncableParser) {
	db.parser.AddSyncableParser(name, p)
}

func (db *DB) ProposeSyncable(ctx context.Context, c *cluster.Configuration) error {
	name, _, _, err := db.ParseSyncable(c.MimeType, c.Data, db.storage)
	if err != nil {
		return cluster.NewConfigError(err)
	}
	c.Name = name

	// Guard: a re-POST some destinations can't absorb in place (e.g. a SQL
	// projection whose CREATE-only DDL never ALTERs the table) is rejected and
	// steered to the rebuild verb, rather than silently no-op'd. The comparison
	// is destination-specific (a SQL kind compares config-derived schemas); this
	// layer stays destination-agnostic. Returns a cluster.RebuildRequiredError the
	// HTTP layer renders as 409. Best-effort/fail-open — see the helper.
	if err := db.guardSyncableConfigChange(c); err != nil {
		return err
	}

	e, err := cluster.NewUpsertSyncableEntity(c)
	if err != nil {
		return err
	}

	p := &cluster.Proposal{Entities: []*cluster.Entity{e}}
	return db.Propose(ctx, p)
}

// guardSyncableConfigChange reports whether replacing the currently-persisted
// config for c.ID with c is safe to apply in place. It compares the two config
// documents' materialized schemas directly (SyncableSchemaChange) — this layer
// never inspects the destination shape, and a SQL kind answers from
// config-derived schemas, so no schema query hits the destination DB.
//
// Both schemas are read from the config documents alone (no database resolution),
// so a config whose ${secret} is unresolvable on this node does NOT defeat the
// guard: the destination shape is a pure function of the document, not of the
// connection. Fail-open remains only for the genuinely un-comparable cases — there
// is no prior config, a document doesn't parse at all, or the syncable kind has no
// materialized schema.
func (db *DB) guardSyncableConfigChange(c *cluster.Configuration) error {
	prior := db.currentSyncableConfig(c.ID)
	if prior == nil {
		return nil // first POST for this id — nothing to compare against
	}
	return db.parser.SyncableSchemaChange(c.MimeType, prior.Data, c.Data, db.storage)
}

// RebuildSyncable re-materializes a syncable's destination in place from index
// 0, keeping the same config (and therefore the same schema — schema changes go
// through DELETE + re-POST). It is the recovery primitive for a drifted or
// corrupted projection: any rows the replay does not reproduce are gone,
// because the destination is torn down and recreated empty first. (For a SQL
// syncable the teardown/re-init is a DROP + CREATE of its table.)
//
// Sequence (the config stays on the log throughout):
//  1. Owner, live-only: stop the worker (drain the goroutine). This MUST
//     precede the checkpoint reset — see below.
//  2. Consensus: reset the checkpoint to 0 (delete the SyncableIndex entity).
//     Replicated and deterministic — every node's Reader will seed from 0.
//  3. Owner, live-only: tear the destination down (clean slate) now that the
//     checkpoint is 0.
//  4. Re-apply the unchanged config: the normal apply path re-initializes the
//     destination (Init) and restarts the worker, which reads from the reset
//     checkpoint (0) and replays — reusing all of saveSyncable's machinery
//     (migration wrap, worker replace) for free.
//
// Why stop the worker (step 1) before the reset (step 2): the worker persists
// its checkpoint with an async consensus bump (proposeSyncableIndex) that lands
// the line AFTER the Sync a caller observes. If the reset were proposed while
// the worker still ran, a bump already in flight could commit just after the
// delete — apply order delete(→0) then bump(→N) — re-establishing a non-zero
// checkpoint, and the re-applied worker would seed from N and skip the replay
// entirely (the intermittent TestRebuildSyncable_ReRunnable CI failure; see the
// deterministic TestRebuildSyncable_StaleWorkerBumpDoesNotDefeatReset). Draining
// the worker first guarantees any bump it submitted was enqueued before this
// delete on the single ordered propose channel, so the delete commits at a
// higher log index and is the last write to the checkpoint — leaving it at 0.
//
// The destination teardown (step 3) deliberately stays AFTER the reset so a
// failure between the reset and the re-apply self-heals: a restart re-sends the
// config, and the worker seeds from the reset checkpoint (0) and replays.
//
// Re-runnable by construction (idempotent teardown + Init + replay from 0), so
// a leadership flap mid-rebuild just means the operator runs it again.
// Leader-pinned at the HTTP layer; since Storage.Node is 0 today the leader is
// the owner, so the live-only steps run where the re-init will.
func (db *DB) RebuildSyncable(ctx context.Context, id string) error {
	cfg := db.currentSyncableConfig(id)
	if cfg == nil {
		return cluster.ErrResourceNotFound
	}

	// 1. Stop the local worker first so it can't bump the checkpoint after the
	//    reset below. Returns its handle so step 3 can tear the destination down.
	//    The drain is BOUNDED (workerDrainTimeout, like every sibling handoff) —
	//    but unlike delete/replace, rebuild must NOT proceed past a failed
	//    drain: a still-live worker's in-flight checkpoint bump could land after
	//    the reset and silently defeat the replay (the exact stale-bump hazard
	//    the drain exists to prevent). Abort instead, with nothing changed; the
	//    wedged handle stays registered so a retry re-checks it (and a re-POST's
	//    replace path can still abandon it under its own semantics).
	handle, drained := db.rebuildStopWorkerLocal(id)
	if !drained {
		return fmt.Errorf("%w: rebuild aborted before the checkpoint reset — nothing changed; wait out (or fix) the destination and retry, or re-POST the config to replace the worker", cluster.ErrWorkerWedged)
	}

	// 2. Reset the checkpoint to 0 (consensus). Blocks until applied.
	reset := &cluster.Proposal{Entities: []*cluster.Entity{cluster.NewDeleteSyncableIndexEntity(id)}}
	if err := db.Propose(ctx, reset); err != nil {
		return err
	}
	if db.afterRebuildCheckpointReset != nil {
		db.afterRebuildCheckpointReset() // test-only seam; nil in production
	}

	// 3. Owner clean-slate: tear the destination down now that the checkpoint
	//    is 0, so the re-apply recreates it empty.
	db.rebuildTeardownDestinationLocal(id, handle)

	// Release the stopped worker's prepared statements before step 4 builds a
	// fresh syncable — otherwise a rebuild leaks a statement set on the pool.
	// rebuildStopWorkerLocal confirmed the drain (we aborted otherwise), so it's safe.
	db.closeDrainedSyncable(handle, id)

	// 4. Re-apply the unchanged config: re-initializes the destination and
	//    restarts the worker reading from index 0. The config is identical, so
	//    the in-place-change guard sees no change and allows it.
	return db.ProposeSyncable(ctx, cfg)
}

// rebuildStopWorkerLocal cancels and drains the local sync worker (if any) and
// deregisters it, returning its handle so the caller can later tear the
// destination down via the still-referenced syncable, plus whether the drain
// completed. A completed drain is what makes it safe to reset the checkpoint
// next: once the worker exited, it makes no further proposals, and any
// checkpoint bump it did submit was necessarily enqueued before the caller's
// subsequent reset — see RebuildSyncable. Returns (nil, true) if no worker was
// registered, and (handle, false) on a wedged worker the caller must abort on.
func (db *DB) rebuildStopWorkerLocal(id string) (*workerHandle, bool) {
	db.workersMu.Lock()
	handle, ok := db.syncWorkers[id]
	if !ok {
		db.workersMu.Unlock()
		return nil, true // no local worker — nothing to drain
	}
	handle.cancel()
	db.workersMu.Unlock()
	// Bounded, on the HTTP handler goroutine: a worker wedged in an
	// uninterruptible tx.Commit never closes done, and an unbounded wait here
	// hung the request past its write timeout and leaked the goroutine (each
	// retry parking another one). On timeout the handle stays REGISTERED —
	// unlike the delete/replace abandons — because the caller aborts and a
	// retry must find (and re-check) the still-live worker rather than run a
	// reset it could still defeat.
	if !waitDone(handle.done, db.workerDrainTimeout) {
		db.logger.Warn("rebuild: worker did not exit in time (wedged on its destination?); aborting the rebuild",
			zap.String("id", id), zap.Duration("timeout", db.workerDrainTimeout))
		return handle, false
	}
	db.workersMu.Lock()
	if db.syncWorkers[id] == handle {
		delete(db.syncWorkers, id)
	}
	db.workersMu.Unlock()
	return handle, true
}

// rebuildTeardownDestinationLocal tears the syncable's destination down on the
// owner so the re-apply that follows recreates it empty. It is the owner-gated,
// best-effort, live-only half of a rebuild — a failed teardown is logged and
// the rebuild continues (replay then writes over the existing destination, a
// degraded-but-not-fatal rebuild). handle is the worker stopped in
// rebuildStopWorkerLocal; nil (no worker was running) or a non-Teardownable
// syncable is a no-op.
func (db *DB) rebuildTeardownDestinationLocal(id string, handle *workerHandle) {
	if handle == nil || handle.syncable == nil || !db.isNode(id) {
		return
	}
	teardownable, ok := handle.syncable.(cluster.Teardownable)
	if !ok {
		return
	}
	// Bounded (runBounded): rebuild runs on an HTTP handler goroutine, and a
	// destination that wedged the worker would otherwise hang the request (and
	// leak the goroutine) until the kernel TCP timeout.
	if err, completed := runBounded(db.workerDrainTimeout, teardownable.Teardown); !completed {
		db.logger.Error("rebuild: destination teardown did not return in time (unreachable destination?); replay will write over the existing destination (rebuild not clean)",
			zap.String("id", id), zap.Duration("timeout", db.workerDrainTimeout))
	} else if err != nil {
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
	// keepData rides the delete tombstone itself (cluster.Entity.KeepData), so
	// every node — whichever holds leadership when the entry applies — honors
	// the operator's intent deterministically. The former node-local intent map
	// was lost on a propose→apply leadership change (the new leader applied
	// keepData=false and dropped the table) and was wrongly cleared on
	// AMBIGUOUS propose failures (ctx deadline / ErrProposalUnknown — the entry
	// could still commit); an entity-borne flag has neither failure mode.
	p := &cluster.Proposal{Entities: cluster.NewDeleteSyncableEntities(id, keepData)}
	return db.Propose(ctx, p)
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
