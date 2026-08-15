package db

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// RematerializeSyncable replays a syncable's topic from index 0 through the
// CURRENT projection + interpretation while its keyed sink keeps serving —
// the NON-destructive sibling of RebuildSyncable (which drops the destination
// first). Keyed upserts converge rows in place; every re-emitted row is
// stamped with the replay's epoch; when the replay reaches the target head
// (the data head at this call) the sink sweeps the rows the replay never
// positively re-emitted and the in-progress record clears. It is the cure
// for projection rot (a mapping added, a migration fixed, an erratum landed:
// everything already in the sink was derived under the old logic) — and the
// re-derivation that refreshes the checkpoint's interpretation pin.
//
// Sequence, mirroring RebuildSyncable's ordering proof (drain before reset,
// so no stale checkpoint bump can defeat the replay):
//  1. Admission: the sink must support in-place convergence (keyed SQL sinks
//     do; keyless/append sinks and webhooks refuse — use rebuild/blue-green).
//  2. Owner, live-only: drain the worker; abort if wedged.
//  3. Consensus: reset the checkpoint to 0. FIRST — a crash after the reset
//     but before the record leaves a plain full replay (harmless, keyed
//     upserts converge, no sweep); the record-first order would let a resumed
//     worker mark only from mid-log and then WRONGLY sweep the unmarked
//     prefix.
//  4. Consensus: the in-progress record {id, targetHead} — replicated, so a
//     restart resumes the re-materialization and any node reports progress.
//  5. Re-apply the unchanged config: the worker restarts, observes the
//     record, begins epoch marking, replays from 0, and sweeps at the target.
func (db *DB) RematerializeSyncable(ctx context.Context, id string) error {
	cfg := db.currentSyncableConfig(id)
	if cfg == nil {
		return cluster.ErrResourceNotFound
	}
	if zone, active, owner := db.syncableZonePin(id); zone != "" && active {
		switch {
		case owner == 0:
			return cluster.ErrZonePinUnsatisfiable
		case owner != db.ID():
			// The HTTP layer routes this verb to the owner; landing here means
			// a stale routing view — refuse rather than race the remote
			// worker's in-flight checkpoint bump.
			return cluster.ErrNotSyncableOwner
		}
	}

	// 1. Admission probe: build (but never run) the syncable to ask its sink.
	probe, err := db.buildSyncable(id)
	if err != nil {
		return cluster.NewConfigError(fmt.Errorf("build syncable for admission: %w", err))
	}
	canRemat := false
	if rm, ok := probe.(cluster.Rematerializable); ok {
		canRemat = rm.CanRematerialize()
	}
	_ = probe.Close()
	if !canRemat {
		return cluster.ErrNotRematerializable
	}

	// 2. Drain the worker (see RebuildSyncable for why this must precede the
	//    checkpoint reset).
	handle, drained := db.rebuildStopWorkerLocal(id)
	if !drained {
		return fmt.Errorf("%w: re-materialization aborted before the checkpoint reset — nothing changed; wait out (or fix) the destination and retry", cluster.ErrWorkerWedged)
	}

	// The target: everything at or below this data head must be re-derived
	// before the sweep may run. Live writes past it simply follow in order.
	_, targetHead, err := db.SyncableProgress(id)
	if err != nil {
		return err
	}

	// 3. Reset the checkpoint (consensus; blocks until applied).
	reset := &cluster.Proposal{Entities: []*cluster.Entity{cluster.NewDeleteSyncableIndexEntity(id)}}
	if err := db.Propose(ctx, reset); err != nil {
		return err
	}

	// 4. The in-progress record (consensus).
	rec, err := cluster.NewSyncableRematerializationEntity(&cluster.SyncableRematerialization{ID: id, TargetHead: targetHead})
	if err != nil {
		return err
	}
	if err := db.Propose(ctx, &cluster.Proposal{Entities: []*cluster.Entity{rec}}); err != nil {
		return err
	}

	// Release the drained worker's statements before the re-apply builds a
	// fresh syncable (mirrors RebuildSyncable).
	db.closeDrainedSyncable(handle, id)

	// 5. Re-apply the unchanged config: worker restarts, sees the record,
	//    replays from 0 with epoch marking, sweeps at the target.
	return db.ProposeSyncable(ctx, cfg)
}

// rematState is a sync worker's view of an in-progress re-materialization:
// zero when none. Loaded at leadership gain, cleared at completion.
type rematState struct {
	target uint64
	active bool
}

// beginRematerializationIfRequested checks for an in-progress record at
// worker start and begins epoch marking. Failures are logged and the record
// left in place — the operator sees the stuck progress and can retry the
// verb; the worker syncs normally (a plain replay) rather than wedging.
func (db *DB) beginRematerializationIfRequested(ctx context.Context, id string, s cluster.Syncable) rematState {
	rec, ok := db.storage.SyncableRematerialization(id)
	if !ok {
		return rematState{}
	}
	rm, isRM := s.(cluster.Rematerializable)
	if !isRM || !rm.CanRematerialize() {
		// Admission prevents this; a config re-POST to a non-keyed shape
		// mid-remat could still reach it. Clear the record loudly: the sweep
		// contract can't be honored, and a plain replay is what runs.
		db.logger.Error("re-materialization record present but the sink cannot converge in place; clearing the record (the replay still runs, without a sweep)",
			zap.String("id", id))
		db.proposeDeleteRematerialization(ctx, id)
		return rematState{}
	}
	if err := rm.BeginRematerialization(ctx, rec.TargetHead); err != nil {
		db.logger.Error("begin re-materialization failed; syncing normally (the record stays; fix the destination and re-run the verb)",
			zap.String("id", id), zap.Error(err))
		return rematState{}
	}
	db.logger.Info("re-materializing: replaying from index 0 with epoch marking",
		zap.String("id", id), zap.Uint64("targetHead", rec.TargetHead))
	return rematState{target: rec.TargetHead, active: true}
}

// completeRematerializationIfDone runs the sweep once the worker's consumed
// head reaches the target, then clears the record. lastSeen is this stint's
// consumed head; the durable checkpoint covers a resumed stint that had
// nothing left to read.
func (db *DB) completeRematerializationIfDone(ctx context.Context, id string, s cluster.Syncable, st *rematState, lastSeen uint64) {
	if !st.active {
		return
	}
	cp, _, err := db.SyncableProgress(id)
	if err != nil {
		return
	}
	if max(lastSeen, cp) < st.target {
		return
	}
	rm, ok := s.(cluster.Rematerializable)
	if !ok {
		st.active = false
		return
	}
	if err := rm.CompleteRematerialization(ctx); err != nil {
		db.logger.Error("re-materialization sweep failed; will retry (the record stays until the sweep succeeds)",
			zap.String("id", id), zap.Error(err))
		return
	}
	db.proposeDeleteRematerialization(ctx, id)
	st.active = false
	db.logger.Info("re-materialization complete: sink converged and swept", zap.String("id", id))
}

func (db *DB) proposeDeleteRematerialization(ctx context.Context, id string) {
	e := cluster.NewDeleteSyncableRematerializationEntity(id)
	if err := db.Propose(ctx, &cluster.Proposal{Entities: []*cluster.Entity{e}}); err != nil {
		db.logger.Warn("clear re-materialization record failed; a later completion pass retries",
			zap.String("id", id), zap.Error(err))
	}
}

// SyncableRematerialization implements cluster.Cluster: the in-progress
// record, from replicated storage.
func (db *DB) SyncableRematerialization(id string) (*cluster.SyncableRematerialization, bool) {
	return db.storage.SyncableRematerialization(id)
}
