package db

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// renderingVerdict is what verifyRenderingStamp tells the worker's start branch.
type renderingVerdict int

const (
	renderingOK    renderingVerdict = iota // stamped current (or just stamped): sync
	renderingPark                          // parked with the remedy published: exit the worker
	renderingRetry                         // the stamp could not be read or written: idle, then retry
)

// verifyRenderingStamp gates a sink that holds derived state in its
// destination (cluster.RenderingStamped): the rows must have been rendered by
// the version this binary renders, or two rules would share one table under
// one config. A pending rematerialization skips the check — the replay
// converges the rows and re-stamps on completion (completeRematerializationIfDone).
//
// An unstamped destination is stamped and served when committed cannot have
// rendered rows into it: the syncable has never checkpointed (a table the
// operator attached), or the destination is empty (a fresh materialization
// whose stamp write never landed). Otherwise committed wrote those rows
// under no stamp — a restore from a backup taken before 0.8.0, or a table
// rebuilt by hand — and their rendering cannot be verified, so it parks.
func (db *DB) verifyRenderingStamp(ctx context.Context, id string, s cluster.Syncable, checkpoint uint64) renderingVerdict {
	st, ok := cluster.SyncableAs[cluster.RenderingStamped](s)
	if !ok {
		return renderingOK
	}
	if _, pending := db.storage.SyncableRematerialization(id); pending {
		return renderingOK
	}
	version, present, err := st.RenderingStamp(ctx)
	if err != nil {
		db.logger.Error("rendering stamp unreadable; not syncing until it can be verified (retrying)",
			zap.String("id", id), zap.Error(err))
		return renderingRetry
	}
	want := st.RenderingVersion()
	switch {
	case present && version == want:
		return renderingOK
	case present:
		cause := fmt.Errorf("destination rendered under rendering version %d; this binary renders version %d — converge it: %s",
			version, want, renderingRemedy(id, s, true))
		db.logger.Error("rendering version mismatch; parked", zap.String("id", id), zap.Error(cause))
		db.publishSyncableParked(ctx, id, 0, cause)
		return renderingPark
	}
	if checkpoint > 0 {
		hasRows, err := st.DestinationHasRows(ctx)
		if err != nil {
			db.logger.Error("unstamped destination could not be probed for rows; not syncing until it is (retrying)",
				zap.String("id", id), zap.Error(err))
			return renderingRetry
		}
		if hasRows {
			cause := fmt.Errorf("destination holds rows but carries no rendering stamp (restored from a backup taken before 0.8.0, or rebuilt by hand), so their rendering cannot be verified — converge it: %s",
				renderingRemedy(id, s, false))
			db.logger.Error("rendering stamp missing; parked", zap.String("id", id), zap.Error(cause))
			db.publishSyncableParked(ctx, id, 0, cause)
			return renderingPark
		}
	}
	if err := st.StampRendering(ctx); err != nil {
		db.logger.Error("rendering stamp could not be written; not syncing until it is (retrying)",
			zap.String("id", id), zap.Error(err))
		return renderingRetry
	}
	db.logger.Info("unstamped destination stamped with the current rendering version",
		zap.String("id", id), zap.Uint64("renderingVersion", want), zap.Uint64("checkpoint", checkpoint))
	return renderingOK
}

// renderingRemedy names the verb that converges this sink: rematerialize
// where the sink can converge in place; otherwise delete and re-POST, which
// drops a destination committed created for a sink that tears down (every
// production sink) — and for a stamped sink with no teardown at all, the
// operator recreates the destination first, or the re-POST meets the same
// stamp. noted is false for a destination with no note at all: committed
// cannot tell it created that table, so a delete leaves it and the operator
// drops it.
func renderingRemedy(id string, s cluster.Syncable, noted bool) string {
	if rm, ok := cluster.SyncableAs[cluster.Rematerializable](s); ok && rm.CanRematerialize() {
		return "POST /v1/syncable/" + id + "/rematerialize"
	}
	if !noted {
		return "DELETE /v1/syncable/" + id + ", drop the table yourself (it carries no note, so committed cannot tell it created it), then re-POST the config"
	}
	if _, ok := cluster.SyncableAs[cluster.Teardownable](s); ok {
		return "DELETE /v1/syncable/" + id + " (drops the table if committed created it; otherwise drop it yourself), then re-POST the config"
	}
	return "this syncable cannot drop its destination: recreate the table by hand, then DELETE /v1/syncable/" + id + " and re-POST the config"
}

// stampAfterRematerialization records the current rendering version once a
// rematerialization has converged the destination. It runs before the
// in-progress record clears, so a stamp that fails to write keeps the
// record and the next completion pass retries.
func (db *DB) stampAfterRematerialization(ctx context.Context, id string, s cluster.Syncable) error {
	st, ok := cluster.SyncableAs[cluster.RenderingStamped](s)
	if !ok {
		return nil
	}
	return st.StampRendering(ctx)
}
