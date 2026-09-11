package db

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// renderingStampMissingAssumesCurrent: 0.8.0 introduces the stamp, so every
// destination that predates it is unstamped. This release stamps such a
// destination with the current version on first start — rendering did not
// change in 0.8.0, so the assumption holds exactly once. 0.8.1 flips this to
// park (a non-empty, unstamped destination then means a restore or a hand
// rebuild whose rendering cannot be verified); see the ticket
// sink-stamp-missing-parks-in-0-8-1.
const renderingStampMissingAssumesCurrent = true

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
func (db *DB) verifyRenderingStamp(ctx context.Context, id string, s cluster.Syncable) renderingVerdict {
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
		cause := fmt.Errorf("destination rendered under sink rendering version %d; this binary renders version %d — converge it: %s",
			version, want, renderingRemedy(id, s))
		db.logger.Error("rendering version mismatch; parked", zap.String("id", id), zap.Error(cause))
		db.publishSyncableParked(ctx, id, 0, cause)
		return renderingPark
	case renderingStampMissingAssumesCurrent:
		if err := st.StampRendering(ctx); err != nil {
			db.logger.Error("rendering stamp could not be written; not syncing until it is (retrying)",
				zap.String("id", id), zap.Error(err))
			return renderingRetry
		}
		db.logger.Info("destination stamped with the current rendering version (first contact)",
			zap.String("id", id), zap.Uint64("renderingVersion", want))
		return renderingOK
	default:
		cause := fmt.Errorf("destination carries no rendering stamp, so its rendering cannot be verified — converge it: %s",
			renderingRemedy(id, s))
		db.logger.Error("rendering stamp missing; parked", zap.String("id", id), zap.Error(cause))
		db.publishSyncableParked(ctx, id, 0, cause)
		return renderingPark
	}
}

// renderingRemedy names the verb that converges this sink: rematerialize
// where the sink can converge in place; otherwise delete and re-POST, which
// drops the destination for a sink that tears down (the SQL family) — and
// for one that does not (Iceberg keeps its table on delete), the operator
// recreates the destination first, or the re-POST meets the same stamp.
func renderingRemedy(id string, s cluster.Syncable) string {
	if rm, ok := cluster.SyncableAs[cluster.Rematerializable](s); ok && rm.CanRematerialize() {
		return "POST /v1/syncable/" + id + "/rematerialize"
	}
	if _, ok := cluster.SyncableAs[cluster.Teardownable](s); ok {
		return "DELETE /v1/syncable/" + id + " (drops the table if committed created it; otherwise drop it yourself), then re-POST the config"
	}
	return "this sink cannot drop its destination: recreate the table by hand, then DELETE /v1/syncable/" + id + " and re-POST the config"
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
