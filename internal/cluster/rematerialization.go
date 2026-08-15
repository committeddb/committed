package cluster

import (
	"context"
	"errors"

	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/clusterpb"
)

// Re-materialization: the derivation layer's "disposable, rebuildable"
// promise as a verb. The worker replays its topic from index 0 through the
// CURRENT projection + interpretation while the keyed sink keeps serving:
// upserts converge rows in place, every touched row is stamped with the
// replay's epoch in a committed-managed column, and when the replay reaches
// the target head the sink SWEEPS the rows the replay never positively
// re-emitted (rows an old projection wrote that the current one no longer
// produces). Non-destructive — the destructive sibling (drop + replay) is
// the rebuild verb.

// syncableRematerializationType is the replicated in-progress record's
// internal system type (see clusterpb.LogSyncableRematerialization).
var syncableRematerializationType = registerSystemType(&Type{
	ID:         reservedSystemID(compatUngated, 3),
	Name:       "InternalSyncableRematerialization",
	Version:    1,
	EntityKind: EntityKindSnapshot,
}, AdmissionCoordination)

func IsSyncableRematerialization(id string) bool {
	return id == syncableRematerializationType.ID
}

// SyncableRematerialization is the in-progress record: the syncable is
// replaying from 0; the sweep fires when its checkpoint reaches TargetHead.
type SyncableRematerialization struct {
	ID         string
	TargetHead uint64
}

func (r *SyncableRematerialization) Marshal() ([]byte, error) {
	return proto.Marshal(&clusterpb.LogSyncableRematerialization{ID: r.ID, TargetHead: r.TargetHead})
}

func (r *SyncableRematerialization) Unmarshal(bs []byte) error {
	lr := &clusterpb.LogSyncableRematerialization{}
	if err := proto.Unmarshal(bs, lr); err != nil {
		return err
	}
	r.ID = lr.ID
	r.TargetHead = lr.TargetHead
	return nil
}

func NewSyncableRematerializationEntity(r *SyncableRematerialization) (*Entity, error) {
	bs, err := r.Marshal()
	if err != nil {
		return nil, err
	}
	return NewUpsertEntity(syncableRematerializationType, []byte(r.ID), bs), nil
}

func NewDeleteSyncableRematerializationEntity(id string) *Entity {
	return NewDeleteEntity(syncableRematerializationType, []byte(id))
}

// ErrNotRematerializable refuses the verb for sinks that cannot converge a
// replay in place: a keyless/append sink would duplicate every row, and a
// webhook has no addressable rows to sweep. The rebuild verb (drop + replay)
// or a blue-green replacement remain available.
var ErrNotRematerializable = errors.New("this syncable's sink cannot re-materialize in place: only keyed sinks converge a replay (keyless/append sinks would duplicate rows; webhooks have no rows to sweep) — use POST /syncable/{id}/rebuild or a blue-green replacement instead")

// Rematerializable is the optional Syncable extension for keyed sinks that
// can converge a non-destructive replay. Wrappers (interpretation, migration)
// forward it, preserving CanRematerialize's answer from the innermost sink.
type Rematerializable interface {
	// CanRematerialize reports whether this sink supports in-place
	// convergence (keyed sinks only). Checked at the verb's admission.
	CanRematerialize() bool
	// BeginRematerialization enables epoch marking: every subsequent apply
	// stamps the sink row with this epoch (a committed-managed column, added
	// idempotently like the generation column). epoch is stable across
	// restarts of one re-materialization (the record's target head), so a
	// resumed replay keeps converging under the same mark.
	BeginRematerialization(ctx context.Context, epoch uint64) error
	// CompleteRematerialization deletes every sink row whose mark predates
	// the epoch — the rows this replay never positively re-emitted — and
	// disables marking. Idempotent: re-running after a completed sweep
	// removes nothing further.
	CompleteRematerialization(ctx context.Context) error
}
