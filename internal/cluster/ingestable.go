package cluster

import (
	"context"
	"errors"

	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/clusterpb"
)

type Position []byte

// ErrIngestableNotRunning is returned by Cluster.IngestableStatus when no
// ingest worker is registered for the id on the node that answered. Callers
// (the HTTP layer) map it to 404.
var ErrIngestableNotRunning = errors.New("cluster: no ingestable worker is running for this id")

// Ingestable pulls changes from an external source (e.g. a SQL database's
// change-data-capture stream) and emits them as Proposals into the log. It is
// the producing counterpart of Syncable: an Ingestable writes Proposals; a
// Syncable consumes the resulting Actuals.
//
// Contract:
//   - Ingest MUST emit deletes. When the source signals that a row was
//     removed (a CDC DELETE), the Ingestable MUST emit a delete entity
//     (NewDeleteEntity) keyed by that row's primary key — NOT an upsert of
//     the row's pre-image. This is mandatory for the same reason honoring
//     deletes is mandatory for a Syncable (see the Syncable contract): the
//     entity flows source → log → Syncable, and only a delete entity makes
//     the Syncable remove the downstream record. An Ingestable that forwards a
//     source delete as an upsert of the old row leaves the deleted data live
//     in every projection forever — a right-to-be-forgotten zombie. The
//     delete's Key MUST equal the key an upsert of that same row uses, so the
//     downstream delete targets the right record.
//   - During ingestion, write Proposals to pr and position checkpoints to po;
//     how often to checkpoint the position is up to the Ingestable.
//   - Check ctx for done after every proposal and stop promptly when it fires.
//   - Ingest MUST support being called multiple times (resume from pos).
//
//counterfeiter:generate . Ingestable
type Ingestable interface {
	Ingest(ctx context.Context, pos Position, pr chan<- *Proposal, po chan<- Position) error
	// Status reports the worker's point-in-time progress for pos (the
	// persisted checkpoint position). It decodes the dialect's own cursor —
	// snapshot phase vs. streaming, per-table snapshot progress, the CDC
	// position — and, where the dialect supports it, queries the source for
	// replication lag. Read-only and side-effect free: safe to call while
	// Ingest is running. It must tolerate any pos a prior Ingest checkpointed,
	// including the empty position (a worker that has not checkpointed yet).
	Status(ctx context.Context, pos Position) (IngestableStatus, error)
	Close() error
}

// IngestableTeardownable is the optional Ingestable extension implemented by
// ingestables that own destructive teardown of their SOURCE-side replication
// resources (the Postgres dialect drops its replication slot + publication; an
// orphaned slot pins the source's WAL and can fill the source's disk). The
// ingest delete path type-asserts it — exactly like Syncable's Teardownable —
// and, on the owner node only, calls Teardown AFTER the logical (consensus)
// deletion succeeds and the worker has stopped (so the slot is inactive).
//
// Teardown must be idempotent (safe to re-run after a leadership flap, e.g. via
// IF EXISTS) and is a destructive side effect: owner-gated and live-only, never
// on a replaying or non-owner node. Best-effort — the logical delete already
// committed, so a failure only leaves an orphaned slot an operator can drop.
type IngestableTeardownable interface {
	Teardown() error
}

// IngestableStatus is a point-in-time operational snapshot of an ingestable
// worker: which phase it is in, how far the initial snapshot got, where the
// change-data-capture cursor sits, and how far behind the source it is. It is
// what GET /v1/ingestable/{id}/status answers — the ingest analogue of a
// syncable's progress/lag.
type IngestableStatus struct {
	// Phase is "snapshot" while the worker is still dumping existing rows, then
	// "streaming" once the snapshot is complete and it is following the CDC
	// stream. Derived from the checkpoint: a position that still carries
	// snapshot progress is in the snapshot phase.
	Phase string
	// SnapshotProgress is per watched table: the keyset cursor reached and
	// whether that table's snapshot finished. Present in both phases (after the
	// snapshot completes every table reads Complete=true) so a caller can see
	// what the snapshot covered.
	SnapshotProgress []TableSnapshotStatus
	// Position is the dialect's CDC cursor in its native text form — a Postgres
	// LSN ("0/1A2B3C8") or a MySQL binlog coordinate ("binlog.000004:1547").
	// For Postgres this checkpoint LSN is also the effectively-once resume +
	// dedup point, so there is no separate sequence to report.
	Position string
	// Lag is how far the source's write head is ahead of what this ingest has
	// durably consumed. The unit is the dialect's natural one: Postgres reports
	// bytes (pg_current_wal_lsn − confirmed_flush_lsn of the slot); MySQL under
	// GTID positioning reports transactions (count of @@gtid_executed − the
	// consumed GTID set). nil when it cannot be determined — during the snapshot
	// phase, when the source is unreachable, on a MySQL source without GTID
	// positioning (gtid_mode=OFF / legacy file:pos checkpoint), or when a
	// re-snapshot is required. A non-nil 0 means fully caught up.
	Lag *uint64
	// CaughtUp is true exactly when the snapshot is complete and Lag is a
	// known 0 — the only state in which the read model is fully current. It is
	// never true while Lag is nil (an unknown lag is not a caught-up lag).
	CaughtUp bool
	// ReSnapshotRequired is true when the source has discarded change data this
	// ingest never consumed and can never re-stream — a MySQL source that purged
	// binlogs past the consumed GTID set (@@gtid_purged ⊄ consumed). It is a
	// distinct, loud state rather than a misleading lag number: recovery means
	// re-running the initial snapshot. Always false for Postgres (the slot holds
	// the WAL, so the source cannot purge unconsumed change data out from under
	// it).
	ReSnapshotRequired bool
}

// TableSnapshotStatus is one watched table's place in the initial snapshot.
type TableSnapshotStatus struct {
	// Table is the source table name as configured.
	Table string
	// LastKey is the keyset-pagination cursor the snapshot has reached for this
	// table (the last primary-key value dumped). Empty if the table's snapshot
	// has not started, or once it is Complete (the cursor is no longer
	// tracked).
	LastKey string
	// Complete is whether this table's snapshot finished.
	Complete bool
}

//counterfeiter:generate . IngestableParser
type IngestableParser interface {
	Parse(c *ParsedConfig) (Ingestable, error)
}

// EntityKindRevision (version-stored config with rollback, retained — see
// typeType).
var ingestableType = registerSystemType(&Type{
	ID:         "c5917145-c248-4d97-a863-8e26ca042b09",
	Name:       "InternalIngestableParser",
	Version:    1,
	EntityKind: EntityKindRevision,
})

func IsIngestable(id string) bool {
	return id == ingestableType.ID
}

func NewUpsertIngestableEntity(c *Configuration) (*Entity, error) {
	bs, err := c.Marshal()
	if err != nil {
		return nil, err
	}

	return NewUpsertEntity(ingestableType, []byte(c.ID), bs), nil
}

// NewDeleteIngestableEntities builds the tombstones that remove an ingestable:
// its config AND its checkpoint position. Both go in one proposal so DELETE is
// atomic — the config removal stops the worker (and, on the owner, tears down the
// source slot), and clearing the checkpoint means a same-id recreate starts from
// a full snapshot rather than resuming from a stale LSN whose slot was dropped.
// The keys mirror the upsert constructors (the ingestable ID).
func NewDeleteIngestableEntities(id string) []*Entity {
	return []*Entity{
		NewDeleteEntity(ingestableType, []byte(id)),
		NewDeleteEntity(ingestablePositionType, []byte(id)),
	}
}

var ingestablePositionType = registerSystemType(&Type{
	ID:         "8ea60a68-e22a-41cd-b09d-31352b0356f1",
	Name:       "InternalIngestablePosition",
	Version:    1,
	EntityKind: EntityKindSnapshot,
})

type IngestablePosition struct {
	ID       string
	Position []byte
}

func (i *IngestablePosition) Marshal() ([]byte, error) {
	li := &clusterpb.LogIngestablePosition{ID: i.ID, Position: i.Position}
	return proto.Marshal(li)
}

func (i *IngestablePosition) Unmarshal(bs []byte) error {
	li := &clusterpb.LogIngestablePosition{}
	err := proto.Unmarshal(bs, li)
	if err != nil {
		return err
	}

	i.ID = li.ID
	i.Position = li.Position

	return nil
}

func IsIngestablePosition(id string) bool {
	return id == ingestablePositionType.ID
}

func NewUpsertIngestablePositionEntity(p *IngestablePosition) (*Entity, error) {
	bs, err := p.Marshal()
	if err != nil {
		return nil, err
	}

	return NewUpsertEntity(ingestablePositionType, []byte(p.ID), bs), nil
}
