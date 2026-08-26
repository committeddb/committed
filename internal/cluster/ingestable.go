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

// IngestableConfigChangeValidator is the optional Ingestable extension that
// vets an in-place config replacement — the ingestable analogue of
// ConfigChangeValidator for syncables. The propose path calls ValidateReplace on
// the newly-parsed ingestable, passing the ingestable built from the
// currently-persisted config; a non-nil result rejects the re-POST.
//
// It exists because an ingestable's primaryKey is part of its on-disk snapshot
// state contract: it defines the entity-key encoding (CompositeKey), the
// snapshot resume cursor (SnapshotProgress.LastPkByTable), and therefore the
// downstream sink's row identity. A persisted Position is inherited wholesale on
// re-POST (keyed by ingestable id) with NO check that it was written under the
// same primaryKey, so changing the primaryKey in place would silently mis-page
// the resume cursor (duplicate rows) and orphan already-synced rows under their
// old keys. A SQL ingestable answers by comparing its primaryKey against prior's
// and returns a RebuildRequiredError steering the operator to delete + recreate
// (which clears the Position — see NewDeleteIngestableEntities) and rebuild the
// syncables consuming its topic. The validator is destination-specific; the
// generic layers never see the destination shape.
type IngestableConfigChangeValidator interface {
	// ValidateReplace reports whether replacing prior's config with this
	// ingestable's config is safe to apply in place, returning a
	// RebuildRequiredError (or other error) if not, nil if it is.
	ValidateReplace(prior Ingestable) error
}

// IngestableStatus is a point-in-time operational snapshot of an ingestable
// worker: which phase it is in, how far the initial snapshot got, where the
// change-data-capture cursor sits, and how far behind the source it is. It is
// what GET /v1/ingestable/{id}/status answers — the ingest analogue of a
// syncable's progress/lag.
// Worker lifecycle states reported by the syncable/ingestable status and pipeline
// endpoints. running/recovering/parked derive from the replicated stuck/parked
// records so any node reports them identically; degraded derives from the
// ANSWERING NODE's config-error record (a build failure is node-local — a
// missing ${VAR} on one node doesn't stop another node's build), so it reports
// this node's view. The worker that matters runs on the owner node; a
// ?readPosition=true call proxies there and carries the owner's view.
const (
	WorkerStateRunning    = "running"     // healthy (sync: or transiently stuck)
	WorkerStateRecovering = "recovering"  // ingest-only: frozen, the supervisor is restarting it
	WorkerStateParked     = "parked"      // terminally parked — operator must fix the config
	WorkerStateRederiving = "re-deriving" // sync only: the worker is re-deriving node-local stage state before consuming (a reset store, an ownership move, or a lost NoSync tail); progress in stageRecovery
	WorkerStateDegraded   = "degraded"    // config persisted but its node-local build FAILED — no worker was (re)started; fix the environment/config and re-POST
)

// ParkedWorker identifies a terminally-parked worker for the /node/status summary.
// Detail (since / message / blocked index) is on the per-resource status endpoint.
type ParkedWorker struct {
	Kind string // "sync" or "ingest"
	ID   string
}

// Lag units reported by IngestableStatus.LagUnit. The unit follows the
// positioning mode, not just the dialect: Postgres and MySQL-file:pos report
// bytes behind the source's write head; MySQL-GTID reports transactions.
const (
	LagUnitBytes        = "bytes"
	LagUnitTransactions = "transactions"
)

type IngestableStatus struct {
	// WorkerState is the worker's lifecycle state: "running" or "parked" (the
	// freeze/restart supervisor gave up; fix the config and re-POST it, or delete).
	// Replicated, so it is reported truthfully from any node — even one with no
	// local worker handle.
	WorkerState string
	// Phase is "pending" until anything has durably checkpointed (a just-created
	// ingestable, or one still retrying its first snapshot batch), "snapshot"
	// while the worker is still dumping existing rows, then "streaming" once the
	// snapshot is complete and it is following the CDC stream. Derived from the
	// checkpoint: an empty position is pending; a position that still carries
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
	// durably consumed. The unit is mode-dependent — read LagUnit: Postgres
	// reports bytes (pg_current_wal_lsn − confirmed_flush_lsn of the slot);
	// MySQL under GTID positioning reports transactions (count of
	// @@gtid_executed − the consumed GTID set); MySQL under file:pos
	// positioning (gtid_mode=OFF / a legacy checkpoint) reports bytes behind
	// the binlog write head, computed from the source's binlog inventory. nil
	// when it cannot be determined — during the snapshot phase, when the
	// source is unreachable, or when a re-snapshot is required. A non-nil 0
	// means fully caught up.
	Lag *uint64
	// LagUnit names Lag's unit — LagUnitBytes or LagUnitTransactions — so a
	// caller never has to guess which scale a number is on (a dashboard alarm
	// at lag > 1000 means wildly different things in transactions vs bytes).
	// Empty exactly when Lag is nil.
	LagUnit string
	// CaughtUp is true exactly when the snapshot is complete and Lag is a
	// known 0 — the only state in which the read model is fully current. It is
	// never true while Lag is nil (an unknown lag is not a caught-up lag).
	CaughtUp bool
	// ReSnapshotRequired is true when the source has discarded change data this
	// ingest never consumed and can never re-stream — a MySQL source that purged
	// binlogs past the consumed GTID set (@@gtid_purged ⊄ consumed). It is a
	// distinct, loud state rather than a misleading lag number: recovery means
	// re-running the initial snapshot. Always false for Postgres — NOT because a
	// slot can't lose WAL (a reaped slot via max_slot_wal_keep_size, drop, or
	// expiry DOES discard unconsumed WAL), but because the dialect recovers
	// automatically: it re-snapshots from the new slot's consistent point and
	// bumps the refresh epoch, emitting a refresh-boundary marker that sweeps the
	// rows deleted in the lost window off the downstream sink (reconciliation, not
	// just re-load). So the gap is handled in-band rather than surfaced here.
	ReSnapshotRequired bool
}

// TableSnapshotStatus is one watched table's place in the initial snapshot.
// The keyset cursor (the last primary-key value dumped) is deliberately NOT
// carried here: a natural PK is often source PII, so it is not exposed via the
// status API — see the status endpoint and the snapshot logger, which omits it
// for the same reason.
type TableSnapshotStatus struct {
	// Table is the source table name as configured.
	Table string
	// Topic is the id of the topic this table feeds (its spec's Type.ID). A
	// single-topic ingestable tags every table with the one topic; a multi-topic
	// ingestable ([[sql.topics]]) shows which topic each table routes to, so an
	// operator can read per-topic snapshot progress off the flat table list.
	Topic string
	// Complete is whether this table's snapshot finished.
	Complete bool
	// ChunksTotal/ChunksDone report a chunked parallel snapshot's per-table
	// progress (snapshot_readers > 1 with a splittable PK): how many PK-range
	// chunks the frozen plan holds and how many have finished. Both zero for
	// a single-stream table.
	ChunksTotal int
	ChunksDone  int
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
}, AdmissionConfig)

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

// ingestableStuckType is the first namespaced system type: a terminal-park
// coordination record is pure observability, so it is UNGATED (index 0) — an
// older node that doesn't know it skips-and-warns rather than bricking, and it
// needs no FeatureLevel gate. See system_type_namespace.go.
var ingestableStuckType = registerSystemType(&Type{
	ID:         reservedSystemID(compatUngated, 0),
	Name:       "InternalIngestableStuck",
	Version:    1,
	EntityKind: EntityKindSnapshot,
}, AdmissionCoordination)

// IngestableStuck records that an ingestable's worker has TERMINALLY parked: the
// freeze/restart supervisor gave up after repeated freezes at the same resume
// position (a proposal it cannot commit). It is the ingest analogue of a parked
// SyncableStuck — replicated so any node reports it, and it survives the worker's
// exit so the parked state stays visible across a leadership change / restart. It
// clears only on an operator fix (a new config version) or a delete. Keyed by the
// ingestable id, one current value per ingestable.
type IngestableStuck struct {
	ID            string
	SinceUnixNano int64
	Message       string
}

func (i *IngestableStuck) Marshal() ([]byte, error) {
	li := &clusterpb.LogIngestableStuck{
		ID:            i.ID,
		SinceUnixNano: i.SinceUnixNano,
		Message:       i.Message,
	}
	return proto.Marshal(li)
}

func (i *IngestableStuck) Unmarshal(bs []byte) error {
	li := &clusterpb.LogIngestableStuck{}
	if err := proto.Unmarshal(bs, li); err != nil {
		return err
	}
	i.ID = li.ID
	i.SinceUnixNano = li.SinceUnixNano
	i.Message = li.Message
	return nil
}

func IsIngestableStuck(id string) bool {
	return id == ingestableStuckType.ID
}

// NewUpsertIngestableStuckEntity wraps a parked-worker record, keyed by the
// ingestable id.
func NewUpsertIngestableStuckEntity(i *IngestableStuck) (*Entity, error) {
	bs, err := i.Marshal()
	if err != nil {
		return nil, err
	}
	return NewUpsertEntity(ingestableStuckType, []byte(i.ID), bs), nil
}

// NewDeleteIngestableStuckEntity clears the parked record for an ingestable.
func NewDeleteIngestableStuckEntity(id string) *Entity {
	return NewDeleteEntity(ingestableStuckType, []byte(id))
}

var ingestablePositionType = registerSystemType(&Type{
	ID:         "8ea60a68-e22a-41cd-b09d-31352b0356f1",
	Name:       "InternalIngestablePosition",
	Version:    1,
	EntityKind: EntityKindSnapshot,
}, AdmissionPosition)

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
