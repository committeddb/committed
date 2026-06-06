package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/clusterpb"
)

// ErrPermanent wraps a Sync error that retrying will not fix (e.g.,
// constraint violations, malformed data). db.sync logs and skips
// proposals returning a permanent error instead of retrying.
var ErrPermanent = errors.New("syncable: permanent error")

// Permanent marks err as non-retryable by wrapping it with ErrPermanent.
// Use this in Syncable implementations to signal that the proposal should
// be skipped rather than retried.
func Permanent(err error) error {
	return fmt.Errorf("%w: %w", ErrPermanent, err)
}

// ErrSyncNotStuck is returned by Cluster.DeadLetterStuckSyncable when the
// syncable is not currently blocked retrying a transient error on this node
// — it is healthy, its id is unknown, or the request reached a node other
// than the one running the worker (the "currently blocked" state is
// node-local). The HTTP layer maps this to 409 Conflict.
var ErrSyncNotStuck = errors.New("cluster: syncable is not currently blocked on this node")

// ErrNotDeadLettered is returned by Cluster.ReplaySyncableDeadLetter when the
// requested raft index is not a dead letter for the syncable (it was never
// dead-lettered, or it has already been replayed and cleared). The HTTP layer
// maps this to 404.
var ErrNotDeadLettered = errors.New("cluster: index is not a dead letter for this syncable")

// ErrReplaySyncFailed wraps the error a replay's re-Sync returned: the
// syncable's downstream still rejected the proposal, so the dead letter is
// left in place. The HTTP layer maps this to 502 and surfaces the cause.
var ErrReplaySyncFailed = errors.New("cluster: replay re-sync failed")

type ShouldSnapshot bool

// Syncable consumes committed Actuals from the log and applies them to
// an external system (e.g., a SQL database, webhook, file). It is handed
// Actuals (committed facts with an Index), in Index order — never Proposals.
//
// Contract:
//   - Sync MUST be idempotent. The same Actual may be delivered more
//     than once due to leader transition, worker replace, or process
//     restart. Implementations should use upsert or equivalent semantics.
//   - Sync MUST honor deletes. When an entity reports IsDelete(), the
//     syncable MUST remove the corresponding downstream record keyed by
//     the entity's Key — it has no Data to apply (Data carries the delete
//     sentinel, not a payload, so it must NOT be unmarshaled). This is the
//     downstream half of right-to-be-forgotten erasure: the scrubber
//     removes a subject's entries from the permanent log, and honoring the
//     delete proposal is what removes the same subject from every
//     projection. A delete for a record that does not exist MUST be a
//     harmless no-op — that is what makes a fresh syncable replaying an
//     already-scrubbed log correct (the bootstrap edge case: the original
//     upsert may have been scrubbed away before this syncable ever saw it).
//   - An Actual's entities are one atomic unit (one committed transaction);
//     apply them together (e.g. in one destination transaction).
//   - Sync errors are retried with exponential backoff. Wrap with
//     cluster.Permanent(err) to skip the Actual instead of retrying.
//   - Sync receives a context tied to the worker lifecycle; respect
//     ctx.Done() for cooperative shutdown.
//
//counterfeiter:generate . Syncable
type Syncable interface {
	Sync(ctx context.Context, a *Actual) (ShouldSnapshot, error)
	Close() error
}

// SyncableMode controls whether a syncable sees entities at the
// version they were proposed under (AsStored) or transparently
// upgraded to the current type version (AlwaysCurrent). Mode is a
// per-syncable configuration declared in the syncable's TOML under
// `syncable.mode`; the default is AsStored, which matches the
// pre-migration-runner behaviour.
type SyncableMode int

const (
	// ModeAsStored delivers entities with their data untouched. The
	// syncable sees the exact bytes that were written under whatever
	// Type version was current at propose time. Version migrations are
	// the syncable's responsibility.
	ModeAsStored SyncableMode = 0
	// ModeAlwaysCurrent runs each entity's data through the migration
	// chain (Type.Migration programs from stamped-version+1 up to
	// current) before the syncable sees it. The syncable only has to
	// handle the shape the current type version defines.
	ModeAlwaysCurrent SyncableMode = 1
)

// ParseSyncableMode maps the TOML string form to a SyncableMode.
// Unknown strings return an error so typos surface at config-parse
// time rather than silently defaulting.
func ParseSyncableMode(s string) (SyncableMode, error) {
	switch s {
	case "", "as-stored":
		return ModeAsStored, nil
	case "always-current":
		return ModeAlwaysCurrent, nil
	}
	return 0, fmt.Errorf("unknown syncable mode %q (expected \"as-stored\" or \"always-current\")", s)
}

// BatchSyncable is an optional extension of Syncable for implementations
// that benefit from processing multiple Actuals in a single transaction
// (e.g., SQL databases). db.sync checks for this interface at startup and
// uses SyncBatch when available, falling back to per-Actual Sync otherwise.
//
// SyncBatch receives a slice of Actuals (each itself an atomic unit) and
// returns whether the batch should be snapshotted. On success, the caller
// advances SyncableIndex to the last Actual in the batch.
//
// Error semantics match Syncable.Sync: wrap with cluster.Permanent(err)
// to skip Actuals. When a batch returns a permanent error, the caller
// falls back to per-Actual Sync on that batch to isolate the bad one.
type BatchSyncable interface {
	Syncable
	SyncBatch(ctx context.Context, as []*Actual) (shouldSnapshot bool, err error)
}

// Parser will parse a viper file into a Syncable
//
//counterfeiter:generate . SyncableParser
type SyncableParser interface {
	Parse(*viper.Viper, DatabaseStorage) (Syncable, error)
}

var syncableType = registerSystemType(&Type{
	ID:      "0cd18065-a0e2-4c19-a4d6-f824f1898cb5",
	Name:    "InternalSyncableParser",
	Version: 1,
}, hiddenFromProposals)

func IsSyncable(id string) bool {
	return id == syncableType.ID
}

func NewUpsertSyncableEntity(c *Configuration) (*Entity, error) {
	bs, err := c.Marshal()
	if err != nil {
		return nil, err
	}

	return NewUpsertEntity(syncableType, []byte(c.ID), bs), nil
}

var syncableIndexType = registerSystemType(&Type{
	ID:      "ab972bba-83fe-4dea-9c5d-877645e8d21e",
	Name:    "InternalSyncableIndex",
	Version: 1,
}, hiddenFromProposals, syncableMetadata)

type SyncableIndex struct {
	ID    string
	Index uint64
}

func (i *SyncableIndex) Marshal() ([]byte, error) {
	li := &clusterpb.LogSyncableIndex{ID: i.ID, Index: i.Index}
	return proto.Marshal(li)
}

func (i *SyncableIndex) Unmarshal(bs []byte) error {
	li := &clusterpb.LogSyncableIndex{}
	err := proto.Unmarshal(bs, li)
	if err != nil {
		return err
	}

	i.ID = li.ID
	i.Index = li.Index

	return nil
}

func IsSyncableIndex(id string) bool {
	return id == syncableIndexType.ID
}

func NewUpsertSyncableIndexEntity(i *SyncableIndex) (*Entity, error) {
	bs, err := i.Marshal()
	if err != nil {
		return nil, err
	}

	return NewUpsertEntity(syncableIndexType, []byte(i.ID), bs), nil
}

var syncableDeadLetterType = registerSystemType(&Type{
	ID:      "5f3b6c8e-1d2a-4e7b-9c0f-2a8d6b4e1f93",
	Name:    "InternalSyncableDeadLetter",
	Version: 1,
}, hiddenFromProposals, syncableMetadata)

// SyncableDeadLetter records that a syncable gave up on and skipped (dead-
// lettered) the proposal at raft Index. It is proposed by the sync worker —
// either automatically when Sync returns cluster.ErrPermanent, or on an
// operator's DeadLetterStuckSyncable request for a worker wedged on a
// transient error — and applied deterministically on every node, so the
// record is durable and queryable cluster-wide rather than stranded on
// whichever node happened to be leader at failure time.
//
// TimestampUnixNano, Kind, and Message are stamped once by the proposer
// so apply writes identical bytes on every replica. Message is truncated
// by the proposer (see db.maxDeadLetterMessageBytes) to bound log growth.
type SyncableDeadLetter struct {
	ID                string
	Index             uint64
	TimestampUnixNano int64
	// Kind is why the proposal was skipped: "permanent" (Sync returned
	// cluster.ErrPermanent) or "manual" (an operator dead-lettered a
	// syncable wedged on a transient error).
	Kind    string
	Message string
}

func (d *SyncableDeadLetter) Marshal() ([]byte, error) {
	ld := &clusterpb.LogSyncableDeadLetter{
		ID:                d.ID,
		Index:             d.Index,
		TimestampUnixNano: d.TimestampUnixNano,
		Kind:              d.Kind,
		Message:           d.Message,
	}
	return proto.Marshal(ld)
}

func (d *SyncableDeadLetter) Unmarshal(bs []byte) error {
	ld := &clusterpb.LogSyncableDeadLetter{}
	if err := proto.Unmarshal(bs, ld); err != nil {
		return err
	}

	d.ID = ld.ID
	d.Index = ld.Index
	d.TimestampUnixNano = ld.TimestampUnixNano
	d.Kind = ld.Kind
	d.Message = ld.Message

	return nil
}

func IsSyncableDeadLetter(id string) bool {
	return id == syncableDeadLetterType.ID
}

// NewUpsertSyncableDeadLetterEntity wraps a SyncableDeadLetter as an
// upsert entity keyed by the syncable id. The apply handler derives the
// per-failure bbolt key (id + raft index) from the unmarshaled record,
// so the entity Key only needs to carry the syncable id.
func NewUpsertSyncableDeadLetterEntity(d *SyncableDeadLetter) (*Entity, error) {
	bs, err := d.Marshal()
	if err != nil {
		return nil, err
	}

	return NewUpsertEntity(syncableDeadLetterType, []byte(d.ID), bs), nil
}

// NewDeleteSyncableDeadLetterEntity clears the dead-letter record at a
// specific raft index (used by replay after a successful re-sync). The upsert
// path keys the entity by syncable id and reads the index from the record
// body, but a delete carries the delete sentinel in the body, so the index
// rides in the Key instead: id bytes followed by the 8-byte big-endian index.
// DecodeSyncableDeadLetterKey reverses it on the apply side.
func NewDeleteSyncableDeadLetterEntity(id string, index uint64) *Entity {
	key := make([]byte, len(id)+8)
	copy(key, id)
	binary.BigEndian.PutUint64(key[len(id):], index)
	return NewDeleteEntity(syncableDeadLetterType, key)
}

// DecodeSyncableDeadLetterKey reverses NewDeleteSyncableDeadLetterEntity's
// composite Key into (id, index). ok is false if the key is too short to
// carry an 8-byte index.
func DecodeSyncableDeadLetterKey(key []byte) (id string, index uint64, ok bool) {
	if len(key) < 8 {
		return "", 0, false
	}
	return string(key[:len(key)-8]), binary.BigEndian.Uint64(key[len(key)-8:]), true
}

var syncableStuckType = registerSystemType(&Type{
	ID:      "8a1c4d2e-7b3f-4a6c-9e8d-1f5b2c7a9d04",
	Name:    "InternalSyncableStuck",
	Version: 1,
}, hiddenFromProposals, syncableMetadata)

// SyncableStuck records that a syncable's worker is currently blocked
// retrying a transient error on the proposal at Index (for a batch syncable,
// the head of the wedged batch). The worker proposes it through Raft after a
// debounce window so every node can report the stall (GET status) and so an
// operator's skip request can be served from any node, not just the one
// running the worker. Upsert/delete keyed by the syncable id — one current
// value per syncable; the worker deletes it on progress.
type SyncableStuck struct {
	ID            string
	Index         uint64
	SinceUnixNano int64
	Message       string
}

func (s *SyncableStuck) Marshal() ([]byte, error) {
	ls := &clusterpb.LogSyncableStuck{
		ID:            s.ID,
		Index:         s.Index,
		SinceUnixNano: s.SinceUnixNano,
		Message:       s.Message,
	}
	return proto.Marshal(ls)
}

func (s *SyncableStuck) Unmarshal(bs []byte) error {
	ls := &clusterpb.LogSyncableStuck{}
	if err := proto.Unmarshal(bs, ls); err != nil {
		return err
	}
	s.ID = ls.ID
	s.Index = ls.Index
	s.SinceUnixNano = ls.SinceUnixNano
	s.Message = ls.Message
	return nil
}

func IsSyncableStuck(id string) bool {
	return id == syncableStuckType.ID
}

// NewUpsertSyncableStuckEntity wraps a SyncableStuck as an upsert entity
// keyed by the syncable id.
func NewUpsertSyncableStuckEntity(s *SyncableStuck) (*Entity, error) {
	bs, err := s.Marshal()
	if err != nil {
		return nil, err
	}
	return NewUpsertEntity(syncableStuckType, []byte(s.ID), bs), nil
}

// NewDeleteSyncableStuckEntity clears the stuck record for a syncable.
func NewDeleteSyncableStuckEntity(id string) *Entity {
	return NewDeleteEntity(syncableStuckType, []byte(id))
}

var syncableSkipRequestType = registerSystemType(&Type{
	ID:      "3d9e6b1a-5c2f-4d7b-8a0e-6f4c1b8d3e25",
	Name:    "InternalSyncableSkipRequest",
	Version: 1,
}, hiddenFromProposals, syncableMetadata)

// SyncableSkipRequest is the operator's request (proposed by the dead-letter
// endpoint from any node) for a syncable's worker to skip what it is blocked
// on. Index is copied from the matching SyncableStuck so the worker can only
// skip the exact proposal the operator saw. Upsert/delete keyed by syncable
// id; the worker deletes it once honored or drops it as stale.
type SyncableSkipRequest struct {
	ID    string
	Index uint64
}

func (r *SyncableSkipRequest) Marshal() ([]byte, error) {
	lr := &clusterpb.LogSyncableSkipRequest{ID: r.ID, Index: r.Index}
	return proto.Marshal(lr)
}

func (r *SyncableSkipRequest) Unmarshal(bs []byte) error {
	lr := &clusterpb.LogSyncableSkipRequest{}
	if err := proto.Unmarshal(bs, lr); err != nil {
		return err
	}
	r.ID = lr.ID
	r.Index = lr.Index
	return nil
}

func IsSyncableSkipRequest(id string) bool {
	return id == syncableSkipRequestType.ID
}

// NewUpsertSyncableSkipRequestEntity wraps a SyncableSkipRequest as an upsert
// entity keyed by the syncable id.
func NewUpsertSyncableSkipRequestEntity(r *SyncableSkipRequest) (*Entity, error) {
	bs, err := r.Marshal()
	if err != nil {
		return nil, err
	}
	return NewUpsertEntity(syncableSkipRequestType, []byte(r.ID), bs), nil
}

// NewDeleteSyncableSkipRequestEntity clears the skip request for a syncable.
func NewDeleteSyncableSkipRequestEntity(id string) *Entity {
	return NewDeleteEntity(syncableSkipRequestType, []byte(id))
}

// IsSyncableMetadata reports whether a type ID identifies a syncable's own
// internal coordination state — its progress index, dead-letter records, and
// stuck / skip-request status. These ride in the permanent event log like any
// other entity, but they must NOT be projected back into a syncable (a
// syncable re-Syncing its own dead letters would loop), so every event-log
// reader filters them out at read time. Derived from the type registry (the
// syncableMetadata flag), so a new such type is classified at its definition.
func IsSyncableMetadata(typeID string) bool {
	return systemTypes[typeID].syncableMeta
}
