package cluster

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"

	"github.com/philborlin/committed/internal/cluster/clusterpb"
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

type ShouldSnapshot bool

// Syncable consumes proposals from the commit log and applies them to
// an external system (e.g., a SQL database, webhook, file).
//
// Contract:
//   - Sync MUST be idempotent. The same proposal may be delivered more
//     than once due to leader transition, worker replace, or process
//     restart. Implementations should use upsert or equivalent semantics.
//   - Sync errors are retried with exponential backoff. Wrap with
//     cluster.Permanent(err) to skip the proposal instead of retrying.
//   - Sync receives a context tied to the worker lifecycle; respect
//     ctx.Done() for cooperative shutdown.
//
//counterfeiter:generate . Syncable
type Syncable interface {
	Sync(ctx context.Context, p *Proposal) (ShouldSnapshot, error)
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
// that benefit from processing multiple proposals in a single transaction
// (e.g., SQL databases). db.sync checks for this interface at startup and
// uses SyncBatch when available, falling back to per-proposal Sync
// otherwise.
//
// SyncBatch receives a slice of proposals and returns the count of
// proposals that should be snapshotted (counted from the start of the
// slice). On success, the caller advances SyncableIndex to the last
// proposal in the batch.
//
// Error semantics match Syncable.Sync: wrap with cluster.Permanent(err)
// to skip proposals. When a batch returns a permanent error, the caller
// falls back to per-proposal Sync on that batch to isolate the bad
// proposal.
type BatchSyncable interface {
	Syncable
	SyncBatch(ctx context.Context, ps []*Proposal) (shouldSnapshot bool, err error)
}

// Parser will parse a viper file into a Syncable
//
//counterfeiter:generate . SyncableParser
type SyncableParser interface {
	Parse(*viper.Viper, DatabaseStorage) (Syncable, error)
}

var syncableType = &Type{
	ID:      "0cd18065-a0e2-4c19-a4d6-f824f1898cb5",
	Name:    "InternalSyncableParser",
	Version: 1,
}

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

var syncableIndexType = &Type{
	ID:      "ab972bba-83fe-4dea-9c5d-877645e8d21e",
	Name:    "InternalSyncableIndex",
	Version: 1,
}

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

var syncableDeadLetterType = &Type{
	ID:      "5f3b6c8e-1d2a-4e7b-9c0f-2a8d6b4e1f93",
	Name:    "InternalSyncableDeadLetter",
	Version: 1,
}

// SyncableDeadLetter records that a syncable permanently skipped (dead-
// lettered) the proposal at raft Index. It is proposed by the leader's
// sync worker when Sync returns cluster.ErrPermanent and applied
// deterministically on every node, so the dead-letter record is durable
// and queryable cluster-wide rather than stranded on whichever node
// happened to be leader at failure time.
//
// TimestampUnixNano, Kind, and Message are stamped once by the proposer
// so apply writes identical bytes on every replica. Message is truncated
// by the proposer (see db.maxDeadLetterMessageBytes) to bound log growth.
type SyncableDeadLetter struct {
	ID                string
	Index             uint64
	TimestampUnixNano int64
	Kind              string
	Message           string
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
