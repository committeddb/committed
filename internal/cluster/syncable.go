package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

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
//
//   - Sync MUST be idempotent — including under REPLAY of an already-synced
//     range, not just one-at-a-time redelivery. The same Actual, and indeed
//     a whole contiguous run of Actuals the syncable already processed, may
//     be delivered again. Implementations should use upsert or equivalent
//     semantics. This is a hard requirement, not best-effort: the framework
//     does not deduplicate downstream for you.
//
//     Why a run, not just one: after each successful Sync the worker bumps a
//     durable SyncableIndex (the resume cursor) through raft. If a leader
//     flaps while that bump is in flight, the bump's log entry can be
//     truncated by the new leader before it commits — db.Propose returns
//     db.ErrProposalUnknown / db.ErrProposalLost and the worker, correctly,
//     does NOT advance. The persisted cursor stays behind the work already
//     done, so a worker replace or process restart re-reads from the stale
//     cursor and re-delivers every Actual between it and where the syncable
//     had actually reached. An UPSERT/dedup sink absorbs this harmlessly; a
//     non-idempotent sink (HTTP webhook, event stream, message queue with no
//     dedup key) emits DUPLICATE downstream events. The committed framework
//     does not prevent that today — sinks that cannot be made idempotent are
//     the operator's responsibility (wrap in an idempotency-key layer, a
//     dedup table, or a HEAD-before-POST check). A future opt-in two-phase
//     mechanism may bound the duplicate window; see
//     .claude-scratch/tickets/sync-two-phase-syncable.md.
//
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
//
//   - An Actual's entities are one atomic unit (one committed transaction);
//     apply them together (e.g. in one destination transaction).
//
//   - A syncable sees ONLY user-defined topic data — including data an
//     ingestable pulled in, which is written under user topic types.
//     committed's own internal entries (its type/database/syncable/ingestable
//     configs and all coordination records — progress indexes, dead-letters,
//     stuck/skip status, ingestable positions, scrub commands) are filtered
//     out before they reach Sync and never appear in the stream. A syncable
//     therefore cannot (and must not try to) observe committed's control
//     plane through the projection path; query the config read endpoints
//     (GET /type, /database, /syncable, and their version history) for that.
//
//   - Sync errors are retried with exponential backoff. Wrap with
//     cluster.Permanent(err) to skip the Actual instead of retrying.
//
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

// Teardownable is the optional Syncable extension implemented by syncables that
// own destructive teardown of their destination state (a SQL syncable drops its
// table). The delete and rebuild paths type-assert it (exactly like
// BatchSyncable) and, on the owner node only, call Teardown after the logical
// (consensus) act succeeds.
//
// Teardown must be idempotent (safe to re-run, e.g. after a leadership flap)
// and reconstructable from the persisted config alone, since the delete path
// parses the pre-delete config to a teardown handle just before removing it.
// It is a destructive side effect and therefore owner-gated and live-only —
// never run on a replaying or non-owner node. Syncables that own no external
// state do not implement it.
type Teardownable interface {
	Teardown() error
}

// ConfigChangeValidator is the optional Syncable extension that vets an
// in-place config replacement. The propose path calls ValidateReplace on the
// newly-parsed syncable, passing the syncable built from the currently-
// persisted config; a non-nil result rejects the re-POST.
//
// It exists because some destinations can't absorb every config change in
// place: a SQL projection's table is created with CREATE TABLE IF NOT EXISTS
// and never ALTERed, so a re-POST that changes its columns would silently
// no-op. Such a syncable returns a RebuildRequiredError to steer the operator
// to the rebuild verb. A syncable whose destination can take any change in
// place (e.g. an HTTP webhook) does not implement this interface. The validator
// is destination-specific; the generic layers never see the destination shape.
type ConfigChangeValidator interface {
	// ValidateReplace reports whether replacing prior's config with this
	// syncable's config is safe to apply in place, returning a
	// RebuildRequiredError (or other error) if not, nil if it is.
	ValidateReplace(prior Syncable) error
}

// RebuildRequiredError is returned by a ConfigChangeValidator when a config
// change can't be applied in place and needs a rebuild instead. The HTTP layer
// renders it as 409 with Code and Details — without knowing what kind of
// syncable (or destination) produced it, so destination-specific shapes
// (e.g. SQL table/column names) stay out of the generic layers.
type RebuildRequiredError interface {
	error
	// Code is the stable machine-readable error code a deploy pipeline branches
	// on (e.g. "schema_change_requires_rebuild").
	Code() string
	// Details is the structured, JSON-serializable payload describing the
	// change (e.g. the table and changed columns).
	Details() any
}

// CheckpointPolicy tunes how often a syncable worker persists its
// SyncableIndex checkpoint — the trade-off between the per-checkpoint raft
// round-trip and how many already-synced proposals a crash re-delivers.
//
//   - Every: persist the checkpoint once per Every successful syncs (the
//     shouldSnapshot=true boundaries). 1 (the default) checkpoints every
//     sync — today's behavior. A larger value THINS checkpoints: a crash
//     re-delivers up to Every already-synced proposals, so only raise it for
//     idempotent sinks; leave it at 1 for non-idempotent sinks like webhooks
//     (every duplicate is externally visible — see
//     sync-fail-fast-bump-tracking).
//   - MaxAge: an upper bound on how long a validated-but-unpersisted
//     checkpoint may sit before being flushed, independent of count. 0
//     disables the age bound — the worker still flushes on reaching Every and
//     whenever it catches up (reader EOF), so even an idle syncable never
//     lags its checkpoint forever.
//
// Cadence can only THIN which valid boundaries get persisted; it never
// persists a boundary the syncable didn't validate (shouldSnapshot=true means
// "everything through this index is durably committed downstream"), so it can
// never advance the index past un-synced data, and it cannot checkpoint finer
// than the syncable's own transaction granularity. For a BatchSyncable, Every
// doubles as the batch size and MaxAge as the batch-age flush.
type CheckpointPolicy struct {
	Every  int
	MaxAge time.Duration
}

// CheckpointConfigurable is the optional Syncable extension that carries a
// CheckpointPolicy. The worker type-asserts it (exactly like BatchSyncable);
// a syncable that doesn't implement it runs at the default cadence. The
// syncable parser stamps the policy from the [syncable] TOML onto the
// syncable's config, and the ModeAlwaysCurrent migration wrapper forwards it.
type CheckpointConfigurable interface {
	CheckpointPolicy() CheckpointPolicy
}

// ParseCheckpointPolicy reads the optional checkpoint-cadence fields from the
// common [syncable] TOML section — checkpointEvery (int >= 1) and
// checkpointMaxAge (a duration string like "500ms" or "1s"). Unset fields
// stay zero, which the worker resolves to the path-appropriate default
// (checkpoint-every-sync for single syncables, the batch limits for batch
// syncables). Returns an error on a malformed value so a typo surfaces at
// config-parse time rather than silently running the wrong cadence.
func ParseCheckpointPolicy(v *ParsedConfig) (CheckpointPolicy, error) {
	var p CheckpointPolicy
	if v.IsSet("syncable.checkpointEvery") {
		every := v.GetInt("syncable.checkpointEvery")
		if every < 1 {
			return CheckpointPolicy{}, fmt.Errorf("syncable.checkpointEvery must be >= 1, got %d", every)
		}
		p.Every = every
	}
	if s := v.GetString("syncable.checkpointMaxAge"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return CheckpointPolicy{}, fmt.Errorf("syncable.checkpointMaxAge %q: %w", s, err)
		}
		if d < 0 {
			return CheckpointPolicy{}, fmt.Errorf("syncable.checkpointMaxAge must be >= 0, got %s", d)
		}
		p.MaxAge = d
	}
	return p, nil
}

// SyncableParser parses a config document into a Syncable
//
//counterfeiter:generate . SyncableParser
type SyncableParser interface {
	Parse(c *ParsedConfig, s DatabaseStorage) (Syncable, error)
}

var syncableType = registerSystemType(&Type{
	ID:      "0cd18065-a0e2-4c19-a4d6-f824f1898cb5",
	Name:    "InternalSyncableParser",
	Version: 1,
})

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
})

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

// NewDeleteSyncableEntities builds the two tombstones that remove a syncable:
// its configuration and its checkpoint index. They are returned together so a
// single Proposal commits them as one Actual — config and checkpoint die
// atomically, leaving no window in which a later same-named syncable could
// resume from a stale checkpoint and come back un-backfilled. The keys mirror
// the upsert constructors (the syncable ID).
func NewDeleteSyncableEntities(id string) []*Entity {
	return []*Entity{
		NewDeleteEntity(syncableType, []byte(id)),
		NewDeleteEntity(syncableIndexType, []byte(id)),
	}
}

// NewDeleteSyncableIndexEntity builds the tombstone that resets a syncable's
// checkpoint to 0 (the Reader falls back to index 0 when the index entity is
// absent), without touching its config. It is the consensus half of a rebuild.
func NewDeleteSyncableIndexEntity(id string) *Entity {
	return NewDeleteEntity(syncableIndexType, []byte(id))
}

var syncableDeadLetterType = registerSystemType(&Type{
	ID:      "5f3b6c8e-1d2a-4e7b-9c0f-2a8d6b4e1f93",
	Name:    "InternalSyncableDeadLetter",
	Version: 1,
})

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
})

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
})

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
