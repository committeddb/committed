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

// RedactedError is an error that can supply a message safe to persist into the
// permanent, Raft-replicated dead-letter log — free of entity Key/Data and of
// values a driver may echo back (a Postgres foreign-key violation, for instance,
// includes "Key (col)=(value)"). recordSyncDeadLetter persists RedactedMessage()
// into the replicated record and keeps the full Error() node-local, so an
// operator can still find complete detail in the node's logs without the erasure
// target — or any other bound value — surviving in the log.
type RedactedError interface {
	error
	// RedactedMessage is the PII-free message to replicate. Error() may still
	// carry the full, possibly PII-bearing detail for node-local logging.
	RedactedMessage() string
}

// RedactedMessage returns the message safe to put into ANY persisted or exposed
// sink — a replicated dead-letter/stuck record, an HTTP body, or the node-status
// config-build-error list. If err (anywhere in its chain) is a RedactedError — a
// driver/migration error that may echo a bound value or connection identity —
// only its PII-free classifier is returned and ok is true; otherwise committed's
// own error text is returned verbatim (it is authored PII-free) and ok is false.
// The full Error() is meant to stay in this node's logs. This is the single
// redaction choke point every sink shares (safeDeadLetterMessage, redactedDetail,
// and the config-build-error recorder all route through it) so the contract can't
// drift between them. A nil err yields ("", false).
func RedactedMessage(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	if red, ok := errors.AsType[RedactedError](err); ok {
		return red.RedactedMessage(), true
	}
	return err.Error(), false
}

// ErrCorruptEntry marks a stored WAL entry that failed its CRC32C checksum on
// read, or a log that will not open. It is the corruption sentinel raised by the
// WAL layer (aliased there as wal.ErrCorruptEntry) and lives in this shared
// domain package so the sync worker can classify a corrupt read as fatal without
// importing the wal package (which imports db — an import cycle). A torn tail is
// repairable with `committed wal repair`; an in-record bitflip needs a rebuild
// from a healthy replica. See docs/operations/rebuild.md.
var ErrCorruptEntry = errors.New("wal: entry checksum mismatch (data corruption); see docs/operations/rebuild.md")

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

// ErrWorkerWedged is returned by RebuildSyncable when the syncable's local
// worker did not stop within the drain bound — typically wedged in an
// uninterruptible tx.Commit against an unreachable destination. The rebuild is
// aborted BEFORE the checkpoint reset (nothing changed): proceeding past a
// still-live worker would let its in-flight checkpoint bump land after the
// reset and silently defeat the replay. The HTTP layer renders it 503 — wait
// out (or fix) the destination and retry, or re-POST the config to replace the
// worker.
var ErrWorkerWedged = errors.New("syncable worker did not stop in time (wedged on its destination?)")

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

// SyncableSchemaComparable is a config's materialized destination schema, produced
// config-alone by SyncableSchemaExtractor.SchemaFromConfig (no database
// resolution). The config-change guard compares the two configs of a re-POST with
// SchemaChange: a non-nil result rejects the re-POST.
//
// It exists because some destinations can't absorb every config change in place: a
// SQL projection's table is created with CREATE TABLE IF NOT EXISTS and never
// ALTERed, so a re-POST that changes its columns or aggregate/lookup shape would
// silently no-op. SchemaChange returns a RebuildRequiredError to steer the operator
// to the rebuild verb. A destination that can take any change in place (e.g. an
// HTTP webhook) has no schema extractor. The generic layers never see the
// destination shape — only this interface and RebuildRequiredError.
type SyncableSchemaComparable interface {
	// SchemaChange reports whether replacing prior's config with the receiver's is
	// safe to apply in place, returning a RebuildRequiredError if not, nil if it
	// is. Comparing against a prior of a different, incomparable kind returns nil.
	SchemaChange(prior SyncableSchemaComparable) error
}

// RebuildRequiredError is returned by SyncableSchemaComparable.SchemaChange (and
// the ingestable/database config-change validators) when a config change can't be
// applied in place and needs a rebuild instead. The HTTP layer
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

// DependentSyncable identifies one syncable that consumes a topic affected by an
// ingestable change and so must be rebuilt after it. Id + name only — no
// destination shape — so it is safe to surface in a generic 409 details payload.
type DependentSyncable struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// SyncableTopicExtractor is the optional SyncableParser extension that reports
// which topics a syncable config consumes, read straight from the parsed config
// WITHOUT building or initializing the syncable (no DDL, no destination
// connection). The propose path uses it to find the syncables an ingestable
// primaryKey change re-keys (see IngestableConfigChangeValidator), so a rejected
// change can name exactly which syncables to rebuild. A parser that does not
// implement it contributes no dependents.
type SyncableTopicExtractor interface {
	TopicsFromConfig(v *ParsedConfig) []string
}

// SyncableDatabaseExtractor is the optional SyncableParser extension that reports
// which database configs a syncable references for its destination pool, read
// straight from the parsed config WITHOUT building the syncable (no Init, no
// destination connection). The propose path uses it to find the syncables a
// database connection change would break — they capture storage.Database(id) at
// build time and are not rebuilt when the database re-applies — so a rejected
// change can name exactly which syncables to tear down (see DatabaseInUseError).
// A parser that does not implement it contributes no dependents.
type SyncableDatabaseExtractor interface {
	DatabasesFromConfig(v *ParsedConfig) []string
}

// SyncableSchemaExtractor is the optional SyncableParser extension that builds a
// config's comparable destination schema from the config alone — WITHOUT resolving
// the config's database, so a missing database secret on this node cannot defeat
// the config-change guard. The destination schema is a pure function of the config
// document, not of the database connection. Returns (nil, nil) for a kind with no
// materialized schema (e.g. a webhook). s is used only for type resolution (e.g. a
// `when` discriminator shorthand), never for the database connection.
type SyncableSchemaExtractor interface {
	SchemaFromConfig(v *ParsedConfig, s DatabaseStorage) (SyncableSchemaComparable, error)
}

// DependentsAware is the optional RebuildRequiredError extension a destination
// error implements when it wants the propose path to enrich it with the
// syncables that consume the affected topic. The propose path owns the topology
// (which syncables consume which topic); the destination owns the error shape.
// This keeps topology out of the destination package and destination shape out
// of the propose path.
type DependentsAware interface {
	// AffectedTopic is the id of the topic whose entity keys the change
	// re-keys; the propose path finds the syncables consuming it.
	AffectedTopic() string
	// SetDependents hands back those syncables so the error can render them in
	// its Details (and message). Called at most once, before the error is
	// returned up the stack.
	SetDependents(dependents []DependentSyncable)
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

// EntityKindRevision (version-stored config with rollback, retained — see
// typeType).
var syncableType = registerSystemType(&Type{
	ID:         "0cd18065-a0e2-4c19-a4d6-f824f1898cb5",
	Name:       "InternalSyncableParser",
	Version:    1,
	EntityKind: EntityKindRevision,
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
	ID:         "ab972bba-83fe-4dea-9c5d-877645e8d21e",
	Name:       "InternalSyncableIndex",
	Version:    1,
	EntityKind: EntityKindSnapshot,
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

// EntityKindSnapshot: a dead letter is keyed by its full identity — syncable id
// + failed-proposal raft index (syncableDeadLetterKey) — in BOTH the upsert and
// the clearing delete, so each record is a distinct, symmetric event-log key.
// That makes the metadata-GC scrubber's keep-latest-per-key compaction correct:
// distinct dead letters never supersede each other, a re-propose keeps the
// newest, and a cleared one keeps its delete tombstone as the final state. (The
// bbolt record is still keyed id+index, derived from the body — unchanged.)
var syncableDeadLetterType = registerSystemType(&Type{
	ID:         "5f3b6c8e-1d2a-4e7b-9c0f-2a8d6b4e1f93",
	Name:       "InternalSyncableDeadLetter",
	Version:    1,
	EntityKind: EntityKindSnapshot,
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

// syncableDeadLetterKey encodes a dead letter's full identity — syncable id
// followed by the 8-byte big-endian failed-proposal raft index — and is the
// event-log entity Key for BOTH the upsert and the clearing delete, so each
// record is a single symmetric, unique key (what lets the scrubber compact them
// keep-latest-per-key). DecodeSyncableDeadLetterKey reverses it.
func syncableDeadLetterKey(id string, index uint64) []byte {
	key := make([]byte, len(id)+8)
	copy(key, id)
	binary.BigEndian.PutUint64(key[len(id):], index)
	return key
}

// NewUpsertSyncableDeadLetterEntity wraps a SyncableDeadLetter as an upsert
// entity keyed by its full identity (id + raft index — syncableDeadLetterKey),
// matching the clearing delete's key. The apply handler still derives the bbolt
// key from the unmarshaled record body, so storage is unchanged.
func NewUpsertSyncableDeadLetterEntity(d *SyncableDeadLetter) (*Entity, error) {
	bs, err := d.Marshal()
	if err != nil {
		return nil, err
	}

	return NewUpsertEntity(syncableDeadLetterType, syncableDeadLetterKey(d.ID, d.Index), bs), nil
}

// NewDeleteSyncableDeadLetterEntity clears the dead-letter record at a specific
// raft index (used by replay after a successful re-sync). It keys by the same
// id+index identity as the upsert (a delete carries the delete sentinel in the
// body, so the index must ride in the Key); DecodeSyncableDeadLetterKey reverses
// it on the apply side.
func NewDeleteSyncableDeadLetterEntity(id string, index uint64) *Entity {
	return NewDeleteEntity(syncableDeadLetterType, syncableDeadLetterKey(id, index))
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
	ID:         "8a1c4d2e-7b3f-4a6c-9e8d-1f5b2c7a9d04",
	Name:       "InternalSyncableStuck",
	Version:    1,
	EntityKind: EntityKindSnapshot,
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
	ID:         "3d9e6b1a-5c2f-4d7b-8a0e-6f4c1b8d3e25",
	Name:       "InternalSyncableSkipRequest",
	Version:    1,
	EntityKind: EntityKindSnapshot,
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
