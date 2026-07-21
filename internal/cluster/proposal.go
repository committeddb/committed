package cluster

import (
	"errors"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/clusterpb"
)

// ErrProposalTooLarge is returned when a marshaled proposal exceeds
// the configured size limit.
var ErrProposalTooLarge = errors.New("proposal exceeds configured size limit")

// ErrInsufficientStorage is returned from Propose when the node's disk-usage
// watcher has put the data directory into a write-rejecting state: at
// "critical" free space, user-data proposals are rejected (config changes and
// internal housekeeping still flow); at "full", every proposal is rejected
// (read-only mode) until disk space recovers. The HTTP layer maps it to 507
// Insufficient Storage. See internal/cluster/db/disk_watcher.go.
var ErrInsufficientStorage = errors.New("insufficient disk space to accept the proposal")

var delete []byte = []byte("7ec589c2-3318-4a3c-839b-a9af9c9443be")

// logEntityWireView is the encoding-agnostic read of a wire LogEntity: the
// same logical fields whether the entity was written as a typed body variant
// (the control envelope, >= 0.7.3-beta) or as the legacy flat fields
// (<= 0.7.2-beta). It is the single decode chokepoint — Proposal.Unmarshal and
// the scrub traversals (FilterProposalEntities, ForEachProposalEntity) all
// read the wire through it — so a new body variant is handled here once.
type logEntityWireView struct {
	key             []byte
	data            []byte
	generation      uint64
	refreshBoundary bool
	keepData        bool
}

// isDelete mirrors Entity.IsDelete on the wire view: the in-memory delete
// representation is the sentinel Data, which logEntityView synthesizes for the
// explicit Delete variant so both encodings answer uniformly.
func (v logEntityWireView) isDelete() bool {
	return string(v.data) == string(delete)
}

// logEntityView maps a wire LogEntity to its logical view. A set body variant
// wins; an unset body means the entity was written by a pre-envelope binary
// (<= 0.7.2-beta) and the legacy flat fields carry the payload.
//
// An entity carrying LogEntity-level wire tags this binary does not know is an
// ERROR, not a legacy entity: proto3 would otherwise drop the unknown tags
// into the legacy path and the entity would silently apply as empty. Two
// populations can trip it: a body variant from a NEWER release (the cluster
// feature level keeps one from being committed while an older member is
// present — this guard is the defense-in-depth behind that gate), and bytes
// from a data dir OLDER than the supported floor (0.7.2-beta; pre-v0.5 logs
// carry the long-removed Timestamp field 4 — see docs/api-compatibility.md).
// Unknown tags INSIDE a known variant's message are still fine (adding a
// field to LogRow stays add-only); only LogEntity-level unknowns trip this.
func logEntityView(le *clusterpb.LogEntity) (logEntityWireView, error) {
	if u := le.ProtoReflect().GetUnknown(); len(u) > 0 {
		return logEntityWireView{}, fmt.Errorf(
			"log entity of type %s carries wire fields this binary does not recognize: either a newer release wrote it (upgrade this node) or the data dir predates the supported floor of 0.7.2-beta (recreate it; see docs/api-compatibility.md)",
			le.Type.GetID())
	}
	switch b := le.GetBody().(type) {
	case *clusterpb.LogEntity_Row:
		return logEntityWireView{key: b.Row.GetKey(), data: b.Row.GetData(), generation: b.Row.GetGeneration()}, nil
	case *clusterpb.LogEntity_Delete:
		return logEntityWireView{
			key:        b.Delete.GetKey(),
			data:       delete,
			generation: b.Delete.GetGeneration(),
			keepData:   b.Delete.GetKeepData(),
		}, nil
	case *clusterpb.LogEntity_Refresh:
		return logEntityWireView{generation: b.Refresh.GetGeneration(), refreshBoundary: true}, nil
	default:
		return logEntityWireView{
			key:             le.GetKey(),
			data:            le.GetData(),
			generation:      le.GetGeneration(),
			refreshBoundary: le.GetRefreshBoundary(),
			// keepData has no legacy flat field — it shipped with the envelope.
		}, nil
	}
}

// Proposal is a TRANSACTION: its entities commit in one raft entry, and a
// sink applies the resulting Actual's entities in one destination transaction
// — if one entity syncs, they all do. That boundary is a system invariant,
// preserved end to end: a CDC source transaction becomes one proposal, a
// config delete-bundle rides one proposal, and consumers may rely on never
// observing a partial application. Nothing may subdivide a proposal,
// dead-letter part of one, or otherwise split the boundary — which is also
// why the proposal size cap is a hard rejection, never a split.
//
// KNOWN DEVIATION (tracked for redesign in the 0.8 series): the snapshot
// ingest path packs INDEPENDENT rows into one proposal to amortize the
// fsync-bound raft round-trip, minting a proposal that looks like a
// transaction but is not — and the system cannot tell. This is safe (atomic
// application of independent upserts strengthens, never breaks, a promise)
// but wrong: the counterfeit inherits transaction-sized blast radii — a
// poison row dead-letters its whole batch, and the size cap constrains an
// arbitrary packing rather than a real transaction. The correct shape is one
// row = one proposal with pipelined proposing to recover throughput.
type Proposal struct {
	Entities []*Entity
	// RequestID lets db.Propose's caller wait for *its* proposal to be
	// applied. db.DB sets this in db.Propose to a unique value before
	// marshaling, registers a waiter under the same ID, and the apply path
	// signals the waiter after the entity bucket writes succeed. A
	// RequestID of 0 means "no waiter" — used by system-internal proposers
	// (ingest worker, sync worker) that don't need read-after-write
	// semantics. Backward-compatible: old log entries unmarshal as 0.
	RequestID uint64

	// IngestableID and SourceSeq tag a proposal emitted by an ingest
	// worker. SourceSeq is the dialect's per-ingestable monotonic source
	// position (Postgres LSN, MySQL binlog file+offset) set on the
	// proposal by the Ingestable; IngestableID is stamped by the ingest
	// worker (which knows the registry id) before propose. They drive
	// effectively-once ingest: the apply path advances a per-ingestable
	// highwater to max(highwater, SourceSeq), and the leader's worker
	// skips re-emitted proposals (SourceSeq <= highwater) before they
	// enter raft. Zero/"" means "not an ingest proposal" — never deduped.
	IngestableID string
	SourceSeq    uint64

	// Position, when set, is the ingest resume checkpoint this proposal
	// establishes (the dialect's encoded position bytes). The apply path
	// persists it — via the same config-guarded put as a standalone position
	// entity, keyed by IngestableID — ATOMICALLY with this proposal's entities,
	// in one raft entry. Snapshot batches set it so a crash can never fall
	// between a committed batch and its checkpoint (the gap SourceSeq dedup can't
	// cover, since snapshot rows carry SourceSeq 0). Empty for non-ingest and
	// streaming proposals, which checkpoint out-of-band.
	Position Position
}

type Entity struct {
	*Type
	Key  []byte
	Data []byte
	// Generation is the ingest refresh epoch that stamped this entity — a
	// per-ingestable monotonic counter (from 1) the worker applies to every
	// emitted entity, snapshot and streaming alike. It drives reconciling
	// ("full-refresh") snapshots: a keyed sink writes it on upsert and, on a
	// RefreshBoundary marker at epoch G, sweeps any row still carrying a
	// generation < G — the rows a positive re-enumeration can never signal are
	// gone (deleted at the source in a lost change-data window). 0 means "not
	// stamped by a refresh-managed ingest" (direct user writes, config records,
	// pre-feature log entries) and is never swept.
	Generation uint64
	// RefreshBoundary marks this entity as a refresh-boundary MARKER, not row
	// data: it carries no Key/Data, and Generation is the epoch G at which its
	// topic was fully refreshed. See NewRefreshBoundaryEntity and IsRefreshBoundary.
	RefreshBoundary bool

	// KeepData marks a syncable-config delete tombstone whose operator asked to
	// preserve the destination data: the owner node skips the destination
	// teardown on apply. Carried ON THE ENTITY so every node applies the same
	// intent deterministically, wherever leadership sits when the delete
	// applies. See NewDeleteSyncableEntities.
	KeepData bool
}

func NewUpsertEntity(t *Type, key []byte, data []byte) *Entity {
	return &Entity{Type: t, Key: key, Data: data}
}

func NewDeleteEntity(t *Type, key []byte) *Entity {
	return &Entity{Type: t, Key: key, Data: delete}
}

// NewRefreshBoundaryEntity builds the marker an ingest worker emits once per
// topic at the end of an all-tables full refresh: it names the topic (via t)
// and the epoch G the refresh reached, and instructs a keyed sink to sweep
// every row of that topic whose generation is < G. It carries no Key/Data — it
// is a control entity, not a row — so IsDelete is false for it and a sink that
// does not implement reconciliation applies it as a no-op. It must ride only
// on an all-tables refresh, never a partial added-table backfill (which would
// sweep the topic's sibling tables). epoch MUST be >= 1.
func NewRefreshBoundaryEntity(t *Type, epoch uint64) *Entity {
	return &Entity{Type: t, RefreshBoundary: true, Generation: epoch}
}

func (e *Entity) IsDelete() bool {
	return string(e.Data) == string(delete)
}

// IsRefreshBoundary reports whether this entity is a refresh-boundary marker
// (see NewRefreshBoundaryEntity) rather than row data. A marker and a delete
// are mutually exclusive — a marker carries no Data, so IsDelete is false.
// Single-property checks (is this a delete? is this a marker?) may use these
// accessors; a consumer choosing how to APPLY an entity must switch on
// Variant() instead, so the precedence lives in one place and a future
// variant lands in its default case rather than being misapplied.
func (e *Entity) IsRefreshBoundary() bool {
	return e.RefreshBoundary
}

// EntityVariant classifies an Entity into exactly one of the typed control
// envelope's body variants (clusterpb.LogEntity's oneof): a row datum, a
// delete tombstone, or a refresh-boundary marker.
type EntityVariant int

const (
	EntityVariantRow EntityVariant = iota
	EntityVariantDelete
	EntityVariantRefresh
)

func (v EntityVariant) String() string {
	switch v {
	case EntityVariantRow:
		return "row"
	case EntityVariantDelete:
		return "delete"
	case EntityVariantRefresh:
		return "refresh"
	default:
		return fmt.Sprintf("unknown(%d)", int(v))
	}
}

// Variant resolves the Entity flag-union into its single logical variant,
// centralizing the precedence (refresh before delete before row) that every
// consumer previously re-implemented by convention as an order-sensitive
// accessor ritual. Consumers switch on it exhaustively; the switch's default
// case is where a variant this consumer does not handle surfaces explicitly
// (a loud error / dead-letter) instead of being misapplied as a row.
func (e *Entity) Variant() EntityVariant {
	switch {
	case e.RefreshBoundary:
		return EntityVariantRefresh
	case e.IsDelete():
		return EntityVariantDelete
	default:
		return EntityVariantRow
	}
}

func (p *Proposal) String() string {
	var sb strings.Builder

	sb.WriteString("Proposal:\n")
	for i, e := range p.Entities {
		fmt.Fprintf(&sb, "  [%d](%s) %v", i, e.Name, string(e.Key))
		if i < len(p.Entities)-1 {
			sb.WriteString("\n")
		}
	}

	return sb.String()
}

func (p *Proposal) Marshal() ([]byte, error) {
	es := make([]*clusterpb.LogEntity, 0, len(p.Entities))
	for _, e := range p.Entities {
		le := &clusterpb.LogEntity{
			Type: &clusterpb.TypeRef{
				ID: e.ID,
				// Type.Version is monotonically assigned by ProposeType
				// starting at 1. Fitting into uint32 is guaranteed by
				// the domain (no realistic system reaches 2^32 schema
				// versions).
				Version: uint32(e.Version), //nolint:gosec // G115: bounded by domain
			},
		}
		// Every entity is encoded as exactly one body variant — the typed
		// control envelope. The legacy flat fields are decode-only (see
		// unmarshal); the delete sentinel exists only in memory, never on the
		// wire (the Delete variant is explicit).
		switch e.Variant() {
		case EntityVariantRefresh:
			le.Body = &clusterpb.LogEntity_Refresh{Refresh: &clusterpb.LogRefresh{Generation: e.Generation}}
		case EntityVariantDelete:
			le.Body = &clusterpb.LogEntity_Delete{Delete: &clusterpb.LogDelete{
				Key:        e.Key,
				Generation: e.Generation,
				KeepData:   e.KeepData,
			}}
		default:
			le.Body = &clusterpb.LogEntity_Row{Row: &clusterpb.LogRow{
				Key:        e.Key,
				Data:       e.Data,
				Generation: e.Generation,
			}}
		}
		es = append(es, le)
	}

	lp := &clusterpb.LogProposal{
		LogEntities:  es,
		RequestID:    p.RequestID,
		IngestableID: p.IngestableID,
		SourceSeq:    p.SourceSeq,
		Position:     p.Position,
	}

	return proto.Marshal(lp)
}

// RequestIDFromProposal decodes only the RequestID from a marshaled
// Proposal, without resolving entity types. Truncation detection in
// db/wal uses it to identify the waiters behind uncommitted entries a
// higher-term leader is about to overwrite: those entries may reference
// types not yet applied on this node, so the type-resolving Unmarshal
// path could spuriously fail on a perfectly decodable RequestID. A
// config-change or other non-Proposal payload that happens to decode
// (proto is lenient about unknown wire data) yields 0, which the caller
// treats as "no waiter".
func RequestIDFromProposal(bs []byte) (uint64, error) {
	lp := &clusterpb.LogProposal{}
	if err := proto.Unmarshal(bs, lp); err != nil {
		return 0, err
	}
	return lp.RequestID, nil
}

// FirstEntityTypeID decodes a marshaled Proposal and returns the type ID of
// its first entity, plus whether the proposal had one, WITHOUT resolving
// types. Recovery/scan paths in db/wal use it to classify an entry — data
// vs. syncable-metadata — when they only need the type ID and must not depend
// on the entity's type being resolvable (a type may have been scrubbed, or
// bbolt may sit at a different point on an rsync-restored node, either of
// which would make the type-resolving Unmarshal fail on a perfectly readable
// classification). Returns ("", false, nil) for a proposal with no entities.
func FirstEntityTypeID(bs []byte) (string, bool, error) {
	lp := &clusterpb.LogProposal{}
	if err := proto.Unmarshal(bs, lp); err != nil {
		return "", false, err
	}
	if len(lp.LogEntities) == 0 || lp.LogEntities[0].Type == nil {
		return "", false, nil
	}
	return lp.LogEntities[0].Type.GetID(), true, nil
}

// Unmarshal decodes a marshaled Proposal and hydrates each entity's Type
// via the resolver. The resolver is required: every production call site
// has one, and a nil resolver indicates a programming bug. Type lookup
// failures are returned — they indicate log/state inconsistency (the
// type entry should always precede entity entries in the log) and must
// not be silently hidden behind stub Types.
func (p *Proposal) Unmarshal(bs []byte, r TypeResolver) error {
	if r == nil {
		return fmt.Errorf("Proposal.Unmarshal: TypeResolver is required")
	}

	lp := &clusterpb.LogProposal{}
	if err := proto.Unmarshal(bs, lp); err != nil {
		return err
	}

	p.RequestID = lp.RequestID
	p.IngestableID = lp.IngestableID
	p.SourceSeq = lp.SourceSeq
	p.Position = lp.Position
	for _, e := range lp.LogEntities {
		ref := TypeRef{ID: e.Type.GetID(), Version: int(e.Type.GetVersion())}
		t, err := resolveType(ref, r)
		if err != nil {
			return err
		}
		v, err := logEntityView(e)
		if err != nil {
			return err
		}
		p.Entities = append(p.Entities, &Entity{
			Type:            t,
			Key:             v.key,
			Data:            v.data,
			Generation:      v.generation,
			RefreshBoundary: v.refreshBoundary,
			KeepData:        v.keepData,
		})
	}

	return nil
}

// resolveType returns the Type at the referenced version via the
// resolver, with one short-circuit: built-in meta-types (the ones that
// wrap Type/Database/Syncable/SyncableIndex/Ingestable config entries)
// are hardcoded package vars not stored anywhere, so we return them
// directly without calling the resolver. Lookup failures on
// user-defined types propagate: a type referenced by a log entry that
// storage doesn't know about is a consistency violation.
func resolveType(ref TypeRef, r TypeResolver) (*Type, error) {
	if t := systemType(ref.ID); t != nil {
		return t, nil
	}
	return r.ResolveType(ref)
}

// systemTypes is the single registry of built-in (committed-internal) entity
// types — every config (type, database, syncable, ingestable) and every
// coordination record (syncable index / dead-letters / stuck / skip,
// ingestable position, scrub, node-API-URL, migration dead-letters). Each
// built-in registers itself once, at its definition, via registerSystemType,
// so adding a built-in is one line at its definition and nothing here changes.
//
// It is the one source of truth for the two questions the rest of the system
// asks about a type: "is it resolvable on the apply path without a storage
// lookup?" (systemType) and "is it committed-internal rather than user topic
// data?" (IsInternal). Those used to be split across this registry plus
// hide-from-listing / syncable-metadata flags and a parallel wal dispatch
// table; they are now all derived from membership here.
var systemTypes = map[string]*Type{}

// registerSystemType records a built-in type and returns it, so a type's var
// definition doubles as its registration:
//
//	var fooType = registerSystemType(&Type{...})
//
// Called from package-var initialisers, which Go runs (after systemTypes is
// initialised, by dependency order) before any proposal is applied.
func registerSystemType(t *Type) *Type {
	systemTypes[t.ID] = t
	return t
}

// systemType returns the package-var Type for a built-in type ID, or nil for a
// user-defined type. Built-in types are hardcoded vars stored in no bucket, so
// the apply path resolves them here without a resolver lookup — without which
// it would fatal-crash the first time such an entity commits.
func systemType(id string) *Type {
	return systemTypes[id]
}

// IsInternal reports whether id identifies a built-in committed type — any
// config (type, database, syncable, ingestable) or coordination record
// (syncable index / dead-letters / stuck / skip, ingestable position, scrub,
// node-API-URL, migration dead-letters) — as opposed to a user-defined topic
// type. It is membership in systemTypes: the single line between committed's
// control plane and user data.
//
// The per-syncable event-log reader skips IsInternal entries so a syncable
// only ever sees user topic data — including ingested data, which is written
// under user-defined topic types, not the internal ingestable type. This keeps
// committed's control plane (its configs and coordination) out of the
// data-projection stream, which is otherwise an out-of-band read path.
func IsInternal(id string) bool {
	return systemType(id) != nil
}

// IsSystemTombstonable reports whether the event-log scrubber's system-tombstone
// (metadata GC) pass may compact this type's superseded entries — i.e. drop all
// but the latest committed entry per key. That is sound only for last-writer-wins
// (EntityKindSnapshot) types, where a later write fully implies the earlier one;
// for an event/standalone/command stream each entry carries information no later
// entry implies, so none are compactable (only an RTBF "user tombstone" removes
// those, regardless of kind).
//
// This predicate currently covers committed-internal metadata only: it is true
// for a built-in iff that built-in is EntityKindSnapshot. Every system type is
// Snapshot today (see TestSystemTypesAreSnapshotKind), but the kind is checked
// rather than assumed so a future non-LWW internal type (e.g. an append-only
// audit log) is excluded automatically. The follow-up
// (compact-user-snapshot-streams) relaxes the internal restriction to also cover
// user-defined EntityKindSnapshot types, which requires a TypeResolver.
func IsSystemTombstonable(id string) bool {
	t := systemType(id)
	return t != nil && t.EntityKind == EntityKindSnapshot
}
