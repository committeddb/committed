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
}

type Entity struct {
	*Type
	Key  []byte
	Data []byte
}

func NewUpsertEntity(t *Type, key []byte, data []byte) *Entity {
	return &Entity{Type: t, Key: key, Data: data}
}

func NewDeleteEntity(t *Type, key []byte) *Entity {
	return &Entity{Type: t, Key: key, Data: delete}
}

func (e *Entity) IsDelete() bool {
	return string(e.Data) == string(delete)
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
		es = append(es, &clusterpb.LogEntity{
			Type: &clusterpb.TypeRef{
				ID: e.ID,
				// Type.Version is monotonically assigned by ProposeType
				// starting at 1. Fitting into uint32 is guaranteed by
				// the domain (no realistic system reaches 2^32 schema
				// versions).
				Version: uint32(e.Version), //nolint:gosec // G115: bounded by domain
			},
			Key:  e.Key,
			Data: e.Data,
		})
	}

	lp := &clusterpb.LogProposal{
		LogEntities:  es,
		RequestID:    p.RequestID,
		IngestableID: p.IngestableID,
		SourceSeq:    p.SourceSeq,
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
	for _, e := range lp.LogEntities {
		ref := TypeRef{ID: e.Type.GetID(), Version: int(e.Type.GetVersion())}
		t, err := resolveType(ref, r)
		if err != nil {
			return err
		}
		p.Entities = append(p.Entities, &Entity{
			Type: t,
			Key:  e.Key,
			Data: e.Data,
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
