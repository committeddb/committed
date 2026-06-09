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

// systemType, IsSystem, and IsSyncableMetadata are all derived from
// systemTypes — the registry of built-in (non-user) entity types. Each
// built-in type registers itself once, at its definition, via
// registerSystemType, so nothing here is edited to add a type. That is what
// keeps "resolvable on the apply path" (systemType) and "hidden from the
// default Proposals() listing" (IsSystem) from drifting out of sync.
type sysType struct {
	typ          *Type
	hideFromList bool // excluded from the default Proposals() listing (IsSystem)
	syncableMeta bool // a syncable's own coordination state, filtered from projection (IsSyncableMetadata)
}

var systemTypes = map[string]sysType{}

type sysTypeOpt func(*sysType)

// hiddenFromProposals marks a built-in type as excluded from the default
// Proposals() listing — config and syncable coordination state, but not
// ingestable data, which is surfaced by design.
func hiddenFromProposals(s *sysType) { s.hideFromList = true }

// syncableMetadata marks a built-in type as a syncable's own coordination
// state (its index, dead letters, stuck/skip status), filtered from syncable
// projection so a syncable never re-Syncs its own bookkeeping.
func syncableMetadata(s *sysType) { s.syncableMeta = true }

// registerSystemType records a built-in type's identity and classification
// and returns it, so a type's var definition doubles as its registration:
//
//	var fooType = registerSystemType(&Type{...}, hiddenFromProposals)
//
// Called from package-var initialisers, which Go runs (after systemTypes is
// initialised, by dependency order) before any proposal is applied.
func registerSystemType(t *Type, opts ...sysTypeOpt) *Type {
	info := sysType{typ: t}
	for _, o := range opts {
		o(&info)
	}
	systemTypes[t.ID] = info
	return t
}

// systemType returns the package-var Type for a built-in meta-type ID, or nil
// for a user-defined type. Built-in types are hardcoded vars stored in no
// bucket, so the apply path resolves them here without a resolver lookup —
// without which it would fatal-crash the first time such an entity commits.
func systemType(id string) *Type {
	return systemTypes[id].typ
}

func IsSystem(id string) bool {
	return systemTypes[id].hideFromList
}
