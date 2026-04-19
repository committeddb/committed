package cluster

import (
	"errors"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/philborlin/committed/internal/cluster/clusterpb"
)

// ErrProposalTooLarge is returned when a marshaled proposal exceeds
// the configured size limit.
var ErrProposalTooLarge = errors.New("proposal exceeds configured size limit")

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
}

type Entity struct {
	*Type
	Key  []byte
	Data []byte
	// Timestamp is the wall-clock time (unix milliseconds) at which the
	// proposer recorded this entity. Set once by the propose path so the
	// apply path can write a content-deterministic time-series row on
	// every node. Zero means "unset" and triggers a wall-clock fallback
	// at apply time (only relevant for pre-PR4 entries — every new
	// propose path sets this). See clusterpb.LogEntity.Timestamp.
	Timestamp int64
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
			Key:       e.Key,
			Data:      e.Data,
			Timestamp: e.Timestamp,
		})
	}

	lp := &clusterpb.LogProposal{LogEntities: es, RequestID: p.RequestID}

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
	for _, e := range lp.LogEntities {
		ref := TypeRef{ID: e.Type.GetID(), Version: int(e.Type.GetVersion())}
		t, err := resolveType(ref, r)
		if err != nil {
			return err
		}
		p.Entities = append(p.Entities, &Entity{
			Type:      t,
			Key:       e.Key,
			Data:      e.Data,
			Timestamp: e.Timestamp,
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

// systemType returns the package-var Type for built-in meta-type IDs,
// or nil for anything else. IsSystem delegates to this so the two never
// drift. Ingestable is included here so the apply path can resolve its
// Type; IsSystem keeps its original four-element scope (Proposals
// excludes system config but includes ingestable data by design).
func systemType(id string) *Type {
	switch id {
	case typeType.ID:
		return typeType
	case databaseType.ID:
		return databaseType
	case syncableType.ID:
		return syncableType
	case syncableIndexType.ID:
		return syncableIndexType
	case ingestableType.ID:
		return ingestableType
	}
	return nil
}

func IsSystem(id string) bool {
	switch id {
	case syncableType.ID, databaseType.ID, typeType.ID, syncableIndexType.ID:
		return true
	}
	return false
}
