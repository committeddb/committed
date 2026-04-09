package cluster

import (
	"fmt"
	"strings"

	"github.com/philborlin/committed/internal/cluster/clusterpb"
	"google.golang.org/protobuf/proto"
)

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

func (p *Proposal) Validate() error {
	for _, e := range p.Entities {
		if e.Validate == NoValidation {
			continue
		}
		// Find Validator for Schema Type
	}

	return nil
}

func (p *Proposal) String() string {
	var sb strings.Builder

	sb.WriteString("Proposal:\n")
	for i, e := range p.Entities {
		sb.WriteString(fmt.Sprintf("  [%d](%s) %v", i, e.Type.Name, string(e.Key)))
		if i < len(p.Entities)-1 {
			sb.WriteString("\n")
		}
	}

	return sb.String()
}

func (p *Proposal) Marshal() ([]byte, error) {
	var es []*clusterpb.LogEntity
	for _, e := range p.Entities {
		es = append(es, &clusterpb.LogEntity{
			TypeID:    e.Type.ID,
			Key:       e.Key,
			Data:      e.Data,
			Timestamp: e.Timestamp,
		})
	}

	lp := &clusterpb.LogProposal{LogEntities: es, RequestID: p.RequestID}

	return proto.Marshal(lp)
}

func (p *Proposal) Unmarshal(bs []byte) error {
	lp := &clusterpb.LogProposal{}
	err := proto.Unmarshal(bs, lp)
	if err != nil {
		return err
	}

	p.RequestID = lp.RequestID
	for _, e := range lp.LogEntities {
		// TODO Get the type from a map of types
		t := &Type{ID: e.TypeID}
		p.Entities = append(p.Entities, &Entity{
			Type:      t,
			Key:       e.Key,
			Data:      e.Data,
			Timestamp: e.Timestamp,
		})
	}

	return nil
}

func IsSystem(id string) bool {
	switch id {
	case syncableType.ID:
		return true
	case databaseType.ID:
		return true
	case typeType.ID:
		return true
	case syncableIndexType.ID:
		return true
	}

	return false
}
