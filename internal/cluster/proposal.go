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
}

type Entity struct {
	*Type
	Key  []byte
	Data []byte
}

func NewUpsertEntity(t *Type, key []byte, data []byte) *Entity {
	return &Entity{t, key, data}
}

func NewDeleteEntity(t *Type, key []byte) *Entity {
	return &Entity{t, key, delete}
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
		es = append(es, &clusterpb.LogEntity{TypeID: e.Type.ID, Key: e.Key, Data: e.Data})
	}

	lp := &clusterpb.LogProposal{LogEntities: es}

	return proto.Marshal(lp)
}

func (p *Proposal) Unmarshal(bs []byte) error {
	lp := &clusterpb.LogProposal{}
	err := proto.Unmarshal(bs, lp)
	if err != nil {
		return err
	}

	for _, e := range lp.LogEntities {
		// TODO Get the type from a map of types
		t := &Type{ID: e.TypeID}
		p.Entities = append(p.Entities, &Entity{Type: t, Key: e.Key, Data: e.Data})
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
	}

	return false
}
