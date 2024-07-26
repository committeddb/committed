package cluster

import (
	"google.golang.org/protobuf/proto"
)

var delete []byte = []byte("7ec589c2-3318-4a3c-839b-a9af9c9443be")

type Entity struct {
	*Type
	Key  []byte
	Data []byte
}

type Proposal struct {
	Entities []*Entity
}

func NewUpsertEntity(t *Type, key []byte, data []byte) *Entity {
	return &Entity{t, key, data}
}

func NewDeleteEntity(t *Type, key []byte) *Entity {
	return &Entity{t, key, delete}
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

func (p *Proposal) Marshal() ([]byte, error) {
	var es []*LogEntity
	for _, e := range p.Entities {
		es = append(es, &LogEntity{TypeID: e.Type.ID, Key: e.Key, Data: e.Data})
	}

	lp := &LogProposal{LogEntities: es}

	return proto.Marshal(lp)
}

func (p *Proposal) Unmarshal(bs []byte) error {
	lp := &LogProposal{}
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
