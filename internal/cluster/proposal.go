package cluster

import (
	"bytes"
	"encoding/gob"
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

func (p *Proposal) convert() *LogProposal {
	var es []*LogEntity
	for _, e := range p.Entities {
		es = append(es, &LogEntity{TypeID: e.Type.ID, Key: e.Key, Data: e.Data})
	}

	return &LogProposal{LogEntities: es}
}

type LogProposal struct {
	LogEntities []*LogEntity
}

type LogEntity struct {
	TypeID string
	Key    []byte
	Data   []byte
}

func Marshal(p *LogProposal) ([]byte, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(p)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// TODO Write a test and unmarshal StateAppendProposal and then convert into Proposal
// func Unmarshal(b []byte) (*Proposal, error) {
// 	var p *Proposal
// 	var buffer bytes.Buffer
// 	dec := gob.NewDecoder(&buffer)
// 	err := dec.Decode(p)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return p, nil
// }
