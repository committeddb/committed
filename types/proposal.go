package types

import (
	"bytes"
	"encoding/gob"
)

// Proposer provides a means to make proposals to the raft
//counterfeiter:generate . Proposer
type Proposer interface {
	Propose(proposal Proposal)
}

// Proposal is an proposal to the raft system
type Proposal struct {
	Topic    string
	Proposal []byte
}

// AcceptedProposal is a proposal accepted by the raft system
type AcceptedProposal struct {
	Topic string
	Index uint64
	Term  uint64
	Data  []byte
}

// NewAcceptedProposal decodes an AcceptedProposal from a byte slice
func NewAcceptedProposal(data []byte) (*AcceptedProposal, error) {
	var ap AcceptedProposal
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	if err := dec.Decode(&ap); err != nil {
		return nil, err
	}
	return &ap, nil
}

// Encode encodes the proposal to a byte array
func (p *AcceptedProposal) Encode() ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(p); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Index represents an Index plus a Term
type Index struct {
	Index uint64
	Term  uint64
}

func NewIndex(data []byte) (*Index, error) {
	var i Index
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	if err := dec.Decode(&i); err != nil {
		return nil, err
	}
	return &i, nil
}

// Encode encodes an Index
func (i *Index) Encode() []byte {
	var buf bytes.Buffer
	_ = gob.NewEncoder(&buf).Encode(i)
	return buf.Bytes()
}
