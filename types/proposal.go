package types

import (
	"bytes"
	"encoding/gob"
)

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
