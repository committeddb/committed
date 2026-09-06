// Package cluster is a stub of the real domain package: just the redaction
// contract and the entity types the analyzer keys on.
package cluster

import "errors"

type RedactedError interface {
	error
	RedactedMessage() string
}

func RedactedMessage(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	var red RedactedError
	if errors.As(err, &red) {
		return red.RedactedMessage(), true
	}
	return err.Error(), false
}

type Entity struct {
	Name string
	Key  []byte
	Data []byte
}

type Proposal struct{ Entities []*Entity }

func (p *Proposal) String() string { return "Proposal" }

type Actual struct {
	Index    uint64
	Entities []*Entity
}

func (a *Actual) String() string { return "Actual" }
