package db

import "github.com/philborlin/committed/internal/cluster"

// uint64 - index of the read
//
//counterfeiter:generate . ProposalReader
type ProposalReader interface {
	Read() (uint64, *cluster.Proposal, error)
}
