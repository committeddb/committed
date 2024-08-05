package db

import "github.com/philborlin/committed/internal/cluster"

//counterfeiter:generate . ProposalReader
type ProposalReader interface {
	Read() (*cluster.Proposal, error)
}
