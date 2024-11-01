package ingestable

import (
	"github.com/philborlin/committed/internal/cluster"
	"github.com/spf13/viper"
)

type ProposalIngestableParser struct {
	ps []*cluster.Proposal
}

func NewProposalIngestableParser(ps []*cluster.Proposal) cluster.IngestableParser {
	return &ProposalIngestableParser{ps: ps}
}

func (p *ProposalIngestableParser) Parse(*viper.Viper) (cluster.Ingestable, error) {
	return &ProposalIngestable{ps: p.ps}, nil
}

type ProposalIngestable struct {
	ps []*cluster.Proposal
}

func (i *ProposalIngestable) Ingest(pos cluster.Position) (<-chan *cluster.Proposal, <-chan cluster.Position, error) {
	proposalChan := make(chan *cluster.Proposal)
	positionChan := make(chan cluster.Position)

	go func() {
		for _, p := range i.ps {
			proposalChan <- p
		}
	}()

	return proposalChan, positionChan, nil
}

func (i *ProposalIngestable) Close() error {
	return nil
}
