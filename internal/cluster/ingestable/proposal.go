package ingestable

import (
	"context"

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

func (i *ProposalIngestable) Ingest(ctx context.Context, pos cluster.Position, pr chan<- *cluster.Proposal, po chan<- cluster.Position) error {
	for _, p := range i.ps {
		select {
		case <-ctx.Done():
			return nil
		default:
			pr <- p
		}
	}

	return nil
}

func (i *ProposalIngestable) Close() error {
	return nil
}
