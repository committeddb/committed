package syncable

import (
	"context"

	"github.com/philborlin/committed/internal/cluster"
)

type Slice struct {
	p []*cluster.Proposal
}

func (s *Slice) Sync(ctx context.Context, p *cluster.Proposal) error {
	s.p = append(s.p, p)
	return nil
}

func (s *Slice) Close() error {
	return nil
}
