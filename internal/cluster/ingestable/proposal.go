package ingestable

import (
	"context"

	"github.com/committeddb/committed/internal/cluster"
)

type ProposalIngestableParser struct {
	ps []*cluster.Proposal
}

func NewProposalIngestableParser(ps []*cluster.Proposal) cluster.IngestableParser {
	return &ProposalIngestableParser{ps: ps}
}

func (p *ProposalIngestableParser) Parse(*cluster.ParsedConfig) (cluster.Ingestable, error) {
	return &ProposalIngestable{ps: p.ps}, nil
}

type ProposalIngestable struct {
	ps []*cluster.Proposal
}

func (i *ProposalIngestable) Ingest(ctx context.Context, pos cluster.Position, pr chan<- *cluster.Proposal, po chan<- cluster.Position) error {
	for _, p := range i.ps {
		// The select must include the send case so that a ctx
		// cancellation while pr is full unblocks the goroutine.
		// The previous shape (ctx-or-default, then a bare send) would
		// happily proceed into a blocking send and then ignore
		// cancellation until the receiver eventually drained the channel
		// — meaning a canceled worker leaked until the consumer woke up.
		select {
		case <-ctx.Done():
			return nil
		case pr <- p:
		}
	}

	return nil
}

// Status reports a static status: a ProposalIngestable has no external source,
// snapshot, or CDC cursor — it emits a fixed set of proposals and finishes — so
// there is nothing to snapshot, no position to track, and no source lag to
// measure.
func (i *ProposalIngestable) Status(context.Context, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{Phase: "streaming"}, nil
}

func (i *ProposalIngestable) Close() error {
	return nil
}
