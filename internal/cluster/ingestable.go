package cluster

import "context"

//counterfeiter:generate . Ingestable
type Ingestable interface {
	Ingest(ctx context.Context) (ShouldSnapshot, *Proposal, error)
	Close() error
}
