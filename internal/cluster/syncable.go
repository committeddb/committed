package cluster

import (
	"context"
)

//counterfeiter:generate . Syncable
type Syncable interface {
	Sync(ctx context.Context, p *Proposal) error
	Close() error
}
