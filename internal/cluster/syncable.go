package cluster

import (
	"context"
)

//counterfeiter:generate . Syncable
type Syncable interface {
	Init(ctx context.Context) error
	Sync(ctx context.Context, p *Proposal) error
	Close() error
}
