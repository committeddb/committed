package syncable

import (
	"context"
	"fmt"

	"github.com/philborlin/committed/internal/cluster"
)

type Console struct{}

func (c *Console) Sync(ctx context.Context, p *cluster.Proposal) error {
	fmt.Printf("[console syncable] %v\n", p)
	return nil
}

func (c *Console) Close() error {
	return nil
}
