package syncable

import (
	"context"

	"github.com/philborlin/committed/internal/cluster"
	"go.uber.org/zap"
)

type Console struct{}

func (c *Console) Sync(ctx context.Context, p *cluster.Proposal) error {
	zap.L().Info("console syncable", zap.Int("entities", len(p.Entities)))
	return nil
}

func (c *Console) Close() error {
	return nil
}
