package syncable

import (
	"context"

	"go.uber.org/zap"

	"github.com/philborlin/committed/internal/cluster"
)

type Console struct{}

func (c *Console) Sync(ctx context.Context, a *cluster.Actual) error {
	zap.L().Info("console syncable", zap.Int("entities", len(a.Entities)))
	return nil
}

func (c *Console) Close() error {
	return nil
}
