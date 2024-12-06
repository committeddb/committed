package sql

import (
	"context"

	"github.com/philborlin/committed/internal/cluster"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

type Ingestable struct {
	config  *Config
	dialect Dialect
}

func New(d Dialect, config *Config) *Ingestable {
	return &Ingestable{config: config, dialect: d}
}

func (i *Ingestable) Ingest(ctx context.Context, pos cluster.Position, pr chan<- *cluster.Proposal, po chan<- cluster.Position) error {
	return i.dialect.Ingest(ctx, i.config, pos, pr, po)
}

func (i *Ingestable) Close() error {
	return nil
}
