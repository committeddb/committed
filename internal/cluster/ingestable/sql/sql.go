package sql

import (
	"context"

	"github.com/committeddb/committed/internal/cluster"
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

func (i *Ingestable) Status(ctx context.Context, pos cluster.Position) (cluster.IngestableStatus, error) {
	return i.dialect.Status(ctx, i.config, pos)
}

func (i *Ingestable) Close() error {
	return nil
}

// sourceTeardowner is the optional Dialect capability to drop the source-side
// replication resources it created. Postgres implements it (drop slot +
// publication); MySQL does not — a binlog reader holds no persistent server-side
// resource, so there is nothing to drop once the worker's connection closes.
type sourceTeardowner interface {
	TeardownSource(config *Config) error
}

// Teardown satisfies cluster.IngestableTeardownable: on ingestable delete the
// owner node drops the source-side replication resources so an orphaned slot
// can't pin the source's WAL. Delegates to the dialect when it owns such
// resources; a no-op otherwise. Idempotent, so a re-run after a flap is safe.
func (i *Ingestable) Teardown() error {
	if td, ok := i.dialect.(sourceTeardowner); ok {
		return td.TeardownSource(i.config)
	}
	return nil
}
