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

// ValidateReplace implements cluster.IngestableConfigChangeValidator: it rejects
// a re-POST that changes this ingestable's primaryKey while a prior config
// exists, because the persisted snapshot Position is encoded under the old key
// and is inherited by ingestable id without re-checking it (see
// cluster.IngestableConfigChangeValidator). Returns a *PrimaryKeyChangeError (a
// cluster.RebuildRequiredError) describing the change, or nil when the key is
// unchanged. Fail-open if prior is not a SQL ingestable — a different
// dialect/kind has no comparable primaryKey.
//
// The guard is config-level, not runtime-level: it fires whenever a prior config
// exists, regardless of whether a snapshot has actually checkpointed a Position
// yet. A pre-checkpoint change is caught too because the initial snapshot may
// already have emitted rows to the sink under the old key before its first
// checkpoint, so "no persisted Position" does not mean "no old-key rows". This
// mirrors the syncable schema guard, which likewise rejects on any prior config.
func (i *Ingestable) ValidateReplace(prior cluster.Ingestable) error {
	p, ok := prior.(*Ingestable)
	if !ok {
		return nil // different ingestable kind — nothing to compare
	}
	if primaryKeyEqual(p.config.PrimaryKey, i.config.PrimaryKey) {
		return nil
	}
	return &PrimaryKeyChangeError{
		TopicID:       i.topicID(),
		TopicName:     i.topicName(),
		OldPrimaryKey: p.config.PrimaryKey,
		NewPrimaryKey: i.config.PrimaryKey,
	}
}

// topicID / topicName read the produced topic's identity, tolerating a nil Type
// (a config built without one) so ValidateReplace never panics on a malformed
// prior.
func (i *Ingestable) topicID() string {
	if i.config.Type == nil {
		return ""
	}
	return i.config.Type.ID
}

func (i *Ingestable) topicName() string {
	if i.config.Type == nil {
		return ""
	}
	return i.config.Type.Name
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
