package sql

import (
	"context"

	"github.com/committeddb/committed/internal/cluster"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

// TopicEpochReader supplies the delete-surviving per-topic refresh-epoch floor —
// the highest generation ever committed for a topic, which the sink still
// carries even after the ingestable's position is cleared by a DeleteIngestable.
// The worker reads it at snapshot start so a same-topic recreate stamps above
// those rows instead of resetting to epoch 1. Backed by the wal storage's
// TopicRefreshEpoch (via the db); nil in tests degrades to floor 0 (the
// pre-feature first-snapshot-at-epoch-1 behavior).
type TopicEpochReader interface {
	TopicRefreshEpoch(topic string) uint64
}

type Ingestable struct {
	config     *Config
	dialect    Dialect
	epochFloor TopicEpochReader
}

func New(d Dialect, config *Config) *Ingestable {
	return &Ingestable{config: config, dialect: d}
}

// WithEpochFloor injects the per-topic refresh-epoch reader. Set by the parser
// from the db; left nil by tests that construct an Ingestable directly.
func (i *Ingestable) WithEpochFloor(r TopicEpochReader) *Ingestable {
	i.epochFloor = r
	return i
}

func (i *Ingestable) Ingest(ctx context.Context, pos cluster.Position, pr chan<- *cluster.Proposal, po chan<- cluster.Position) error {
	var floor uint64
	if i.epochFloor != nil && i.config.Type != nil {
		floor = i.epochFloor.TopicRefreshEpoch(i.config.Type.ID)
	}
	return i.dialect.Ingest(ctx, i.config, pos, floor, pr, po)
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
	// A primaryKey change re-keys every row; keep the dedicated error, which
	// carries the dependent-syncable enumeration a re-key needs.
	if !primaryKeyEqual(p.config.PrimaryKey, i.config.PrimaryKey) {
		return &PrimaryKeyChangeError{
			TopicID:       i.topicID(),
			TopicName:     i.topicName(),
			OldPrimaryKey: p.config.PrimaryKey,
			NewPrimaryKey: i.config.PrimaryKey,
		}
	}
	// Source-identity changes leave the persisted Position stale for the new
	// source or the new topic (see SourceIdentityChangeError for what is and is
	// not flagged, and why).
	var changed []string
	if p.topicID() != i.topicID() {
		changed = append(changed, "topic")
	}
	if serverIdentityChanged(p.config.ConnectionString, i.config.ConnectionString) {
		changed = append(changed, "connectionString")
	}
	if len(changed) > 0 {
		return &SourceIdentityChangeError{
			TopicID:       i.topicID(),
			TopicName:     i.topicName(),
			ChangedFields: changed,
		}
	}
	// The table set is identity too: removing a table in place arms a delayed
	// sweep of its sink rows at the next full refresh (see TableRemovalError).
	// Additions and reorders pass.
	if removed := removedTables(p.config.Tables, i.config.Tables); len(removed) > 0 {
		return &TableRemovalError{
			TopicID:       i.topicID(),
			TopicName:     i.topicName(),
			RemovedTables: removed,
		}
	}
	return nil
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
