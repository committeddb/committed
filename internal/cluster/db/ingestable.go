package db

import (
	"context"
	"errors"
	"slices"

	"github.com/committeddb/committed/internal/cluster"
)

type IngestableWithID struct {
	ID         string
	Ingestable cluster.Ingestable
	// Delete signals that the ingestable with ID was removed from the log
	// (deleteIngestable on the apply path), rather than upserted. The DB-layer
	// consumer (listenForIngestables) cancels the worker and, on the owner, tears
	// down the source-side replication resources (drops the Postgres slot +
	// publication). Ingestable is nil for a delete.
	Delete bool
	// ReconcileList: see SyncableWithID.ReconcileList — the ingest twin.
	ReconcileList func() ([]*IngestableWithID, error)
}

func (db *DB) AddIngestableParser(name string, p cluster.IngestableParser) {
	db.parser.AddIngestableParser(name, p)
}

func (db *DB) ProposeIngestable(ctx context.Context, c *cluster.Configuration) error {
	name, ingestable, err := db.ParseIngestable(c.MimeType, c.Data)
	if err != nil {
		return cluster.NewConfigError(err)
	}
	c.Name = name

	// Guard: a re-POST that changes the ingestable's primaryKey can't be applied
	// in place (the persisted snapshot Position is encoded under the old key and
	// is inherited by id) — reject it and steer the operator to delete + recreate
	// plus rebuilding the affected syncables, rather than silently duplicating and
	// orphaning rows. The ingestable itself decides (IngestableConfigChangeValidator);
	// this layer stays destination-agnostic. Returns a cluster.RebuildRequiredError
	// the HTTP layer renders as 409. Best-effort/fail-open — see the helper.
	if err := db.guardIngestableConfigChange(c.ID, ingestable); err != nil {
		return err
	}

	e, err := cluster.NewUpsertIngestableEntity(c)
	if err != nil {
		return err
	}

	p := &cluster.Proposal{Entities: []*cluster.Entity{e}}
	return db.Propose(ctx, p)
}

// guardIngestableConfigChange asks the newly-parsed ingestable whether replacing
// the currently-persisted config with it is safe to apply in place. The
// ingestable owns the decision (cluster.IngestableConfigChangeValidator) — this
// layer never inspects the source shape — and a SQL ingestable answers by
// comparing its primaryKey, so no query hits the source database.
//
// When the ingestable rejects the change with a cluster.DependentsAware error,
// this layer enriches it with the syncables that consume the affected topic (the
// topology it owns and the ingestable does not), so the 409 tells the operator
// exactly what to rebuild.
//
// Fail-open: if the new ingestable doesn't validate replacements (no
// IngestableConfigChangeValidator), there is no prior config, or the prior
// config can't be rebuilt on this node, the guard allows the POST. It is a
// signpost toward delete + recreate, not a correctness gate — blocking a deploy
// on an un-parseable old config would be worse than the corruption it guards
// against, which only recurs in that already-degraded case.
func (db *DB) guardIngestableConfigChange(id string, next cluster.Ingestable) error {
	validator, ok := next.(cluster.IngestableConfigChangeValidator)
	if !ok {
		return nil // ingestable absorbs any config change in place — nothing to guard
	}

	prior := db.currentIngestableConfig(id)
	if prior == nil {
		return nil // first POST for this id — nothing to compare against
	}

	_, priorIngestable, err := db.ParseIngestable(prior.MimeType, prior.Data)
	if err != nil {
		return nil // prior config not buildable on this node — fail open
	}
	defer func() { _ = priorIngestable.Close() }()

	err = validator.ValidateReplace(priorIngestable)
	if err == nil {
		return nil
	}

	// Enrich a dependents-aware rejection with the syncables that consume the
	// re-keyed topic — the topology this layer owns. Best-effort: enumeration
	// failure just leaves the dependents list empty (the topic is still named).
	var dependents cluster.DependentsAware
	if errors.As(err, &dependents) {
		dependents.SetDependents(db.syncablesConsumingTopic(dependents.AffectedTopic()))
	}
	return err
}

// syncablesConsumingTopic returns the persisted syncables that consume topic —
// the ones an ingestable primaryKey change on that topic re-keys, so a rejected
// change can tell the operator exactly what to rebuild. Topics are read from
// each syncable's config alone (no Init / no DDL / no destination I/O). Empty
// topic → no dependents. Best-effort: a syncable whose config won't parse is
// skipped rather than failing the whole enumeration (the topic is still named in
// the error), which can only undercount in an already-degraded case.
func (db *DB) syncablesConsumingTopic(topic string) []cluster.DependentSyncable {
	if topic == "" {
		return nil
	}
	cfgs, err := db.storage.Syncables()
	if err != nil {
		return nil
	}
	var deps []cluster.DependentSyncable
	for _, c := range cfgs {
		topics, err := db.parser.SyncableTopics(c.MimeType, c.Data)
		if err != nil {
			continue
		}
		if slices.Contains(topics, topic) {
			deps = append(deps, cluster.DependentSyncable{ID: c.ID, Name: c.Name})
		}
	}
	return deps
}

// currentIngestableConfig returns the currently-persisted ingestable
// configuration for id, or nil if there is none (or it can't be read). nil means
// "no prior version", which the config-change guard treats as "nothing to
// compare".
func (db *DB) currentIngestableConfig(id string) *cluster.Configuration {
	cfgs, err := db.storage.Ingestables()
	if err != nil {
		return nil
	}
	for _, c := range cfgs {
		if c.ID == id {
			return c
		}
	}
	return nil
}

func (db *DB) ParseIngestable(mimeType string, data []byte) (string, cluster.Ingestable, error) {
	return db.parser.ParseIngestable(mimeType, data)
}

// DeleteIngestable removes an ingestable: its config and checkpoint position are
// deleted atomically (one proposal), and on apply the owner node cancels the
// worker and tears down the source-side replication resources (drops the Postgres
// slot + publication) so an orphaned slot can't pin the source's WAL. Blocks
// until applied (Propose returns after the delete is durable). There is no
// keep-data option — dropping the slot is always the right thing on decommission.
func (db *DB) DeleteIngestable(ctx context.Context, id string) error {
	p := &cluster.Proposal{Entities: cluster.NewDeleteIngestableEntities(id)}
	return db.Propose(ctx, p)
}

func (db *DB) Ingestables() ([]*cluster.Configuration, error) {
	return db.storage.Ingestables()
}

func (db *DB) IngestableVersions(id string) ([]cluster.VersionInfo, error) {
	return db.storage.IngestableVersions(id)
}

func (db *DB) IngestableVersion(id string, version uint64) (*cluster.Configuration, error) {
	return db.storage.IngestableVersion(id, version)
}
