package migration

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/metrics"
)

// errTypeUnavailable marks a migration failure caused by the topic's latest
// type not being resolvable (see migrateEntities). It is schema/timing-shaped —
// it fails every entity of the topic identically, so the Sync wrappers classify
// it TRANSIENT, not cluster.Permanent.
var errTypeUnavailable = errors.New("migration: latest type unavailable")

// Wrap returns a cluster.Syncable that transforms each proposal's
// user-data entities through the migration chain from their stamped
// version up to the current latest before handing them to inner. If
// inner implements cluster.BatchSyncable, the returned syncable does
// too — the batch optimization is preserved.
//
// Wrap is the wal-layer hook for ModeAlwaysCurrent syncables. The
// rest of the system (db.Sync, the worker loop, tests that don't care
// about migration) sees a plain Syncable with the usual contract.
// Migration failures are classified per the egress rule: a type-unavailable
// failure (schema/timing-shaped, every entity of the topic alike) stays
// TRANSIENT so the worker wedges until the type resolves; a migration-program
// failure on an entity is reported cluster.Permanent so the worker dead-letters
// that proposal and moves on.
//
// m drives the committed.type.migration.duration histogram (recorded per
// migrated entity, on success). Nil when metrics are disabled.
func Wrap(inner cluster.Syncable, r Resolver, m *metrics.Metrics) cluster.Syncable {
	if bs, ok := inner.(cluster.BatchSyncable); ok {
		return &batchSyncable{single: single{inner: inner, resolver: r, metrics: m}, batch: bs}
	}
	return &single{inner: inner, resolver: r, metrics: m}
}

type single struct {
	inner    cluster.Syncable
	resolver Resolver
	metrics  *metrics.Metrics
}

func (s *single) Sync(ctx context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	entities, err := migrateEntities(ctx, s.resolver, s.metrics, a.Entities)
	if err != nil {
		if ctx.Err() != nil {
			// The worker ctx was cancelled mid-migration (shutdown / replace) — a
			// transient interruption, not a bad program. Return it unwrapped so the
			// worker retries this entity on restart rather than dead-lettering
			// (permanently skipping) a valid proposal. A per-run TIMEOUT leaves the
			// parent ctx live, so it still falls through to Permanent below.
			return false, ctx.Err()
		}
		if errors.Is(err, errTypeUnavailable) {
			// The topic's latest type isn't resolvable → every entity fails alike →
			// TRANSIENT (wedge until it's available), not a per-entity dead-letter.
			return false, err
		}
		return false, cluster.Permanent(err)
	}
	return s.inner.Sync(ctx, &cluster.Actual{Index: a.Index, Entities: entities})
}

func (s *single) Close() error { return s.inner.Close() }

// CheckpointPolicy forwards the wrapped syncable's checkpoint cadence so a
// ModeAlwaysCurrent syncable keeps the cadence parsed from its TOML — the
// worker only sees this wrapper, so without the forward the policy would be
// lost and the syncable would silently run at the default cadence. A wrapped
// syncable that doesn't configure cadence yields the zero policy, which the
// worker resolves to its default. batchSyncable embeds single, so it inherits
// this. See cluster.CheckpointConfigurable.
func (s *single) CheckpointPolicy() cluster.CheckpointPolicy {
	if cc, ok := s.inner.(cluster.CheckpointConfigurable); ok {
		return cc.CheckpointPolicy()
	}
	return cluster.CheckpointPolicy{}
}

type batchSyncable struct {
	single
	batch cluster.BatchSyncable
}

func (b *batchSyncable) SyncBatch(ctx context.Context, as []*cluster.Actual) (bool, error) {
	migrated := make([]*cluster.Actual, len(as))
	for i, a := range as {
		entities, err := migrateEntities(ctx, b.resolver, b.metrics, a.Entities)
		if err != nil {
			if ctx.Err() != nil {
				return false, ctx.Err() // shutdown/replace mid-migration — retry, don't dead-letter
			}
			if errors.Is(err, errTypeUnavailable) {
				return false, err // topic's latest type unavailable → transient, wedge
			}
			return false, cluster.Permanent(err)
		}
		migrated[i] = &cluster.Actual{Index: a.Index, Entities: entities}
	}
	return b.batch.SyncBatch(ctx, migrated)
}

// migrateEntities returns a copy of es with every user-data entity's Data
// run through the migration chain up to the current latest type version.
// System entities (config entries) pass through untouched. The input
// entities are not modified — retry paths see consistent input across
// attempts.
func migrateEntities(ctx context.Context, r Resolver, m *metrics.Metrics, es []*cluster.Entity) ([]*cluster.Entity, error) {
	out := make([]*cluster.Entity, 0, len(es))
	for _, e := range es {
		// Only row data migrates. System entities (config) and every non-row
		// variant pass through untouched: a delete carries the sentinel and a
		// refresh-boundary marker carries no Data at all, so running either
		// through the migration chain would corrupt it into a permanent error
		// — silently dropping an erasure, or dead-lettering a control marker
		// on any topic whose type has since gained a version. The downstream
		// syncable switches on Variant() to honor each shape.
		if cluster.IsInternal(e.ID) || e.Variant() != cluster.EntityVariantRow {
			out = append(out, e)
			continue
		}
		latest, err := r.ResolveType(cluster.LatestTypeRef(e.ID))
		if err != nil {
			// The latest type isn't resolvable. The ref is keyed on e.ID (the
			// topic's type), never this entity's data/key, so this fails EVERY
			// entity of the topic identically — a not-yet-replicated or a deleted
			// type: schema/timing-shaped, not entry-specific. Per the egress
			// classification rule (permanent ⟺ entry-specific) it must stay
			// TRANSIENT — wedge until the type is available — not be dead-lettered.
			// Mark it so the Sync wrappers keep it transient instead of Permanent.
			return nil, fmt.Errorf("%w: resolve latest type %s: %w", errTypeUnavailable, e.ID, err)
		}
		if latest.Version <= e.Version {
			out = append(out, e)
			continue
		}
		start := time.Now()
		data, err := Chain(ctx, r, e.ID, e.Version, latest.Version, e.Data)
		if err != nil {
			// KNOWN LIMITATION (ambiguous classification): unlike ResolveType above,
			// Chain takes e.Data, so a failure here can be EITHER entry-specific (a
			// malformed row this program can't transform → permanent is right) OR
			// config-shaped (a broken/incompatible migration program that fails every
			// row of that version-range → should be transient). They are
			// indistinguishable at the failure point, so it stays Permanent (the same
			// accepted asymmetry as Postgres 23502). The clean fix is config-time
			// migration-program validation — see the 0.8 ticket
			// classify-config-shaped-syncable-errors.
			return nil, err
		}
		if m != nil {
			m.MigrationCompleted(e.ID, time.Since(start))
		}
		copy := *e
		copy.Type = latest
		copy.Data = data
		out = append(out, &copy)
	}
	return out, nil
}
