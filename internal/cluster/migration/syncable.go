package migration

import (
	"context"
	"fmt"
	"time"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/metrics"
)

// Wrap returns a cluster.Syncable that transforms each proposal's
// user-data entities through the migration chain from their stamped
// version up to the current latest before handing them to inner. If
// inner implements cluster.BatchSyncable, the returned syncable does
// too — the batch optimization is preserved.
//
// Wrap is the wal-layer hook for ModeAlwaysCurrent syncables. The
// rest of the system (db.Sync, the worker loop, tests that don't care
// about migration) sees a plain Syncable with the usual contract.
// Migration failures are reported as cluster.Permanent errors so the
// worker logs and skips the bad proposal rather than retrying.
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
			return nil, fmt.Errorf("resolve latest type %s: %w", e.ID, err)
		}
		if latest.Version <= e.Version {
			out = append(out, e)
			continue
		}
		start := time.Now()
		data, err := Chain(ctx, r, e.ID, e.Version, latest.Version, e.Data)
		if err != nil {
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
