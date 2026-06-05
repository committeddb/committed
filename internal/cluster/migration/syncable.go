package migration

import (
	"context"
	"fmt"

	"github.com/philborlin/committed/internal/cluster"
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
func Wrap(inner cluster.Syncable, r Resolver) cluster.Syncable {
	if bs, ok := inner.(cluster.BatchSyncable); ok {
		return &batchSyncable{single: single{inner: inner, resolver: r}, batch: bs}
	}
	return &single{inner: inner, resolver: r}
}

type single struct {
	inner    cluster.Syncable
	resolver Resolver
}

func (s *single) Sync(ctx context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	entities, err := migrateEntities(s.resolver, a.Entities)
	if err != nil {
		return false, cluster.Permanent(err)
	}
	return s.inner.Sync(ctx, &cluster.Actual{Index: a.Index, Entities: entities})
}

func (s *single) Close() error { return s.inner.Close() }

type batchSyncable struct {
	single
	batch cluster.BatchSyncable
}

func (b *batchSyncable) SyncBatch(ctx context.Context, as []*cluster.Actual) (bool, error) {
	migrated := make([]*cluster.Actual, len(as))
	for i, a := range as {
		entities, err := migrateEntities(b.resolver, a.Entities)
		if err != nil {
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
func migrateEntities(r Resolver, es []*cluster.Entity) ([]*cluster.Entity, error) {
	out := make([]*cluster.Entity, 0, len(es))
	for _, e := range es {
		if cluster.IsSystem(e.ID) {
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
		data, err := Chain(r, e.ID, e.Version, latest.Version, e.Data)
		if err != nil {
			return nil, err
		}
		copy := *e
		copy.Type = latest
		copy.Data = data
		out = append(out, &copy)
	}
	return out, nil
}
