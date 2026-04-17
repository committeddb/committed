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

func (s *single) Sync(ctx context.Context, p *cluster.Proposal) (cluster.ShouldSnapshot, error) {
	migrated, err := migrateProposal(s.resolver, p)
	if err != nil {
		return false, cluster.Permanent(err)
	}
	return s.inner.Sync(ctx, migrated)
}

func (s *single) Close() error { return s.inner.Close() }

type batchSyncable struct {
	single
	batch cluster.BatchSyncable
}

func (b *batchSyncable) SyncBatch(ctx context.Context, ps []*cluster.Proposal) (bool, error) {
	migrated := make([]*cluster.Proposal, len(ps))
	for i, p := range ps {
		m, err := migrateProposal(b.resolver, p)
		if err != nil {
			return false, cluster.Permanent(err)
		}
		migrated[i] = m
	}
	return b.batch.SyncBatch(ctx, migrated)
}

// migrateProposal rebuilds a Proposal with every user-data entity's
// Data run through the migration chain up to the current latest type
// version. System entities (config entries) pass through untouched.
// The input Proposal is not modified — retry paths see consistent
// input across attempts.
func migrateProposal(r Resolver, p *cluster.Proposal) (*cluster.Proposal, error) {
	out := &cluster.Proposal{RequestID: p.RequestID, Entities: make([]*cluster.Entity, 0, len(p.Entities))}
	for _, e := range p.Entities {
		if cluster.IsSystem(e.Type.ID) {
			out.Entities = append(out.Entities, e)
			continue
		}
		latest, err := r.ResolveType(cluster.LatestTypeRef(e.Type.ID))
		if err != nil {
			return nil, fmt.Errorf("resolve latest type %s: %w", e.Type.ID, err)
		}
		if latest.Version <= e.Type.Version {
			out.Entities = append(out.Entities, e)
			continue
		}
		data, err := Chain(r, e.Type.ID, e.Type.Version, latest.Version, e.Data)
		if err != nil {
			return nil, err
		}
		copy := *e
		copy.Type = latest
		copy.Data = data
		out.Entities = append(out.Entities, &copy)
	}
	return out, nil
}
