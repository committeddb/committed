package interpretation

import (
	"context"
	"errors"
	"fmt"

	"github.com/committeddb/committed/internal/cluster"
)

// errTypeUnavailable marks a rebind failure caused by the effective version's
// type record not resolving. Schema/timing-shaped — it fails every entity of
// the topic identically — so the Sync wrappers keep it TRANSIENT (the worker
// wedges until it resolves), mirroring the migration wrapper's
// classification.
var errTypeUnavailable = errors.New("interpretation: effective version's type unavailable")

// Wrap returns a cluster.Syncable that rebinds each user-data entity to its
// AUTHORITATIVE reading — stamp ⊕ errata fold — before inner sees it. Applied
// to EVERY syncable (both modes): version-pinned and version-aware consumers
// dispatch on the effective version, and the always-current migration chain
// (wrapped INSIDE this) starts from it. registry returns the live compiled
// snapshot so errata applied mid-run take effect on subsequent reads; the
// errata-free path is one nil-map lookup per entity. If inner implements
// cluster.BatchSyncable, the wrapper does too.
func Wrap(inner cluster.Syncable, registry func() *Registry, r cluster.TypeResolver) cluster.Syncable {
	if bs, ok := inner.(cluster.BatchSyncable); ok {
		return &batchSyncable{single: single{inner: inner, registry: registry, resolver: r}, batch: bs}
	}
	return &single{inner: inner, registry: registry, resolver: r}
}

type single struct {
	inner    cluster.Syncable
	registry func() *Registry
	resolver cluster.TypeResolver
}

func (s *single) Sync(ctx context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	entities, err := rebindEntities(ctx, s.registry(), s.resolver, a)
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err() // shutdown mid-rebind — retry, don't dead-letter
		}
		if errors.Is(err, errTypeUnavailable) {
			return false, err // schema/timing-shaped → transient, wedge
		}
		// Entry-specific (a payload a predicate cannot evaluate) → dead-letter
		// that proposal rather than silently choosing a reading.
		return false, cluster.Permanent(err)
	}
	if entities == nil {
		return s.inner.Sync(ctx, a) // nothing rebound — hand through untouched
	}
	return s.inner.Sync(ctx, &cluster.Actual{Index: a.Index, Entities: entities})
}

func (s *single) Close() error { return s.inner.Close() }

// Unwrap exposes the wrapped syncable (cluster.SyncableUnwrapper), so
// capability interfaces this wrapper doesn't deliberately re-implement
// resolve through cluster.SyncableAs instead of each needing a hand-written
// forward — the migration wrapper's lesson (a masked Teardownable), re-learned
// here when the stage recoverer was silently masked for EVERY syncable: this
// wrapper is unconditional, so a staged projection's reset store resumed from
// the checkpoint without re-deriving (the 0.7.10-merge e2e failure: an
// aggregate of 1 where 3 rows were folded). Unwrapping is semantically safe
// for stage recovery because this wrapper rebinds version STAMPS, never
// payload bytes — a recovery fold through the inner projection sees identical
// data. The explicit forwards above (Teardown, Rematerializable,
// CheckpointPolicy) stay for call sites that assert directly on the delivered
// syncable rather than through SyncableAs. batchSyncable embeds single, so it
// inherits this.
func (s *single) Unwrap() cluster.Syncable { return s.inner }

// CheckpointPolicy forwards the wrapped syncable's checkpoint cadence (see
// the migration wrapper's identical forward).
func (s *single) CheckpointPolicy() cluster.CheckpointPolicy {
	if cc, ok := s.inner.(cluster.CheckpointConfigurable); ok {
		return cc.CheckpointPolicy()
	}
	return cluster.CheckpointPolicy{}
}

// Teardown forwards destination teardown. Unlike migration.Wrap — which only
// decorates always-current syncables — this wrapper decorates EVERY syncable,
// so the delete/rebuild paths' Teardownable type-assertion must keep working
// through it: forward when inner tears down, no-op when it owns no external
// state (the same outcome as not implementing the interface).
func (s *single) Teardown() error {
	if td, ok := s.inner.(cluster.Teardownable); ok {
		return td.Teardown()
	}
	return nil
}

// CanRematerialize / Begin / Complete forward the re-materialization
// extension, preserving the innermost sink's answer (a sink that doesn't
// implement it reports false, and the verbs refuse).
func (s *single) CanRematerialize() bool {
	if rm, ok := s.inner.(cluster.Rematerializable); ok {
		return rm.CanRematerialize()
	}
	return false
}

func (s *single) BeginRematerialization(ctx context.Context, epoch uint64) error {
	if rm, ok := s.inner.(cluster.Rematerializable); ok {
		return rm.BeginRematerialization(ctx, epoch)
	}
	return cluster.ErrNotRematerializable
}

func (s *single) CompleteRematerialization(ctx context.Context) error {
	if rm, ok := s.inner.(cluster.Rematerializable); ok {
		return rm.CompleteRematerialization(ctx)
	}
	return cluster.ErrNotRematerializable
}

type batchSyncable struct {
	single
	batch cluster.BatchSyncable
}

func (b *batchSyncable) SyncBatch(ctx context.Context, as []*cluster.Actual) (bool, error) {
	reg := b.registry()
	out := as
	copied := false
	for i, a := range as {
		entities, err := rebindEntities(ctx, reg, b.resolver, a)
		if err != nil {
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			if errors.Is(err, errTypeUnavailable) {
				return false, err
			}
			return false, cluster.Permanent(err)
		}
		if entities == nil {
			continue
		}
		if !copied {
			// First rebind in the batch: copy the slice so callers' input
			// stays untouched (retry paths see consistent input).
			out = make([]*cluster.Actual, len(as))
			copy(out, as)
			copied = true
		}
		out[i] = &cluster.Actual{Index: a.Index, Entities: entities}
	}
	return b.batch.SyncBatch(ctx, out)
}

// rebindEntities returns a copy of the actual's entities with every rebound
// user-data row carrying its effective version's Type, or nil when nothing
// rebound (the common case — zero allocation). System entities and non-row
// variants pass through untouched.
func rebindEntities(ctx context.Context, reg *Registry, r cluster.TypeResolver, a *cluster.Actual) ([]*cluster.Entity, error) {
	var out []*cluster.Entity
	for i, e := range a.Entities {
		if e.Type == nil || cluster.IsInternal(e.ID) || e.Variant() != cluster.EntityVariantRow {
			if out != nil {
				out = append(out, e)
			}
			continue
		}
		eff, err := reg.EffectiveVersion(ctx, e.ID, a.Index, e.Version, e.Data)
		if err != nil {
			return nil, err
		}
		if eff == e.Version {
			if out != nil {
				out = append(out, e)
			}
			continue
		}
		t, err := r.ResolveType(cluster.TypeRefAt(e.ID, eff))
		if err != nil {
			// Admission guarantees the rebind target existed; a later type
			// delete makes this fail every entity of the topic alike —
			// schema/timing-shaped, so transient (wedge), never dead-letter.
			return nil, fmt.Errorf("%w: resolve %s@%d: %w", errTypeUnavailable, e.ID, eff, err)
		}
		if out == nil {
			out = make([]*cluster.Entity, 0, len(a.Entities))
			out = append(out, a.Entities[:i]...)
		}
		copy := *e
		copy.Type = t
		out = append(out, &copy)
	}
	return out, nil
}
