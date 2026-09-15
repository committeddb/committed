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
// AUTHORITATIVE reading — stamp ⊕ restatement fold — before inner sees it. Applied
// to EVERY syncable (both modes): version-pinned and version-aware consumers
// dispatch on the effective version, and the always-current migration chain
// (wrapped INSIDE this) starts from it. registry returns the live compiled
// snapshot so restatements applied mid-run take effect on subsequent reads; the
// restatement-free path is one nil-map lookup per entity. If inner implements
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
		// Fully classified at the failure site: errTypeUnavailable passes
		// through transient (schema/timing-shaped → wedge), and a predicate
		// failure arrives from the restatement's own ambiguity tracker —
		// Permanent (dead-letter that proposal rather than silently choosing
		// a reading) until a run of distinct rows establishes it
		// config-shaped, transient (wedge) after.
		return false, err
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

// No capability interface is implemented here — not Teardownable, not
// Rematerializable, not RenderingStamped. The engine resolves every
// capability through the Unwrap chain (cluster.SyncableAs), which reaches
// the sink; a forwarding-with-fallback method on the wrapper would make
// every wrapped syncable LOOK capable, and the engine asks exactly those
// questions (does delete drop the destination? can this sink converge in
// place?) of the sink itself. Pinned by TestWrapExposesNoCapabilities.

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
			return false, err // classified at the failure site — see Sync
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
