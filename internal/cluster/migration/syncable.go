package migration

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
// TRANSIENT so the worker wedges until the type resolves; a Chain failure on
// an entity is classified by that chain's ambiguity tracker — Permanent
// (dead-letter this proposal, move on) while it may be entry-specific,
// transient (wedge) once a run of consecutive distinct rows establishes the
// program config-shaped (see cluster.AmbiguityTracker).
//
// m drives the committed.type.migration.duration histogram (recorded per
// migrated entity, on success). Nil when metrics are disabled.
func Wrap(inner cluster.Syncable, r Resolver, m *metrics.Metrics) cluster.Syncable {
	if bs, ok := inner.(cluster.BatchSyncable); ok {
		return &batchSyncable{single: single{inner: inner, resolver: r, metrics: m, trackers: map[string]*cluster.AmbiguityTracker{}}, batch: bs}
	}
	return &single{inner: inner, resolver: r, metrics: m, trackers: map[string]*cluster.AmbiguityTracker{}}
}

type single struct {
	inner    cluster.Syncable
	resolver Resolver
	metrics  *metrics.Metrics
	// trackers classify Chain failures per (topic, fromVersion→toVersion)
	// site (see cluster.AmbiguityTracker): a program broken for one
	// version-range accumulates evidence only from the rows that actually
	// run that chain. trackerMu guards the map (Sync and SyncBatch share
	// it through the embedded single).
	trackerMu sync.Mutex
	trackers  map[string]*cluster.AmbiguityTracker
}

// chainTracker returns (allocating on first use) the ambiguity tracker for
// the chain migrating topic id from version `from` to `to`. The site is the
// whole stamped path, not the individual failing step: per-step pooling would
// converge faster on mixed-stamp topics, but resetting a step's evidence
// requires knowing which steps a SUCCESSFUL chain walked — which Chain does
// not report — and resetting on any success would let one stamp's healthy
// path mask another stamp's broken step (the when-gated gap all over again).
// Per-path keying already isolates interleaved stamps correctly; each broken
// stamp class wedges on its own run.
func (s *single) chainTracker(id string, from, to int) *cluster.AmbiguityTracker {
	s.trackerMu.Lock()
	defer s.trackerMu.Unlock()
	key := fmt.Sprintf("%s@%d>%d", id, from, to)
	t := s.trackers[key]
	if t == nil {
		t = cluster.NewAmbiguityTracker()
		s.trackers[key] = t
	}
	return t
}

func (s *single) Sync(ctx context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	entities, err := s.migrateEntities(ctx, a)
	if err != nil {
		// Fully classified at the failure site (see migrateEntities): a ctx
		// interruption and a type-unavailable failure pass through TRANSIENT;
		// a Chain failure arrives Permanent — or, once its chain's tracker
		// establishes a config-shaped run, transient (wedge).
		return false, err
	}
	return s.inner.Sync(ctx, &cluster.Actual{Index: a.Index, Entities: entities})
}

func (s *single) Close() error { return s.inner.Close() }

// Unwrap exposes the wrapped syncable (cluster.SyncableUnwrapper), so
// capability interfaces the wrapper doesn't deliberately re-implement —
// Teardownable and its future siblings — resolve through
// cluster.SyncableAs instead of each needing a hand-written forward
// (Teardownable was silently masked here for every always-current
// syncable until the field found deleted projections' tables surviving).
// batchSyncable embeds single, so it inherits this.
func (s *single) Unwrap() cluster.Syncable { return s.inner }

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

// CanRematerialize / Begin / Complete forward the re-materialization
// extension through the migration wrapper (the interpretation wrapper — the
// outermost — forwards to this), preserving the innermost sink's answer.
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
	migrated := make([]*cluster.Actual, len(as))
	for i, a := range as {
		entities, err := b.migrateEntities(ctx, a)
		if err != nil {
			return false, err // classified at the failure site — see migrateEntities
		}
		migrated[i] = &cluster.Actual{Index: a.Index, Entities: entities}
	}
	return b.batch.SyncBatch(ctx, migrated)
}

// migrateEntities returns a copy of a's entities with every user-data
// entity's Data run through the migration chain up to the current latest
// type version. System entities (config entries) pass through untouched.
// The input entities are not modified — retry paths see consistent input
// across attempts. Every error it returns is FULLY classified for the
// worker: ctx interruptions unwrapped (retry), type-unavailable transient
// (wedge), Chain failures through the chain's ambiguity tracker.
func (s *single) migrateEntities(ctx context.Context, a *cluster.Actual) ([]*cluster.Entity, error) {
	r, m, es := s.resolver, s.metrics, a.Entities
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
			if ctx.Err() != nil {
				// The worker ctx was cancelled mid-chain (shutdown / replace) —
				// a transient interruption, not a bad program and not evidence.
				// Return it unwrapped so the worker retries after restart. A
				// per-run TIMEOUT leaves the parent ctx live, so it still
				// classifies below.
				return nil, ctx.Err()
			}
			// Chain takes e.Data, so a failure here is EITHER entry-specific
			// (a malformed row this program can't transform → permanent is
			// right) OR config-shaped (a program that fails every row of a
			// version-range → transient is right). Indistinguishable from ONE
			// failure — this chain's tracker classifies from its own history:
			// isolated failures dead-letter; a consecutive-distinct-row run
			// with no success wedges loudly (see cluster.AmbiguityTracker).
			// Program syntax and determinism are already validated at type
			// registration (compileMigration).
			return nil, s.chainTracker(e.ID, e.Version, latest.Version).Classify(a.Index, err)
		}
		s.chainTracker(e.ID, e.Version, latest.Version).Succeeded()
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
