package db

import (
	"bytes"
	"context"
	"fmt"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/migration"
)

// featureLevelRestatements gates restatement emission: the Restatement record is GATED
// (must-understand) in the system-type namespace — a member that cannot fold
// restatements would fatal on apply — so a restatement is only admitted once every
// member announces version.FeatureLevel >= 2.
const featureLevelRestatements uint64 = 2

// ParseRestatement reads the [restatement] TOML/JSON envelope into the record. The
// storage-dependent admission checks live in ProposeRestatement.
func ParseRestatement(c *cluster.Configuration) (*cluster.Restatement, error) {
	v, err := cluster.ParseConfigBytes(c.MimeType, c.Data)
	if err != nil {
		return nil, err
	}
	e := &cluster.Restatement{
		ID:            c.ID,
		TypeID:        v.GetString("restatement.type"),
		FromIndex:     uint64(max(v.GetInt("restatement.fromIndex"), 0)), //nolint:gosec // G115: negatives clamped, admission validates
		ToIndex:       uint64(max(v.GetInt("restatement.toIndex"), 0)),   //nolint:gosec // G115: negatives clamped, admission validates
		ReadAsVersion: v.GetInt("restatement.readAsVersion"),
		FromVersion:   v.GetInt("restatement.fromVersion"),
		Predicate:     v.GetString("restatement.predicate"),
	}
	if e.TypeID == "" {
		return nil, fmt.Errorf("restatement.type is required: the type (topic) whose readings this restatement rebinds")
	}
	if e.FromIndex == 0 || e.ToIndex == 0 {
		return nil, fmt.Errorf("restatement.fromIndex and restatement.toIndex are required (1-based raft indexes; a restatement binds an existing range)")
	}
	if e.FromIndex > e.ToIndex {
		return nil, fmt.Errorf("restatement.fromIndex (%d) must not exceed restatement.toIndex (%d)", e.FromIndex, e.ToIndex)
	}
	if e.ReadAsVersion <= 0 {
		return nil, fmt.Errorf("restatement.readAsVersion is required: the version matching actuals read as")
	}
	if e.Predicate != "" {
		// The predicate is part of the trust base: it re-evaluates on every
		// read, so it is pinned to the same deterministic subset as
		// migrations (no now(), no env, no external input).
		if err := migration.Compile([]byte(e.Predicate)); err != nil {
			return nil, fmt.Errorf("restatement.predicate is not a valid deterministic jq program: %w", err)
		}
	}
	return e, nil
}

// admitRestatementChecks runs the storage-backed admission checks SHARED by
// ProposeRestatement and DryRunRestatement — one home, so the rehearsal can never
// drift from the real refusals ("same checks, same words" is the dry-run's
// contract). The propose-only checks stay with ProposeRestatement: the feature
// gate (a rehearsal admits nothing and works mid-upgrade) and append-only id
// immutability (a rehearsal has no id).
func (db *DB) admitRestatementChecks(e *cluster.Restatement) error {
	// The rebind target — and, when narrowed, the stamp selector — must be
	// declared versions of a USER type.
	if cluster.IsInternal(e.TypeID) || cluster.IsReservedSystemID(e.TypeID) {
		return cluster.NewConfigError(fmt.Errorf("restatement.type %q is a committed system type; restatements rebind user topics only", e.TypeID))
	}
	if _, err := db.storage.ResolveType(cluster.TypeRefAt(e.TypeID, e.ReadAsVersion)); err != nil {
		return cluster.NewConfigError(fmt.Errorf("restatement.readAsVersion %d is not a declared version of type %q: %w", e.ReadAsVersion, e.TypeID, err))
	}
	if e.FromVersion != 0 {
		if _, err := db.storage.ResolveType(cluster.TypeRefAt(e.TypeID, e.FromVersion)); err != nil {
			return cluster.NewConfigError(fmt.Errorf("restatement.fromVersion %d is not a declared version of type %q: %w", e.FromVersion, e.TypeID, err))
		}
	}
	// A restatement binds the PAST: a range beyond the applied log would be a
	// statement about data that doesn't exist.
	if applied := db.storage.AppliedIndex(); e.ToIndex > applied {
		return cluster.NewConfigError(fmt.Errorf("restatement.toIndex %d is beyond the applied log (%d): a restatement rebinds existing actuals", e.ToIndex, applied))
	}
	return nil
}

// ProposeRestatement admits one interpretation-registry statement. Restatements are
// APPEND-ONLY: an id that already exists with different content is refused
// ("author a new restatement to correct this one"); an identical re-POST is an
// idempotent no-op. Admission is loud at POST — unknown type, unknown target
// version, a range beyond the applied log, a non-deterministic predicate all
// refuse here, never at first read.
func (db *DB) ProposeRestatement(ctx context.Context, c *cluster.Configuration) error {
	e, err := ParseRestatement(c)
	if err != nil {
		return cluster.NewConfigError(err)
	}

	// Mixed-version safety: the record is gated (an old member would fatal
	// applying it), so refuse until every member can fold restatements.
	if !db.featureEnabled(featureLevelRestatements) {
		return &cluster.ClusterBelowFeatureLevelError{
			Feature: "restatements", Required: featureLevelRestatements, ClusterMin: db.clusterMinFeatureLevel(),
		}
	}

	if err := db.admitRestatementChecks(e); err != nil {
		return err
	}

	// Immutability: same id + same content = idempotent retry; same id +
	// different content = an edit, which append-only forbids.
	if existing, _, ok := db.storage.RestatementByID(e.ID); ok {
		newBytes, err := e.Marshal()
		if err != nil {
			return err
		}
		oldBytes, err := existing.Marshal()
		if err != nil {
			return err
		}
		if bytes.Equal(newBytes, oldBytes) {
			return nil // idempotent re-POST
		}
		return cluster.NewConfigError(fmt.Errorf("restatement %q already exists with different content: restatements are append-only — author a NEW restatement to correct it (later in the log wins)", e.ID))
	}

	entity, err := cluster.NewRestatementEntity(e)
	if err != nil {
		return err
	}
	return db.Propose(ctx, &cluster.Proposal{Entities: []*cluster.Entity{entity}})
}

// Restatements implements cluster.Cluster: every applied restatement with its raft
// index (unordered).
func (db *DB) Restatements() ([]cluster.AppliedRestatement, error) {
	return db.storage.AppliedRestatements()
}

// SyncableInterpretation implements cluster.Cluster: the syncable's
// interpretation pin (from its checkpoint; 0 = pinned before any restatements) and
// whether a restatement affecting a consumed topic landed past it — meaning some
// already-synced rows were derived under a superseded reading and stay that
// way until the operator re-derives.
func (db *DB) SyncableInterpretation(id string) (uint64, bool, error) {
	var pin uint64
	if ck, ok := db.storage.SyncableCheckpoint(id); ok {
		pin = ck.InterpretationIndex
	}
	cfg := db.currentSyncableConfig(id)
	if cfg == nil {
		return pin, false, nil
	}
	topics, err := db.parser.SyncableTopics(cfg.MimeType, cfg.Data)
	if err != nil {
		return pin, false, fmt.Errorf("enumerate consumed topics: %w", err)
	}
	reg := db.storage.InterpretationRegistry()
	for _, topic := range topics {
		if reg.TypeHighwater(topic) > pin {
			return pin, true, nil
		}
	}
	// An in-place MIGRATION edit is the other way the current reading of a
	// topic's history changes (restatements rebind stamps; a migration edit
	// rewrites the always-current transform). It moves the same coordinate:
	// an always-current consumer pinned below the edit has rows synced under
	// the previous transform — stale until re-materialized. As-stored
	// consumers deliver written bytes and never apply migrations, so the
	// edit cannot stale them.
	if mode, merr := db.parser.SyncableMode(cfg.MimeType, cfg.Data); merr == nil && mode == cluster.ModeAlwaysCurrent {
		for _, topic := range topics {
			if db.storage.TypeMigrationEditedAt(topic) > pin {
				return pin, true, nil
			}
		}
	}
	return pin, false, nil
}

// freshInterpretationPin is the interpretation coordinate a syncable
// materialization STARTING NOW is derived under: the restatement registry
// highwater joined with the latest in-place migration-edit index of each
// consumed topic. A fresh worker (no checkpoint — replaying from index 0)
// pins here: its replay reads everything through the CURRENT readings, so
// neither an already-applied restatement nor an already-applied migration edit
// may flag it stale. The mode does not matter for the pin — a higher pin
// never creates false staleness, it only records what "current" meant.
func (db *DB) freshInterpretationPin(id string) uint64 {
	pin := db.storage.InterpretationRegistry().Highwater()
	cfg := db.currentSyncableConfig(id)
	if cfg == nil {
		return pin
	}
	topics, err := db.parser.SyncableTopics(cfg.MimeType, cfg.Data)
	if err != nil {
		return pin
	}
	for _, topic := range topics {
		if e := db.storage.TypeMigrationEditedAt(topic); e > pin {
			pin = e
		}
	}
	return pin
}
