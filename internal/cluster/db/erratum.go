package db

import (
	"bytes"
	"context"
	"fmt"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/migration"
)

// featureLevelErrata gates erratum emission: the Erratum record is GATED
// (must-understand) in the system-type namespace — a member that cannot fold
// errata would fatal on apply — so an erratum is only admitted once every
// member announces version.FeatureLevel >= 2.
const featureLevelErrata uint64 = 2

// ParseErratum reads the [erratum] TOML/JSON envelope into the record. The
// storage-dependent admission checks live in ProposeErratum.
func ParseErratum(c *cluster.Configuration) (*cluster.Erratum, error) {
	v, err := cluster.ParseConfigBytes(c.MimeType, c.Data)
	if err != nil {
		return nil, err
	}
	e := &cluster.Erratum{
		ID:              c.ID,
		TypeID:          v.GetString("erratum.type"),
		FromIndex:       uint64(max(v.GetInt("erratum.fromIndex"), 0)), //nolint:gosec // G115: negatives clamped, admission validates
		ToIndex:         uint64(max(v.GetInt("erratum.toIndex"), 0)),   //nolint:gosec // G115: negatives clamped, admission validates
		RebindToVersion: v.GetInt("erratum.rebindToVersion"),
		FromVersion:     v.GetInt("erratum.fromVersion"),
		Predicate:       v.GetString("erratum.predicate"),
	}
	if e.TypeID == "" {
		return nil, fmt.Errorf("erratum.type is required: the type (topic) whose readings this erratum rebinds")
	}
	if e.FromIndex == 0 || e.ToIndex == 0 {
		return nil, fmt.Errorf("erratum.fromIndex and erratum.toIndex are required (1-based raft indexes; an erratum binds an existing range)")
	}
	if e.FromIndex > e.ToIndex {
		return nil, fmt.Errorf("erratum.fromIndex (%d) must not exceed erratum.toIndex (%d)", e.FromIndex, e.ToIndex)
	}
	if e.RebindToVersion <= 0 {
		return nil, fmt.Errorf("erratum.rebindToVersion is required: the version matching actuals read as")
	}
	if e.Predicate != "" {
		// The predicate is part of the trust base: it re-evaluates on every
		// read, so it is pinned to the same deterministic subset as
		// migrations (no now(), no env, no external input).
		if err := migration.Compile([]byte(e.Predicate)); err != nil {
			return nil, fmt.Errorf("erratum.predicate is not a valid deterministic jq program: %w", err)
		}
	}
	return e, nil
}

// ProposeErratum admits one interpretation-registry statement. Errata are
// APPEND-ONLY: an id that already exists with different content is refused
// ("author a new erratum to correct this one"); an identical re-POST is an
// idempotent no-op. Admission is loud at POST — unknown type, unknown target
// version, a range beyond the applied log, a non-deterministic predicate all
// refuse here, never at first read.
func (db *DB) ProposeErratum(ctx context.Context, c *cluster.Configuration) error {
	e, err := ParseErratum(c)
	if err != nil {
		return cluster.NewConfigError(err)
	}

	// Mixed-version safety: the record is gated (an old member would fatal
	// applying it), so refuse until every member can fold errata.
	if !db.featureEnabled(featureLevelErrata) {
		return &cluster.ClusterBelowFeatureLevelError{
			Feature: "errata", Required: featureLevelErrata, ClusterMin: db.clusterMinFeatureLevel(),
		}
	}

	// The rebind target — and, when narrowed, the stamp selector — must be
	// declared versions of a USER type.
	if cluster.IsInternal(e.TypeID) || cluster.IsReservedSystemID(e.TypeID) {
		return cluster.NewConfigError(fmt.Errorf("erratum.type %q is a committed system type; errata rebind user topics only", e.TypeID))
	}
	if _, err := db.storage.ResolveType(cluster.TypeRefAt(e.TypeID, e.RebindToVersion)); err != nil {
		return cluster.NewConfigError(fmt.Errorf("erratum.rebindToVersion %d is not a declared version of type %q: %w", e.RebindToVersion, e.TypeID, err))
	}
	if e.FromVersion != 0 {
		if _, err := db.storage.ResolveType(cluster.TypeRefAt(e.TypeID, e.FromVersion)); err != nil {
			return cluster.NewConfigError(fmt.Errorf("erratum.fromVersion %d is not a declared version of type %q: %w", e.FromVersion, e.TypeID, err))
		}
	}

	// An erratum binds the PAST: a range beyond the applied log would be a
	// statement about data that doesn't exist.
	if applied := db.storage.AppliedIndex(); e.ToIndex > applied {
		return cluster.NewConfigError(fmt.Errorf("erratum.toIndex %d is beyond the applied log (%d): an erratum rebinds existing actuals", e.ToIndex, applied))
	}

	// Immutability: same id + same content = idempotent retry; same id +
	// different content = an edit, which append-only forbids.
	if existing, _, ok := db.storage.ErratumByID(e.ID); ok {
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
		return cluster.NewConfigError(fmt.Errorf("erratum %q already exists with different content: errata are append-only — author a NEW erratum to correct it (later in the log wins)", e.ID))
	}

	entity, err := cluster.NewErratumEntity(e)
	if err != nil {
		return err
	}
	return db.Propose(ctx, &cluster.Proposal{Entities: []*cluster.Entity{entity}})
}

// Errata implements cluster.Cluster: every applied erratum with its raft
// index (unordered).
func (db *DB) Errata() ([]cluster.AppliedErratum, error) {
	return db.storage.AppliedErrata()
}

// SyncableInterpretation implements cluster.Cluster: the syncable's
// interpretation pin (from its checkpoint; 0 = pinned before any errata) and
// whether an erratum affecting a consumed topic landed past it — meaning some
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
	// topic's history changes (errata rebind stamps; a migration edit
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
// materialization STARTING NOW is derived under: the errata registry
// highwater joined with the latest in-place migration-edit index of each
// consumed topic. A fresh worker (no checkpoint — replaying from index 0)
// pins here: its replay reads everything through the CURRENT readings, so
// neither an already-applied erratum nor an already-applied migration edit
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
