package sql

import (
	"context"
	gosql "database/sql"
	"errors"
	"fmt"
)

// SinkRenderingVersion is the version of every rendering the SQL sink family
// writes into a destination: projected column values, the aggregate and
// forEach sidecars, the keyless applied log. It is the destination's twin of
// the stage store's format version (stages/store_format_reference_test.go):
// the config fingerprint covers what the operator declared, this number
// covers what the engine chose. Bump it when any rendering changes; a
// destination stamped with another version parks until it is converged
// again — rematerialize for a keyed sink, delete + re-POST otherwise — so
// rows rendered under two rules never share a table. The docker reference
// (sink_rendering_reference_test.go) fails when the bytes change under the
// current number.
const SinkRenderingVersion uint64 = 1

// SinkMetaTable is the per-database helper table holding one stamp row per
// destination table: (table_name, rendering_version, materialized_at). It
// lives beside the rows it describes so it moves, drops, and restores with
// them.
const SinkMetaTable = "committed__sink_meta"

// renderingStamp reads the stamp for table, creating the meta table on
// first contact. present is false for a never-stamped destination.
func renderingStamp(ctx context.Context, db *gosql.DB, dialect Dialect, table string) (uint64, bool, error) {
	if err := dialect.EnsureSinkMeta(ctx, db); err != nil {
		return 0, false, err
	}
	var version int64
	err := db.QueryRowContext(ctx, dialect.SinkMetaSelectSQL(), table).Scan(&version)
	switch {
	case errors.Is(err, gosql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("read rendering stamp for %s: %w", table, err)
	case version < 0:
		return 0, false, fmt.Errorf("read rendering stamp for %s: negative version %d", table, version)
	}
	return uint64(version), true, nil
}

// stampRendering records SinkRenderingVersion as table's stamp.
func stampRendering(ctx context.Context, db *gosql.DB, dialect Dialect, table string) error {
	if err := dialect.EnsureSinkMeta(ctx, db); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, dialect.SinkMetaUpsertSQL(), table, int64(SinkRenderingVersion)); err != nil { //nolint:gosec // G115: a small constant
		return fmt.Errorf("stamp rendering for %s: %w", table, err)
	}
	return nil
}

// deleteRenderingStamp removes table's stamp: teardown drops the rows, so
// the stamp goes with them (a recreated table must not inherit it).
func deleteRenderingStamp(ctx context.Context, db *gosql.DB, dialect Dialect, table string) error {
	if err := dialect.EnsureSinkMeta(ctx, db); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, dialect.SinkMetaDeleteSQL(), table); err != nil {
		return fmt.Errorf("delete rendering stamp for %s: %w", table, err)
	}
	return nil
}

// RenderingVersion implements cluster.RenderingStamped.
func (c *Syncable) RenderingVersion() uint64 { return SinkRenderingVersion }

// RenderingStamp implements cluster.RenderingStamped.
func (c *Syncable) RenderingStamp(ctx context.Context) (uint64, bool, error) {
	return renderingStamp(ctx, c.db, c.dialect, c.config.Table)
}

// StampRendering implements cluster.RenderingStamped.
func (c *Syncable) StampRendering(ctx context.Context) error {
	return stampRendering(ctx, c.db, c.dialect, c.config.Table)
}

// RenderingVersion implements cluster.RenderingStamped.
func (p *Projection) RenderingVersion() uint64 { return SinkRenderingVersion }

// RenderingStamp implements cluster.RenderingStamped.
func (p *Projection) RenderingStamp(ctx context.Context) (uint64, bool, error) {
	return renderingStamp(ctx, p.db, p.dialect, p.config.Table)
}

// StampRendering implements cluster.RenderingStamped.
func (p *Projection) StampRendering(ctx context.Context) error {
	return stampRendering(ctx, p.db, p.dialect, p.config.Table)
}
