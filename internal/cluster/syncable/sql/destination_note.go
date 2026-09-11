package sql

import (
	"context"
	gosql "database/sql"
	"errors"
	"fmt"

	"github.com/committeddb/committed/internal/cluster"
)

// RenderingVersion is the version of every rendering the SQL sink family
// writes into a destination: projected column values, the aggregate and
// forEach sidecars, the keyless applied log. It is the destination's twin of
// the stage store's format version (stages/store_format_reference_test.go):
// the config fingerprint covers what the operator declared, this number
// covers what the engine chose. Bump it when any rendering changes; a
// destination stamped with another version parks until it is converged
// again — rematerialize for a keyed sink, delete + re-POST otherwise — so
// rows rendered under two rules never share a table. The docker reference
// (destination_reference_test.go) fails when the bytes change under the
// current number.
const RenderingVersion uint64 = 1

// DestinationsTable is the per-database helper table holding committed's note
// on each destination table: (table_name, rendering_version, owned,
// materialized_at). It lives beside the rows it describes so it moves,
// drops, and restores with them. rendering_version says which version wrote
// the rows; owned says whether committed created the table — and so whether
// a delete drops it (the protocol: delete what we created, leave what we
// didn't; keepData hands a created table over by clearing owned).
const DestinationsTable = "committed__destinations"

// destinationNote is one row of DestinationsTable.
type destinationNote struct {
	version uint64
	owned   bool
}

// readNote reads table's note, creating the meta table on first contact.
// present is false for a table committed has never noted.
func readNote(ctx context.Context, db *gosql.DB, dialect Dialect, table string) (note destinationNote, present bool, err error) {
	if err := dialect.EnsureDestinations(ctx, db); err != nil {
		return destinationNote{}, false, err
	}
	var version int64
	var owned bool
	err = db.QueryRowContext(ctx, dialect.DestinationSelectSQL(), table).Scan(&version, &owned)
	switch {
	case errors.Is(err, gosql.ErrNoRows):
		return destinationNote{}, false, nil
	case err != nil:
		return destinationNote{}, false, fmt.Errorf("read destination note for %s: %w", table, err)
	case version < 0:
		return destinationNote{}, false, fmt.Errorf("read destination note for %s: negative version %d", table, version)
	}
	return destinationNote{version: uint64(version), owned: owned}, true, nil
}

// stampRendering records RenderingVersion as table's rendering,
// leaving ownership as it is (absent rows are inserted not owned: a table
// committed did not create).
func stampRendering(ctx context.Context, db *gosql.DB, dialect Dialect, table string) error {
	if err := dialect.EnsureDestinations(ctx, db); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, dialect.DestinationStampSQL(), table, int64(RenderingVersion)); err != nil { //nolint:gosec // G115: a small constant
		return fmt.Errorf("stamp rendering for %s: %w", table, err)
	}
	return nil
}

// claimOwnership records that committed created table: owned, rendered by
// this binary.
func claimOwnership(ctx context.Context, db *gosql.DB, dialect Dialect, table string) error {
	if err := dialect.EnsureDestinations(ctx, db); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, dialect.DestinationClaimSQL(), table, int64(RenderingVersion)); err != nil { //nolint:gosec // G115: a small constant
		return fmt.Errorf("claim ownership of %s: %w", table, err)
	}
	return nil
}

// disown hands table over: committed keeps its note but no longer owns the
// table, so no later delete drops it.
func disown(ctx context.Context, db *gosql.DB, dialect Dialect, table string) error {
	if err := dialect.EnsureDestinations(ctx, db); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, dialect.DestinationDisownSQL(), table); err != nil {
		return fmt.Errorf("disown %s: %w", table, err)
	}
	return nil
}

// deleteNote removes table's note: the table is gone, so is what described it.
func deleteNote(ctx context.Context, db *gosql.DB, dialect Dialect, table string) error {
	if err := dialect.EnsureDestinations(ctx, db); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, dialect.DestinationDeleteSQL(), table); err != nil {
		return fmt.Errorf("delete destination note for %s: %w", table, err)
	}
	return nil
}

// claimIfCreated is Init's half of the ownership protocol: CREATE TABLE IF
// NOT EXISTS cannot say whether it created, so the caller probes first
// (existed) and claims the table only when it was absent.
func claimIfCreated(ctx context.Context, db *gosql.DB, dialect Dialect, table string, existed bool) error {
	if existed {
		return nil // attached, not created: no claim; the worker stamps it at first contact
	}
	return claimOwnership(ctx, db, dialect, table)
}

// ownsDestination answers Teardownable.OwnsDestination for a table: the note
// says committed created it, or there is no table yet (Init will create and
// claim it). A table with no note, or a note saying not owned, is one
// committed attached to.
func ownsDestination(ctx context.Context, db *gosql.DB, dialect Dialect, table string) (bool, error) {
	note, present, err := readNote(ctx, db, dialect, table)
	if err != nil {
		return false, err
	}
	if present && note.owned {
		return true, nil
	}
	exists, err := dialect.TableExists(ctx, db, table)
	if err != nil {
		return false, fmt.Errorf("probe %s: %w", table, err)
	}
	return !exists, nil
}

// OwnsDestination implements cluster.Teardownable.
func (c *Syncable) OwnsDestination(ctx context.Context) (bool, error) {
	return ownsDestination(ctx, c.db, c.dialect, c.config.Table)
}

// OwnsDestination implements cluster.Teardownable.
func (p *Projection) OwnsDestination(ctx context.Context) (bool, error) {
	return ownsDestination(ctx, p.db, p.dialect, p.config.Table)
}

// RenderingVersion implements cluster.RenderingStamped.
func (c *Syncable) RenderingVersion() uint64 { return RenderingVersion }

// RenderingStamp implements cluster.RenderingStamped.
func (c *Syncable) RenderingStamp(ctx context.Context) (uint64, bool, error) {
	note, present, err := readNote(ctx, c.db, c.dialect, c.config.Table)
	return note.version, present, err
}

// StampRendering implements cluster.RenderingStamped.
func (c *Syncable) StampRendering(ctx context.Context) error {
	return stampRendering(ctx, c.db, c.dialect, c.config.Table)
}

// RenderingVersion implements cluster.RenderingStamped.
func (p *Projection) RenderingVersion() uint64 { return RenderingVersion }

// RenderingStamp implements cluster.RenderingStamped.
func (p *Projection) RenderingStamp(ctx context.Context) (uint64, bool, error) {
	note, present, err := readNote(ctx, p.db, p.dialect, p.config.Table)
	return note.version, present, err
}

// StampRendering implements cluster.RenderingStamped.
func (p *Projection) StampRendering(ctx context.Context) error {
	return stampRendering(ctx, p.db, p.dialect, p.config.Table)
}

var (
	_ cluster.RenderingStamped = (*Syncable)(nil)
	_ cluster.RenderingStamped = (*Projection)(nil)
	_ cluster.Teardownable     = (*Syncable)(nil)
	_ cluster.Teardownable     = (*Projection)(nil)
)
