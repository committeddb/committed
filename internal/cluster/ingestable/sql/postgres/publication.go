package postgres

import (
	"context"
	gosql "database/sql"
	"errors"
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster/sqlident"
)

// Publication ownership. committed creates the publication an ingestable
// streams through, and the source-mutation rule is: drop what committed
// created, leave what it didn't. The ownership record lives on the publication
// itself, as its comment, naming the replication slot of the ingestable that
// owns it — the slot is the ingestable's identity on the source.
//
// An owned publication is committed's to shape: its table list is kept equal
// to the configured tables, and teardown drops it. A publication committed
// did not create (the user's) only ever gains the configured tables; committed
// never removes a table from it and never drops it.

// publicationMarkerPrefix opens the comment committed sets on a publication it
// owns; the owning slot's name follows.
const publicationMarkerPrefix = "committed: owned by the ingestable using replication slot "

func publicationMarker(slot string) string { return publicationMarkerPrefix + slot }

// errSourceMisconfigured marks a stream error no reconnect can clear: the
// worker parks until the ingestable is re-POSTed or the source is fixed.
var errSourceMisconfigured = errors.New("postgres source misconfigured")

// publication is the stream's view of the publication it reads.
type publication struct {
	owned bool
	// added are the configured tables the reconcile put into the publication,
	// which the caller backfills on a resumed slot.
	added []string
}

// ensurePublication creates the publication if it does not exist, or
// reconciles it if it does, so that every configured table is published.
//
// Without reconciliation pgoutput streams only the tables the publication had
// when first created, and two silent-row-loss cases slip through: (a) a
// re-POST adds a table — the slot already exists, so no snapshot runs either,
// and the table is never streamed; (b) a watched table is DROPped and
// reCREATEd — Postgres drops it from the publication and the new table (new
// OID) is never re-added. Comparing by OID and adding what is missing covers
// both. An owned publication also loses the tables the config no longer
// names, so a change that arrives for an unconfigured member is a fault the
// stream can report (see unroutableRelation).
//
// The publication publishes via the partition root (PostgreSQL 13+): without
// it pgoutput reports a change to a partitioned table under the partition's
// name, which no watched table matches. A partition watched on its own still
// reports as itself — the option names the topmost PUBLISHED ancestor.
//
// resumed says the ingestable has streamed before (it holds a position). A
// publication with no comment is then the one an earlier committed version
// created for it, before the ownership record existed, and is adopted;
// without a position an uncommented publication is the user's.
func ensurePublication(ctx context.Context, db *gosql.DB, pgCfg *pgConfig, resumed bool) (publication, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return publication{}, err
	}
	defer func() { _ = tx.Rollback() }()

	tableOID := make(map[string]uint32, len(pgCfg.tables))
	configured := make(map[uint32]bool, len(pgCfg.tables))
	for _, t := range pgCfg.tables {
		var oid uint32
		if err := tx.QueryRowContext(ctx, `SELECT $1::regclass::oid`, t).Scan(&oid); err != nil {
			return publication{}, fmt.Errorf("resolve table %q: %w", t, err)
		}
		tableOID[t] = oid
		configured[oid] = true
	}

	pubName := quoteIdent(pgCfg.publication)
	var comment gosql.NullString
	err = tx.QueryRowContext(ctx,
		`SELECT obj_description(oid, 'pg_publication') FROM pg_publication WHERE pubname = $1`,
		pgCfg.publication).Scan(&comment)
	if errors.Is(err, gosql.ErrNoRows) {
		return createPublication(ctx, tx, pgCfg)
	}
	if err != nil {
		return publication{}, fmt.Errorf("read publication %q: %w", pgCfg.publication, err)
	}

	var pub publication
	switch c := comment.String; {
	case c == publicationMarker(pgCfg.slotName):
		pub.owned = true
	case strings.HasPrefix(c, publicationMarkerPrefix):
		return publication{}, fmt.Errorf("%w: publication %q belongs to the ingestable using replication slot %q, not this one (slot %q); give each ingestable its own publication (options.publication)",
			errSourceMisconfigured, pgCfg.publication, strings.TrimPrefix(c, publicationMarkerPrefix), pgCfg.slotName)
	case c == "" && resumed:
		if err := markPublication(ctx, tx, pgCfg); err != nil {
			return publication{}, err
		}
		pub.owned = true
	}

	members := map[uint32]string{}
	rows, err := tx.QueryContext(ctx,
		`SELECT r.prrelid, r.prrelid::regclass::text FROM pg_publication_rel r
			JOIN pg_publication p ON p.oid = r.prpubid WHERE p.pubname = $1`,
		pgCfg.publication)
	if err != nil {
		return publication{}, fmt.Errorf("read members of publication %q: %w", pgCfg.publication, err)
	}
	for rows.Next() {
		var oid uint32
		var name string
		if err := rows.Scan(&oid, &name); err != nil {
			_ = rows.Close()
			return publication{}, err
		}
		members[oid] = name
	}
	if err := rows.Close(); err != nil {
		return publication{}, err
	}

	for _, t := range pgCfg.tables {
		if _, ok := members[tableOID[t]]; ok {
			continue
		}
		//nolint:gosec // G201: the identifiers are quoteIdent/quoteTable-escaped
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s", pubName, quoteTable(t))); err != nil {
			return publication{}, fmt.Errorf("reconcile publication: add table %s: %w", t, err)
		}
		pub.added = append(pub.added, t)
	}
	if pub.owned {
		for oid, name := range members {
			if configured[oid] {
				continue
			}
			// name is the server's own regclass rendering: quoted where needed and
			// schema-qualified when off the search_path.
			//nolint:gosec // G201: the publication is quoteIdent-escaped; name is server-rendered regclass text
			if _, err := tx.ExecContext(ctx, fmt.Sprintf("ALTER PUBLICATION %s DROP TABLE %s", pubName, name)); err != nil {
				return publication{}, fmt.Errorf("reconcile publication: drop unconfigured table %s: %w", name, err)
			}
			zap.L().Info("removed a table the ingestable no longer watches from its publication",
				zap.String("publication", pgCfg.publication), zap.String("table", name))
		}
	}
	return pub, tx.Commit()
}

func createPublication(ctx context.Context, tx *gosql.Tx, pgCfg *pgConfig) (publication, error) {
	var versionNum int
	if err := tx.QueryRowContext(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&versionNum); err != nil {
		return publication{}, fmt.Errorf("read server version: %w", err)
	}
	quoted := make([]string, len(pgCfg.tables))
	for i, t := range pgCfg.tables {
		quoted[i] = quoteTable(t)
	}
	create := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", quoteIdent(pgCfg.publication), strings.Join(quoted, ", "))
	if versionNum >= 130000 {
		create += " WITH (publish_via_partition_root = true)"
	}
	if _, err := tx.ExecContext(ctx, create); err != nil {
		return publication{}, err
	}
	if err := markPublication(ctx, tx, pgCfg); err != nil {
		return publication{}, err
	}
	// The caller's slot-is-new branch does the full snapshot; if the slot
	// somehow already exists, these are treated as newly added and backfilled.
	return publication{owned: true, added: pgCfg.tables}, tx.Commit()
}

func markPublication(ctx context.Context, tx *gosql.Tx, pgCfg *pgConfig) error {
	//nolint:gosec // G201: the publication is quoteIdent-escaped and the marker a quoted literal
	_, err := tx.ExecContext(ctx, fmt.Sprintf("COMMENT ON PUBLICATION %s IS %s",
		quoteIdent(pgCfg.publication), "'"+sqlident.EscapeStringLiteral(publicationMarker(pgCfg.slotName))+"'"))
	if err != nil {
		return fmt.Errorf("record ownership of publication %q: %w", pgCfg.publication, err)
	}
	return nil
}

// ownsPublication reports whether the publication carries this ingestable's
// ownership record. A missing publication is not owned.
func ownsPublication(ctx context.Context, db *gosql.DB, pgCfg *pgConfig) (bool, error) {
	var comment gosql.NullString
	err := db.QueryRowContext(ctx,
		`SELECT obj_description(oid, 'pg_publication') FROM pg_publication WHERE pubname = $1`,
		pgCfg.publication).Scan(&comment)
	if errors.Is(err, gosql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read publication %q: %w", pgCfg.publication, err)
	}
	return comment.String == publicationMarker(pgCfg.slotName), nil
}

// unroutableRelation decides what a change for a relation no watched table
// matches means. In a publication committed owns, the table list equals the
// configured tables, so a CURRENT member that routes nowhere is a fault — a
// partition reported under its own name, a table added behind committed's
// back — and the stream stops before acknowledging the change rather than
// drop it. A relation that is no longer a member is a table the config
// stopped watching, whose earlier changes are still in the slot's WAL; a
// user-owned publication may carry tables of its own. Both are skipped,
// logged once per relation per session.
func unroutableRelation(ctx context.Context, db *gosql.DB, pgCfg *pgConfig, owned bool, namespace, name string) error {
	table := namespace + "." + name
	if owned {
		var member bool
		if err := db.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM pg_publication_tables WHERE pubname = $1 AND schemaname = $2 AND tablename = $3)`,
			pgCfg.publication, namespace, name).Scan(&member); err != nil {
			return fmt.Errorf("read membership of %s in publication %q: %w", table, pgCfg.publication, err)
		}
		if member {
			return fmt.Errorf("%w: changes to %s arrive on publication %q, but no watched table matches them, "+
				"so ingest stopped rather than drop them; watch the table (or its partitioned root) in tables, "+
				"or remove it from the publication (a partition of a watched partitioned root reports under its own "+
				"name only when the publication lacks publish_via_partition_root: set it with ALTER PUBLICATION ... SET)",
				errSourceMisconfigured, table, pgCfg.publication)
		}
		zap.L().Info("skipping changes to a table this ingestable no longer watches",
			zap.String("table", table), zap.String("publication", pgCfg.publication))
		return nil
	}
	zap.L().Info("skipping changes to a table the ingestable does not watch; it is in a publication committed did not create",
		zap.String("table", table), zap.String("publication", pgCfg.publication))
	return nil
}
