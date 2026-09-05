// Package sqlserver implements the SQL Server ingest dialect over Change
// Tracking (CT) — the capture mechanism available on EVERY edition, Web and
// Express included, where CDC is licence-gated to Standard/Enterprise.
//
// CT yields net changes per row since a version: primary keys plus operation
// (insert/update/delete). Upserts join the base table for current values —
// the SAME database/sql read path the snapshot uses, so snapshot and change
// payloads are byte-identical by construction. Deletes arrive PK-only, which
// is exactly committed's keyed-tombstone contract. What CT does not carry is
// history fidelity: intermediate values between polls are never observed —
// this dialect is a convergent replicator (committed's documented ingest
// contract: last-write-wins keyed re-observations), not a change-event
// historian.
//
// The cursor is CHANGE_TRACKING_CURRENT_VERSION(): a per-database monotonic
// bigint incrementing per committed transaction — so lag reads in
// transactions (LagUnitTransactions), matching MySQL-GTID. The purge hole is
// CHANGE_TRACKING_MIN_VALID_VERSION(table) climbing past the consumed
// version (retention cleanup), answered like binlog expiry: a loud automatic
// re-snapshot at a bumped refresh epoch.
package sqlserver

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"time"

	mssql "github.com/microsoft/go-mssqldb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
	"github.com/committeddb/committed/internal/cluster/sqlident"
)

// SQLServerDialect implements sql.Dialect for SQL Server sources via Change
// Tracking. Stateless — per-worker state lives in Ingest's locals.
type SQLServerDialect struct {
	// Features is the cluster feature-level gate (db.FeatureEnabled), wired by
	// the node. It decides the uniqueidentifier rendering for each session —
	// see featureLevelCanonicalUUID. Nil (tests constructing the dialect
	// directly) means the gate is open: the 0.8.0 rendering.
	Features FeatureReader
	// snapshotBatchHook is test-only failure injection for resume tests (the
	// same seam the other dialects expose). Nil in production.
	snapshotBatchHook func(table string, batch int) error
}

const (
	// statusQueryTimeout bounds Status/Preflight source queries — read
	// endpoints must not hang on an unreachable source.
	statusQueryTimeout = 5 * time.Second
	// defaultPollInterval is the CT poll cadence when the config sets none.
	// Seconds-scale, comparable to the sync workers' backoff ceiling: read
	// models trail the source by roughly this much at rest.
	defaultPollInterval = 3 * time.Second
	// ctOwnerProperty is the extended-property marker committed stamps on a
	// table whose CT it enabled. Ownership must be REMEMBERED, not inferred —
	// CT is a shared table property, not an owned named object like a PG
	// slot — and TeardownSource receives only the config, so the record
	// lives on the source, with the thing it records. The etiquette:
	// committed may turn ON what is off, and only ever turns OFF what this
	// marker proves it turned on itself.
	ctOwnerProperty = "committed_ct_enabled"
)

// ctLivenessTimeout bounds readCTState's per-cycle catalog queries — the
// poll loop's half-open-socket guard (see readCTState). A var so tests can
// lower it to red-proof the deadline without a 30s wait.
var ctLivenessTimeout = 30 * time.Second

// ctEnableTimeout bounds ensureChangeTracking — the RETRY PREAMBLE's
// half-open-socket guard, closing the gap the first liveness sweep missed
// (field-found: a TDS-cancellation failure warned once, then ingestOnce's
// retry blocked ~56 minutes inside ensureChangeTracking's first unbounded
// query — silent, because the stall was INSIDE one attempt, before the next
// per-attempt warn could print). Bounding the preamble restores both
// liveness and the loop's visible retry cadence. Generous, because the
// ALTERs here do real work (enabling DB-level CT touches every connection's
// session state); a var so tests can red-proof without the wait.
var ctEnableTimeout = 60 * time.Second

// quoter is the identifier quoting for composed SQL. Sessions run with
// QUOTED_IDENTIFIER ON (the driver default), so the ISO "..." form is safe.
var quoter = sqlident.SQLServer

// openSQLServer validates the canonical URL scheme and opens a pooled handle.
// go-mssqldb accepts sqlserver:// URLs natively, so no DSN conversion.
// cluster.ParseConnString is the mandatory choke point: its error path never
// carries the secret-bearing string (the forbidigo rule that steers here).
func openSQLServer(connString string) (*gosql.DB, error) {
	u, err := cluster.ParseConnString(connString)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "sqlserver" {
		return nil, fmt.Errorf("connection string must be a sqlserver:// URL")
	}
	return gosql.Open("sqlserver", connString)
}

// tableRef is a watched table resolved to (schema, name). A bare configured
// name scopes to dbo, mirroring how the MySQL dialect scopes bare names to
// the DSN database; a "schema.table" entry keeps its schema.
type tableRef struct{ schema, name string }

func resolveTableRef(configured string) tableRef {
	if i := strings.IndexByte(configured, '.'); i >= 0 {
		return tableRef{schema: configured[:i], name: configured[i+1:]}
	}
	return tableRef{schema: "dbo", name: configured}
}

// qualified returns the quoted schema.name form for composed SQL.
func (r tableRef) qualified() string {
	return quoter.Ident(r.schema) + "." + quoter.Ident(r.name)
}

// objectID renders the unquoted schema.name form OBJECT_ID() and the
// change-tracking catalog views key on (bound as a parameter, never
// interpolated).
func (r tableRef) objectID() string { return r.schema + "." + r.name }

// pollInterval reads the poll cadence option, defaulting sanely. Invalid
// values were rejected at parse time by validateOptions-style checks; here a
// bad value just falls back to the default.
func pollInterval(options map[string]string) time.Duration {
	if v := options["poll_interval"]; v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return defaultPollInterval
}

// Preflight validates the source can be ingested safely — read-only, because
// it runs on every node at config build; anything that mutates the source
// (CT enablement) happens once, in Ingest, on the owning node. It verifies
// each watched table exists and that its REAL primary key covers the
// configured primaryKey: CT change rows carry exactly the table's PK
// columns, so that coverage is what guarantees a delete can always be keyed
// (the tombstone contract).
func (d *SQLServerDialect) Preflight(config *sql.Config) error {
	config.EnsureTopics()
	ctx, cancel := context.WithTimeout(context.Background(), statusQueryTimeout)
	defer cancel()

	db, err := openSQLServer(config.ConnectionString)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	for ti := range config.Topics {
		spec := &config.Topics[ti]
		for _, table := range spec.Tables {
			ref := resolveTableRef(table)
			pkCols, err := tablePrimaryKey(ctx, db, ref)
			if err != nil {
				return fmt.Errorf("[sqlserver.preflight] table %s: %w", table, err)
			}
			if len(pkCols) == 0 {
				return fmt.Errorf(
					"[sqlserver.preflight] table %s has no primary key; Change Tracking requires one, and committed needs it to key delete tombstones — add a primary key covering the configured primaryKey", table)
			}
			if err := sql.CheckKeyCoverage(spec.PrimaryKey, pkCols, table,
				"configure primaryKey to columns of the table's PRIMARY KEY (Change Tracking change rows carry exactly those columns)"); err != nil {
				return fmt.Errorf("[sqlserver.preflight] %w", err)
			}
		}
	}
	return nil
}

// tablePrimaryKey returns the table's primary-key column names in key order,
// or empty when the table has no PK. Errors when the table does not exist.
func tablePrimaryKey(ctx context.Context, db *gosql.DB, ref tableRef) ([]string, error) {
	var objectID int64
	err := db.QueryRowContext(ctx,
		"SELECT OBJECT_ID(@p1)", ref.objectID()).Scan(&objectID)
	if err != nil || objectID == 0 {
		return nil, fmt.Errorf("table not found (schema %s)", ref.schema)
	}
	rows, err := db.QueryContext(ctx, `
		SELECT c.name
		FROM sys.indexes i
		JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
		JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
		WHERE i.object_id = OBJECT_ID(@p1) AND i.is_primary_key = 1
		ORDER BY ic.key_ordinal`, ref.objectID())
	if err != nil {
		return nil, fmt.Errorf("read primary key: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

// SourceColumns returns, per configured table, the column names in source
// order and the computed-column subset (SQL Server's generated columns:
// absent from meaningful replication, excluded from MapAllColumns, rejected
// if explicitly mapped — the shared contract).
func (d *SQLServerDialect) SourceColumns(config *sql.Config) (columns, generated map[string][]string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), statusQueryTimeout)
	defer cancel()

	db, err := openSQLServer(config.ConnectionString)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = db.Close() }()

	columns = make(map[string][]string, len(config.Tables))
	generated = make(map[string][]string)
	for _, table := range config.Tables {
		ref := resolveTableRef(table)
		cols, gen, err := sourceTableColumns(ctx, db, ref)
		if err != nil {
			return nil, nil, fmt.Errorf("[sqlserver] columns of %s: %w", table, err)
		}
		if len(cols) == 0 {
			return nil, nil, fmt.Errorf("[sqlserver] table %s not found (schema %s)", table, ref.schema)
		}
		columns[table] = cols
		if len(gen) > 0 {
			generated[table] = gen
		}
	}
	return columns, generated, nil
}

// sourceTableColumns reads one table's column names in source order and its
// computed-column subset from sys.columns. Empty cols means the table does
// not exist (OBJECT_ID resolved to NULL). Shared by SourceColumns and the
// poll loop's key-drift classifier.
func sourceTableColumns(ctx context.Context, db *gosql.DB, ref tableRef) (cols, generated []string, err error) {
	rows, err := db.QueryContext(ctx, `
		SELECT name, is_computed
		FROM sys.columns
		WHERE object_id = OBJECT_ID(@p1)
		ORDER BY column_id`, ref.objectID())
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name string
		var computed bool
		if err := rows.Scan(&name, &computed); err != nil {
			return nil, nil, err
		}
		cols = append(cols, name)
		if computed {
			generated = append(generated, name)
		}
	}
	return cols, generated, rows.Err()
}

// Status decodes the checkpoint into a point-in-time IngestableStatus and,
// when streaming, diffs the consumed CT version against the source's current
// version for a transaction-count lag — plus the retention check: any
// watched table whose minimum valid version has climbed past the consumed
// version is the purge hole, surfaced as the distinct re-snapshot state
// rather than an understated lag. Source-query failures leave Lag nil (soft,
// logged at Debug), same posture as the other dialects.
func (d *SQLServerDialect) Status(ctx context.Context, config *sql.Config, pos cluster.Position) (cluster.IngestableStatus, error) {
	var version uint64
	decode := func(pos cluster.Position) (sql.StatusInputs, error) {
		posProto := &dialectpb.SQLServerPosition{}
		if err := proto.Unmarshal(pos, posProto); err != nil {
			return sql.StatusInputs{}, fmt.Errorf("[sqlserver.status] decode position: %w", err)
		}
		version = posProto.Version
		return sql.StatusInputs{Position: fmt.Sprintf("ct:%d", posProto.Version), Progress: posProto.SnapshotProgress}, nil
	}
	probe := func(ctx context.Context, status *cluster.IngestableStatus) {
		cctx, cancel := context.WithTimeout(ctx, statusQueryTimeout)
		defer cancel()
		db, err := openSQLServer(config.ConnectionString)
		if err != nil {
			zap.L().Debug("[sqlserver.status] open failed", zap.Error(err))
			return
		}
		defer func() { _ = db.Close() }()
		current, minValid, err := readCTState(cctx, db, config)
		if err != nil {
			zap.L().Debug("[sqlserver.status] ct state query failed", zap.Error(err))
			return
		}
		if minValid > version {
			status.ReSnapshotRequired = true
			return
		}
		var lag uint64
		if current > version {
			lag = current - version
		}
		status.Lag = &lag
		status.LagUnit = cluster.LagUnitTransactions
		status.CaughtUp = lag == 0
	}
	return sql.RenderStatus(ctx, config, pos, decode, probe)
}

// readCTState reads the source's current CT version and the MAXIMUM of the
// watched tables' minimum valid versions (the tightest retention floor — if
// any one table has purged past the consumed version, streaming cannot close
// that table's gap and the whole ingestable re-snapshots, the shared
// failure-domain contract).
func readCTState(ctx context.Context, db *gosql.DB, config *sql.Config) (current, minValid uint64, err error) {
	// The half-open-socket guard, poll-loop edition. These tiny catalog
	// queries run every poll cycle, so a bounded deadline here means the
	// worker can NEVER silently freeze on a dead connection — the deadline
	// fires, the error routes into the existing backoff/reconnect, and the
	// next attempt draws a fresh pooled connection. The DATA query
	// (pollTable's CHANGETABLE read) deliberately carries no fixed deadline:
	// a large catch-up window must not livelock against one, and with this
	// per-cycle guard a dead socket is caught within one cycle either way.
	// (MySQL's analogue is the binlog heartbeat+read-deadline pair; Postgres
	// is bidirectional and self-detects via standby-status writes.)
	ctx, cancel := context.WithTimeout(ctx, ctLivenessTimeout)
	defer cancel()
	var cur gosql.NullInt64
	if err := db.QueryRowContext(ctx, "SELECT CHANGE_TRACKING_CURRENT_VERSION()").Scan(&cur); err != nil {
		return 0, 0, fmt.Errorf("current version: %w", err)
	}
	current, err = ctVersion(cur, "database")
	if err != nil {
		return 0, 0, err
	}
	for _, table := range config.Tables {
		ref := resolveTableRef(table)
		var mv gosql.NullInt64
		if err := db.QueryRowContext(ctx,
			"SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(@p1))", ref.objectID()).Scan(&mv); err != nil {
			return 0, 0, fmt.Errorf("min valid version of %s: %w", table, err)
		}
		v, err := ctVersion(mv, table)
		if err != nil {
			return 0, 0, err
		}
		if v > minValid {
			minValid = v
		}
	}
	return current, minValid, nil
}

// ctVersion converts a CT catalog version to uint64: NULL means change
// tracking is not enabled on the subject, and a negative value is a
// contract violation from the server/driver — refuse rather than wrap
// around (the G115 posture).
func ctVersion(v gosql.NullInt64, subject string) (uint64, error) {
	if !v.Valid {
		return 0, fmt.Errorf("change tracking is not enabled on %s", subject)
	}
	if v.Int64 < 0 {
		return 0, fmt.Errorf("change tracking version of %s is negative (%d)", subject, v.Int64)
	}
	return uint64(v.Int64), nil
}

// ensureChangeTracking turns on what is off — database-level CT (with a
// 2-day auto-cleanup retention, the SQL Server default) and per-table CT —
// stamping the ownership marker on each table committed itself enables.
// Pre-existing CT is left untouched and unmarked. Runs at Ingest start on
// the owning node only (never in Preflight, which every node runs at config
// build). Idempotent.
func ensureChangeTracking(ctx context.Context, db *gosql.DB, config *sql.Config) error {
	// The retry-preamble deadline (see ctEnableTimeout): every query and ALTER
	// below is bounded so a wedged connection surfaces as an error into the
	// backoff loop within one minute — never a silent indefinite stall.
	ctx, cancel := context.WithTimeout(ctx, ctEnableTimeout)
	defer cancel()
	var dbEnabled int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sys.change_tracking_databases WHERE database_id = DB_ID()").Scan(&dbEnabled)
	if err != nil {
		return fmt.Errorf("check database change tracking: %w", err)
	}
	if dbEnabled == 0 {
		if _, err := db.ExecContext(ctx,
			"ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)"); err != nil {
			return fmt.Errorf(
				"enable database change tracking (grant the ingest user ALTER on the database, or have a DBA run: ALTER DATABASE <db> SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)): %w", err)
		}
		// Database-level CT is enable-only by policy: committed never disables
		// it (other consumers and other committed ingestables may rely on it,
		// and SQL Server requires every table's CT off first anyway).
		zap.L().Info("[sqlserver] enabled database-level change tracking (2-day retention, auto cleanup)")
	}

	for _, table := range config.Tables {
		ref := resolveTableRef(table)
		var tblEnabled int
		if err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM sys.change_tracking_tables WHERE object_id = OBJECT_ID(@p1)",
			ref.objectID()).Scan(&tblEnabled); err != nil {
			return fmt.Errorf("check change tracking on %s: %w", table, err)
		}
		if tblEnabled != 0 {
			continue // pre-existing (or previously enabled by us): never re-mark
		}
		if _, err := db.ExecContext(ctx,
			fmt.Sprintf("ALTER TABLE %s ENABLE CHANGE_TRACKING", ref.qualified())); err != nil {
			return fmt.Errorf(
				"enable change tracking on %s (grant ALTER on the table, or have a DBA run: ALTER TABLE %s ENABLE CHANGE_TRACKING): %w",
				table, ref.objectID(), err)
		}
		if _, err := db.ExecContext(ctx,
			"EXEC sp_addextendedproperty @name = @p1, @value = 'true', @level0type = 'SCHEMA', @level0name = @p2, @level1type = 'TABLE', @level1name = @p3",
			ctOwnerProperty, ref.schema, ref.name); err != nil {
			return fmt.Errorf("mark change-tracking ownership on %s: %w", table, err)
		}
		zap.L().Info("[sqlserver] enabled change tracking", zap.String("table", table))
	}
	return nil
}

// TeardownSource disables Change Tracking ONLY on tables carrying committed's
// ownership marker — the tables ensureChangeTracking itself enabled — and
// removes the marker. Pre-existing CT (no marker) is never touched, and
// database-level CT is never disabled (enable-only policy). Idempotent and
// ctx-bounded; runs on the owner after the logical delete.
func (d *SQLServerDialect) TeardownSource(config *sql.Config) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := openSQLServer(config.ConnectionString)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	for _, table := range config.Tables {
		ref := resolveTableRef(table)
		var marked int
		err := db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM sys.extended_properties
			WHERE class = 1 AND major_id = OBJECT_ID(@p1) AND minor_id = 0 AND name = @p2`,
			ref.objectID(), ctOwnerProperty).Scan(&marked)
		if err != nil {
			return fmt.Errorf("[sqlserver.teardown] read ownership marker on %s: %w", table, err)
		}
		if marked == 0 {
			continue // we did not enable it; not ours to disable
		}
		if _, err := db.ExecContext(ctx,
			fmt.Sprintf("ALTER TABLE %s DISABLE CHANGE_TRACKING", ref.qualified())); err != nil {
			return fmt.Errorf("[sqlserver.teardown] disable change tracking on %s: %w", table, err)
		}
		if _, err := db.ExecContext(ctx,
			"EXEC sp_dropextendedproperty @name = @p1, @level0type = 'SCHEMA', @level0name = @p2, @level1type = 'TABLE', @level1name = @p3",
			ctOwnerProperty, ref.schema, ref.name); err != nil {
			return fmt.Errorf("[sqlserver.teardown] drop ownership marker on %s: %w", table, err)
		}
		zap.L().Info("[sqlserver] disabled committed-enabled change tracking", zap.String("table", table))
	}
	return nil
}

// FeatureReader answers whether the whole cluster is at a feature level —
// the db's cluster-minimum gate, sized to this dialect's one question.
type FeatureReader interface {
	FeatureEnabled(level uint64) bool
}

// featureLevelCanonicalUUID gates the canonical (RFC 4122 lowercase)
// uniqueidentifier rendering. The spelling is in entity KEYS: a cluster where
// one owner renders uppercase and the next lowercase would key the same row
// twice, so every node keeps the pre-0.8.0 uppercase rendering until every
// member announces level 5. See version.FeatureLevel and sessionUUIDRendering.
const featureLevelCanonicalUUID uint64 = 5

// The two uniqueidentifier renderings a checkpoint can record
// (SQLServerPosition.uuid_rendering).
const (
	uuidRenderingLegacy    uint32 = 0 // the driver's UPPERCASE GUID; every pre-field checkpoint
	uuidRenderingCanonical uint32 = 1 // RFC 4122 lowercase — what PostgreSQL's uuid ingests as
)

// canonicalUUIDEnabled reports whether this node may render canonically.
func (d *SQLServerDialect) canonicalUUIDEnabled() bool {
	return d.Features == nil || d.Features.FeatureEnabled(featureLevelCanonicalUUID)
}

// sessionUUIDRendering decides one session's uniqueidentifier rendering from
// the checkpoint it resumes and the cluster gate, and whether the session must
// first re-snapshot (rerender): the checkpoint was spelled the OTHER way, so
// every uniqueidentifier key and payload field on the sink is the old spelling
// and only a full re-emission at a bumped epoch (whose closing markers sweep
// the old rows on keyed sinks) re-keys it. The promotion is one-way: a
// canonical checkpoint stays canonical even if the gate reads closed (a
// member mid-join announces level 0 for a moment), so a membership change can
// never flip a sink back to uppercase.
func sessionUUIDRendering(checkpoint uint32, hasCheckpoint, gateOpen bool) (rendering uint32, rerender bool) {
	if checkpoint == uuidRenderingCanonical {
		return uuidRenderingCanonical, false
	}
	if gateOpen {
		return uuidRenderingCanonical, hasCheckpoint
	}
	return uuidRenderingLegacy, false
}

// renderUniqueidentifier spells a GUID for this session: the driver's
// uppercase form (legacy), or RFC 4122 lowercase (canonical).
func renderUniqueidentifier(u mssql.UniqueIdentifier, canonical bool) string {
	if canonical {
		return strings.ToLower(u.String())
	}
	return u.String()
}

// columnCategories maps a result set's driver type metadata onto the shared
// JSON categories, keyed by LOWERCASED column name (the shared decode
// contract). uniqueidentifier is flagged for the mixed-endian byte fix (see
// scanRowValues).
func columnCategories(types []*gosql.ColumnType) (cats map[string]sql.JSONCategory, uuidCols map[string]bool) {
	cats = make(map[string]sql.JSONCategory, len(types))
	uuidCols = make(map[string]bool)
	for _, t := range types {
		name := strings.ToLower(t.Name())
		switch strings.ToUpper(t.DatabaseTypeName()) {
		case "BIT":
			cats[name] = sql.CatBool
		case "TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL", "NUMERIC", "FLOAT", "REAL", "MONEY", "SMALLMONEY":
			cats[name] = sql.CatNumber
		case "BINARY", "VARBINARY", "IMAGE", "ROWVERSION", "TIMESTAMP":
			cats[name] = sql.CatBinary
		case "UNIQUEIDENTIFIER":
			cats[name] = sql.CatText
			uuidCols[name] = true
		default:
			// char/varchar/nchar/nvarchar/text/ntext, date/time/datetime*,
			// datetimeoffset, xml, sql_variant → text. JSON-in-nvarchar needs
			// the per-column hint (open decision) before CatJSON applies.
			cats[name] = sql.CatText
		}
	}
	return cats, uuidCols
}

// scanRowValues scans the current row into a lowercased-column value map,
// fixing the uniqueidentifier representation: the driver hands GUIDs back as
// their 16 raw mixed-endian bytes, which would render as garbage text —
// convert to the session's string form the same way on every read path so
// snapshot and CT payloads agree byte-for-byte (see renderUniqueidentifier).
func scanRowValues(rows *gosql.Rows, cols []string, uuidCols map[string]bool, canonicalUUID bool) (map[string]any, error) {
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	m := make(map[string]any, len(cols))
	for i, c := range cols {
		name := strings.ToLower(c)
		v := vals[i]
		if uuidCols[name] {
			if b, ok := v.([]byte); ok && len(b) == 16 {
				var u mssql.UniqueIdentifier
				if err := u.Scan(b); err == nil {
					v = renderUniqueidentifier(u, canonicalUUID)
				}
			}
		}
		m[name] = v
	}
	return m, nil
}
