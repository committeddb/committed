package dialects

import (
	"context"
	gosql "database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/sqlident"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

type MySQLDialect struct{}

// mysqlIdent quotes config identifiers with MySQL backtick rules.
var mysqlIdent = sqlident.MySQL

// CreateDDL implements Dialect
func (d *MySQLDialect) CreateDDL(c *sql.Config) string {
	return createDDL(c, mysqlIdent)
}

// DropDDL implements Dialect.
func (d *MySQLDialect) DropDDL(c *sql.Config) string {
	return dropDDL(c, mysqlIdent)
}

// mysqlPlaceholder renders MySQL's placeholder (always ?, position-blind).
func mysqlPlaceholder(int) string { return "?" }

// CreateDeleteSQL implements Dialect. MySQL binds the WHERE value with a ?
// placeholder.
func (d *MySQLDialect) CreateDeleteSQL(c *sql.Config) string {
	return createDeleteSQL(c, mysqlPlaceholder, mysqlIdent)
}

// CreateUpdateSQL implements Dialect: the decorator's update-only apply,
// SET placeholders first, key placeholders last (bound once — a plain
// UPDATE has none of the upsert's value doubling).
func (d *MySQLDialect) CreateUpdateSQL(c *sql.Config) string {
	return createUpdateSQL(c, mysqlPlaceholder, mysqlIdent)
}

// CreateEnrichedUpdateSQL implements Dialect: CreateUpdateSQL with enriched
// columns' SET entries as dimension subqueries, each binding one ? in SET
// position (bound once — no upsert value doubling).
func (d *MySQLDialect) CreateEnrichedUpdateSQL(config *sql.Config, enrich map[string]sql.SpineEnrichment) string {
	sets := config.Mappings[len(config.PrimaryKey):]
	var b strings.Builder
	fmt.Fprintf(&b, "UPDATE %s SET ", mysqlIdent.Table(config.Table))
	for i, m := range sets {
		if i > 0 {
			b.WriteString(",")
		}
		if e, ok := enrich[m.Column]; ok {
			fmt.Fprintf(&b, "%s=(SELECT %s->>'$.%s' FROM %s WHERE %s = ?)",
				mysqlIdent.Ident(m.Column), mysqlIdent.Ident(sql.LookupFields),
				sqlident.EscapeStringLiteral(e.SelectField), mysqlIdent.Table(e.DimTable),
				mysqlIdent.Ident(sql.LookupKey))
		} else {
			fmt.Fprintf(&b, "%s=?", mysqlIdent.Ident(m.Column))
		}
	}
	fmt.Fprintf(&b, " WHERE %s", keyWhere(config.PrimaryKey, mysqlPlaceholder, mysqlIdent))
	return b.String()
}

// CreateClearSQL implements Dialect; MySQL binds the WHERE value with ?.
func (d *MySQLDialect) CreateClearSQL(c *sql.Config, columns []string) string {
	return createClearSQL(c, columns, mysqlPlaceholder, mysqlIdent)
}

// mysqlAggSubquery re-aggregates one parent's children into a JSON array.
// JSON_ARRAYAGG ignores ORDER BY, so the rows are ordered in a derived table
// first — MySQL 8 honors that derived-table order for the aggregate in
// practice, but it is not guaranteed by the spec, so MySQL aggregate ordering
// is best-effort (PostgreSQL is the supported target for deterministic order;
// see README § Aggregate columns). COALESCE(... , JSON_ARRAY()) makes an empty
// set yield [] not NULL. <ph> binds the parent key.
func mysqlAggSubquery(spec sql.AggregateSpec, ph string) string {
	if len(spec.Enrichments) == 0 {
		sort := sql.SidecarElementKey
		if spec.NumericSort {
			sort = fmt.Sprintf("CAST(%s AS DECIMAL)", sql.SidecarElementKey)
		}
		return fmt.Sprintf(
			"(SELECT COALESCE(JSON_ARRAYAGG(%s), JSON_ARRAY()) FROM (SELECT %s,%s FROM %s WHERE %s = %s ORDER BY %s) AS ordered)",
			sql.SidecarElement, sql.SidecarElement, sql.SidecarElementKey, mysqlIdent.Table(spec.Sidecar), sql.SidecarParentKey, ph, sort)
	}

	sort := "s." + sql.SidecarElementKey
	if spec.NumericSort {
		sort = fmt.Sprintf("CAST(s.%s AS DECIMAL)", sql.SidecarElementKey)
	}
	var joins, build strings.Builder
	for i, e := range spec.Enrichments {
		alias := fmt.Sprintf("d%d", i)
		// e.Dimension is a config-derived table (quote); e.OnField is a JSON key
		// landing inside a '$.<key>' path literal (MySQL-escape the ' AND the \ so
		// it can't break out). The alias and Sidecar* columns are fixed, so they
		// stay raw.
		fmt.Fprintf(&joins, " LEFT JOIN %s %s ON s.%s->>'$.%s' = %s.%s",
			mysqlIdent.Table(e.Dimension), alias, sql.SidecarElement, sqlident.EscapeStringLiteralMySQL(e.OnField), alias, sql.LookupKey)
		for _, f := range e.Selects {
			// f.Output lands in a '<key>' object-key literal, f.Source in a
			// '$.<key>' path literal — MySQL-escape both.
			fmt.Fprintf(&build, ",'%s',JSON_EXTRACT(%s.%s,'$.%s')",
				sqlident.EscapeStringLiteralMySQL(f.Output), alias, sql.LookupFields, sqlident.EscapeStringLiteralMySQL(f.Source))
		}
	}
	element := fmt.Sprintf("JSON_MERGE_PATCH(s.%s,JSON_OBJECT(%s))", sql.SidecarElement, strings.TrimPrefix(build.String(), ","))
	return fmt.Sprintf(
		"(SELECT COALESCE(JSON_ARRAYAGG(%s), JSON_ARRAY()) FROM (SELECT %s AS %s,s.%s FROM %s s%s WHERE s.%s = %s ORDER BY %s) AS ordered)",
		sql.SidecarElement, element, sql.SidecarElement, sql.SidecarElementKey, mysqlIdent.Table(spec.Sidecar), joins.String(), sql.SidecarParentKey, ph, sort)
}

// CreateAggregateSidecarDDL implements Dialect; MySQL stores the element as
// JSON and the keys as VARCHAR(255) (a bounded type so child_key can be a
// PRIMARY KEY).
func (d *MySQLDialect) CreateAggregateSidecarDDL(spec sql.AggregateSpec) string {
	return createDDL(aggregateSidecarConfig(spec, "JSON", "VARCHAR(255)"), mysqlIdent)
}

// mysqlScalarSubquery renders one scalar aggregate fold over the sidecar's
// element JSON — the MySQL mirror of pgScalarSubquery (`->>'$.field'`
// extraction, DECIMAL(65,10) casts where the fold or ofType requires,
// where values compared as JSON text).
func mysqlScalarSubquery(spec sql.AggregateSpec, sc sql.AggregateScalar, ph string) string {
	extract := fmt.Sprintf("s.%s->>'$.%s'", sql.SidecarElement, sqlident.EscapeStringLiteralMySQL(sc.Of))
	numeric := "CAST(" + extract + " AS DECIMAL(65,10))"
	var fold string
	switch sc.Fn {
	case "count":
		fold = "COUNT(*)"
	case "sum":
		fold = "SUM(" + numeric + ")"
	case "countdistinct":
		if sc.OfNumeric {
			fold = "COUNT(DISTINCT " + numeric + ")"
		} else {
			fold = "COUNT(DISTINCT " + extract + ")"
		}
	case "min", "max":
		op := strings.ToUpper(sc.Fn)
		if sc.OfNumeric {
			fold = op + "(" + numeric + ")"
		} else {
			fold = op + "(" + extract + ")"
		}
	}
	var where strings.Builder
	for _, w := range sc.Where {
		if w.Null {
			// Absent → JSON_EXTRACT is SQL NULL; a stored JSON null →
			// JSON_TYPE 'NULL'. Both are the IS NULL reading of a mirrored
			// source's nullable column.
			f := sqlident.EscapeStringLiteralMySQL(w.Field)
			fmt.Fprintf(&where, " AND (JSON_EXTRACT(s.%s,'$.%s') IS NULL OR JSON_TYPE(JSON_EXTRACT(s.%s,'$.%s')) = 'NULL')",
				sql.SidecarElement, f, sql.SidecarElement, f)
			continue
		}
		fmt.Fprintf(&where, " AND s.%s->>'$.%s' = '%s'",
			sql.SidecarElement, sqlident.EscapeStringLiteralMySQL(w.Field),
			sqlident.EscapeStringLiteralMySQL(sql.ScalarWhereText(w.Equals)))
	}
	return fmt.Sprintf("(SELECT %s FROM %s s WHERE s.%s = %s%s)",
		fold, mysqlIdent.Table(spec.Sidecar), sql.SidecarParentKey, ph, where.String())
}

// CreateAggregateMaterializeSQL implements Dialect; the first ? binds the
// inserted parent key and every subquery ? binds it again — one per value
// column (array column, then each scalar). Table, primary key and value
// columns are config identifiers quoted for MySQL.
func (d *MySQLDialect) CreateAggregateMaterializeSQL(spec sql.AggregateSpec) string {
	table, pk := mysqlIdent.Table(spec.Table), mysqlIdent.Ident(spec.PrimaryKey)
	cols, vals, updates := []string{pk}, []string{"?"}, []string{}
	if spec.Column != "" {
		col := mysqlIdent.Ident(spec.Column)
		cols = append(cols, col)
		vals = append(vals, mysqlAggSubquery(spec, "?"))
		updates = append(updates, fmt.Sprintf("%s=VALUES(%s)", col, col))
	}
	for _, sc := range spec.Scalars {
		col := mysqlIdent.Ident(sc.Column)
		cols = append(cols, col)
		vals = append(vals, mysqlScalarSubquery(spec, sc, "?"))
		updates = append(updates, fmt.Sprintf("%s=VALUES(%s)", col, col))
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		table, strings.Join(cols, ","), strings.Join(vals, ","), strings.Join(updates, ","))
}

// CreateAggregateRebuildSQL implements Dialect; every subquery ? and the
// final WHERE ? bind the parent key.
func (d *MySQLDialect) CreateAggregateRebuildSQL(spec sql.AggregateSpec) string {
	var sets []string
	if spec.Column != "" {
		sets = append(sets, fmt.Sprintf("%s=%s", mysqlIdent.Ident(spec.Column), mysqlAggSubquery(spec, "?")))
	}
	for _, sc := range spec.Scalars {
		sets = append(sets, fmt.Sprintf("%s=%s", mysqlIdent.Ident(sc.Column), mysqlScalarSubquery(spec, sc, "?")))
	}
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s=?",
		mysqlIdent.Table(spec.Table), strings.Join(sets, ","), mysqlIdent.Ident(spec.PrimaryKey))
}

// CreateForEachChildrenSQL implements Dialect; MySQL binds the parent key
// with ?.
func (d *MySQLDialect) CreateForEachChildrenSQL(sidecar string) string {
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?",
		sql.SidecarChildKey, mysqlIdent.Table(sidecar), sql.SidecarParentKey)
}

// CreateAggregateParentLookupSQL implements Dialect; MySQL binds the child key
// with ?.
func (d *MySQLDialect) CreateAggregateParentLookupSQL(spec sql.AggregateSpec) string {
	return createAggregateParentLookupSQL(spec, "?", mysqlIdent)
}

// CreateLookupDimensionDDL implements Dialect; MySQL stores the fields as JSON
// and the key as VARCHAR(255) (a bounded type so lookup_key can be a PRIMARY
// KEY).
func (d *MySQLDialect) CreateLookupDimensionDDL(spec sql.LookupSpec) string {
	return createDDL(lookupDimensionConfig(spec, "JSON", "VARCHAR(255)"), mysqlIdent)
}

// CreateAggregateAffectedParentsSQL implements Dialect; MySQL extracts the
// element field with `->>'$.field'` and binds the changed dimension key with ?.
// onField is a JSON key inside a '$.<key>' path literal, so its single quotes AND
// backslashes are MySQL-escaped; SidecarElement is a fixed column.
func (d *MySQLDialect) CreateAggregateAffectedParentsSQL(spec sql.AggregateSpec, onField string) string {
	extract := fmt.Sprintf("%s->>'$.%s'", sql.SidecarElement, sqlident.EscapeStringLiteralMySQL(onField))
	return createAggregateAffectedParentsSQL(spec, extract, "?", mysqlIdent)
}

// CreateSQL implements Dialect
// CreateEnrichedUpsertSQL implements Dialect: mysqlUpsertSQL with enriched
// columns' entries as dimension subqueries (MySQL JSON path extract; no cast —
// MySQL assignment-coerces). The subquery repeats in the ON DUPLICATE half, so
// the doubled BindArgs convention holds uniformly: every placeholder — key
// placeholders included — appears once per half.
func (d *MySQLDialect) CreateEnrichedUpsertSQL(config *sql.Config, enrich map[string]sql.SpineEnrichment) string {
	entry := func(item sql.Mapping) string {
		if e, ok := enrich[item.Column]; ok {
			return fmt.Sprintf("(SELECT %s->>'$.%s' FROM %s WHERE %s = ?)",
				mysqlIdent.Ident(sql.LookupFields), sqlident.EscapeStringLiteral(e.SelectField),
				mysqlIdent.Table(e.DimTable), mysqlIdent.Ident(sql.LookupKey))
		}
		return "?"
	}
	var sqlb strings.Builder
	fmt.Fprintf(&sqlb, "INSERT INTO %s(", mysqlIdent.Table(config.Table))
	for i, item := range config.Mappings {
		if i > 0 {
			sqlb.WriteString(",")
		}
		sqlb.WriteString(mysqlIdent.Ident(item.Column))
	}
	fmt.Fprint(&sqlb, ") VALUES (")
	for i, item := range config.Mappings {
		if i > 0 {
			sqlb.WriteString(",")
		}
		sqlb.WriteString(entry(item))
	}
	fmt.Fprint(&sqlb, ") ON DUPLICATE KEY UPDATE ")
	for i, item := range config.Mappings {
		if i > 0 {
			sqlb.WriteString(",")
		}
		fmt.Fprintf(&sqlb, "%s=%s", mysqlIdent.Ident(item.Column), entry(item))
	}
	return sqlb.String()
}

// CreateSpineFanOutSQL implements Dialect.
func (d *MySQLDialect) CreateSpineFanOutSQL(config *sql.Config, column, onColumn string) string {
	return fmt.Sprintf("UPDATE %s SET %s = ? WHERE %s = ?",
		mysqlIdent.Table(config.Table), mysqlIdent.Ident(column), mysqlIdent.Ident(onColumn))
}

// EnsureSpineIndex implements Dialect: MySQL has no CREATE INDEX IF NOT
// EXISTS, so check information_schema first (the EnsureGenerationColumn
// pattern).
func (d *MySQLDialect) EnsureSpineIndex(ctx context.Context, db *gosql.DB, config *sql.Config, onColumn string) error {
	name := spineIndexName(config.Table, onColumn)
	var n int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?",
		config.Table, name).Scan(&n)
	if err != nil {
		return fmt.Errorf("spine index check: %w", err)
	}
	if n > 0 {
		return nil
	}
	ddl := fmt.Sprintf("CREATE INDEX %s ON %s (%s)",
		mysqlIdent.Ident(name), mysqlIdent.Table(config.Table), mysqlIdent.Ident(onColumn))
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("spine index [%s]: %w", ddl, err)
	}
	return nil
}

func (d *MySQLDialect) CreateSQL(config *sql.Config) string {
	return mysqlUpsertSQL(config, false)
}

// CreateGenerationUpsertSQL implements Dialect: CreateSQL plus the
// committed-managed generation column (last column / placeholder / update
// assignment), used only by the keyed plain Syncable so a refresh sweep can find
// stale rows. Projections and keyless syncables keep the plain CreateSQL.
func (d *MySQLDialect) CreateGenerationUpsertSQL(config *sql.Config) string {
	return mysqlUpsertSQL(config, true)
}

// mysqlUpsertSQL builds the INSERT ... ON DUPLICATE KEY UPDATE upsert.
// withGeneration appends the GenerationColumn as the final column, VALUES
// placeholder, and update assignment; BindArgs doubles every value for the
// UPDATE clause, so the appended epoch value is bound in both halves. With
// withGeneration=false the output is byte-identical to the pre-feature CreateSQL.
func mysqlUpsertSQL(config *sql.Config, withGeneration bool) string {
	var sqlb strings.Builder

	fmt.Fprintf(&sqlb, "INSERT INTO %s(", mysqlIdent.Table(config.Table))
	for i, item := range config.Mappings {
		if i == 0 {
			fmt.Fprintf(&sqlb, "%s", mysqlIdent.Ident(item.Column))
		} else {
			fmt.Fprintf(&sqlb, ",%s", mysqlIdent.Ident(item.Column))
		}
	}
	if withGeneration {
		fmt.Fprintf(&sqlb, ",%s", sql.GenerationColumn)
	}
	fmt.Fprint(&sqlb, ") VALUES (")
	n := len(config.Mappings)
	if withGeneration {
		n++
	}
	for i := 0; i < n; i++ {
		if i == 0 {
			fmt.Fprint(&sqlb, "?")
		} else {
			fmt.Fprint(&sqlb, ",?")
		}
	}
	fmt.Fprint(&sqlb, ") ON DUPLICATE KEY UPDATE ")
	for i, item := range config.Mappings {
		if i == 0 {
			fmt.Fprintf(&sqlb, "%s=?", mysqlIdent.Ident(item.Column))
		} else {
			fmt.Fprintf(&sqlb, ",%s=?", mysqlIdent.Ident(item.Column))
		}
	}
	if withGeneration {
		fmt.Fprintf(&sqlb, ",%s=?", sql.GenerationColumn)
	}

	return sqlb.String()
}

// EnsureGenerationColumn implements Dialect. MySQL has no ADD COLUMN IF NOT
// EXISTS, so it checks information_schema first and adds the column only when
// absent — idempotent across a freshly-created table (CreateDDL omits the
// column) and an upgraded pre-feature table. Existing rows baseline to
// generation 1.
func (d *MySQLDialect) EnsureGenerationColumn(ctx context.Context, db *gosql.DB, config *sql.Config) error {
	// information_schema.table_name holds only the bare table, so a
	// schema-qualified sink ("db.tbl") must bind the schema and table halves
	// apart. Binding the whole "db.tbl" to table_name never matches, so the
	// count stays 0 and every Init re-ALTERs — which succeeds the first time but
	// fails with duplicate-column on the second Init (restart), wedging the
	// keyed syncable's Init. An unqualified sink resolves against the
	// connection's current database, so it keeps DATABASE(). Both statements are
	// constant and fully parameterized (no identifier is interpolated).
	var n int
	var err error
	if dot := strings.IndexByte(config.Table, '.'); dot >= 0 {
		err = db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND column_name = ?",
			config.Table[:dot], config.Table[dot+1:], sql.GenerationColumn).Scan(&n)
	} else {
		err = db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?",
			config.Table, sql.GenerationColumn).Scan(&n)
	}
	if err != nil {
		return fmt.Errorf("ensure generation column: introspect %s: %w", config.Table, err)
	}
	if n > 0 {
		return nil
	}
	// Table is a config identifier quoted for MySQL; GenerationColumn is a package
	// constant. No user value is interpolated unquoted, so no gosec suppression.
	stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s BIGINT NOT NULL DEFAULT 1",
		mysqlIdent.Table(config.Table), sql.GenerationColumn)
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("ensure generation column [%s]: %w", stmt, err)
	}
	return nil
}

// CreateGenerationSweepSQL implements Dialect; MySQL binds the epoch with ?.
func (d *MySQLDialect) CreateGenerationSweepSQL(config *sql.Config) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s >= 1 AND %s < ?",
		mysqlIdent.Table(config.Table), sql.GenerationColumn, sql.GenerationColumn)
}

// CreateAppliedSidecarDDL implements Dialect: the dedup sidecar for a keyless
// (append) syncable — (committed_index, committed_seq) under a composite PK.
func (d *MySQLDialect) CreateAppliedSidecarDDL(config *sql.Config) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (%s BIGINT NOT NULL,%s INT NOT NULL,PRIMARY KEY (%s,%s));",
		mysqlIdent.Table(sql.AppliedSidecarName(config.Table)),
		sql.AppliedIndexColumn, sql.AppliedSeqColumn,
		sql.AppliedIndexColumn, sql.AppliedSeqColumn)
}

// CreateAppliedMarkSQL implements Dialect: INSERT IGNORE, so RowsAffected is 1 on
// a first apply and 0 on a replay of the same (index, seq).
func (d *MySQLDialect) CreateAppliedMarkSQL(config *sql.Config) string {
	return fmt.Sprintf(
		"INSERT IGNORE INTO %s (%s,%s) VALUES (?,?)",
		mysqlIdent.Table(sql.AppliedSidecarName(config.Table)), sql.AppliedIndexColumn, sql.AppliedSeqColumn)
}

func (d *MySQLDialect) Open(connectionString string) (*gosql.DB, error) {
	// Connection strings are canonically mysql:// URLs everywhere (ingest AND
	// syncable). cluster.ParseMySQLConn is the single parse authority the ingest
	// side uses too, and it carries the TLS posture (sslmode / custom CA / client
	// cert) into mysql.Config.TLS — the same *tls.Config the ingest snapshot and
	// CDC stream use — so a URL means, and secures, the same thing wherever it is
	// opened. (Twin of ingestable/sql/mysql.openMySQL; the shared TLS logic lives
	// in cluster.MySQLConn.) A malformed/TLS-misconfigured URL yields a
	// redaction-safe error (never echoes the ${VAR}-resolved string).
	conn, err := cluster.ParseMySQLConn(connectionString)
	if err != nil {
		return nil, err
	}
	tlsCfg, err := conn.TLSClientConfig()
	if err != nil {
		return nil, err
	}
	cfg := mysql.NewConfig()
	cfg.User = conn.User
	cfg.Passwd = conn.Password
	cfg.Net = "tcp"
	cfg.Addr = conn.Addr()
	cfg.DBName = conn.Database
	cfg.TLS = tlsCfg
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	return gosql.OpenDB(connector), nil
}

// IsPermanent classifies a MySQL error as permanent (non-retryable) only when
// it is unambiguously about the data or schema — the bad proposal will never
// apply no matter how many times we retry. MySQL doesn't use SQLSTATE classes
// the way PostgreSQL does, so this is an explicit allowlist of error numbers.
//
// Everything NOT listed stays transient and retries forever, by design: a
// wrongly-permanent error silently drops data past the dead letter, while a
// wrongly-transient one only wedges the worker visibly for an operator to
// skip. So infrastructure errors are deliberately absent and stay transient —
// 1205 lock wait timeout, 1213 deadlock, 1040/1203 too many connections,
// 2006/2013 server gone / lost connection, 1317 query interrupted. See the
// asymmetric-risk principle in the sync-permanent-error-classification ticket.
func (d *MySQLDialect) IsPermanent(err error) bool {
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	switch mysqlErr.Number {
	// Data: a specific row's value is bad and will never apply.
	case 1048, // Column cannot be null
		1264, // Out of range value for column
		1265, // Data truncated for column
		1292, // Truncated incorrect value (bad date/number literal)
		1366, // Incorrect value for column (charset/type)
		1406, // Data too long for column
		1690: // Numeric value out of range (e.g. BIGINT overflow)
		return true
	// Constraint: THIS row violates an integrity constraint (entry-specific).
	case 1062, // Duplicate entry (only reachable on the no-PK path; upsert masks it otherwise)
		1452, // FK constraint fails (matches PostgreSQL class 23; see the FK note below)
		3819, // Check constraint violated
		4025: // CHECK constraint is violated (column-level; MySQL 8.0.16+)
		return true
	}
	// Deliberately NOT permanent (entry-specific rule, cluster.ErrPermanent):
	// 1054 unknown column, 1136 column-count mismatch, and 1364 field-has-no-
	// default are SCHEMA / MAPPING shaped — they fail EVERY row identically (a
	// destination column dropped, an operator ALTER, a sink/mapping mismatch),
	// not a bad row value. They stay transient so the worker wedges visibly and
	// resumes on the fix, dead-lettering nothing — the MySQL mirror of the
	// Postgres class-42 carve-out and the webhook 401/403 carve-out.
	// FK note: 1452 / PostgreSQL 23503 are treated permanent for parity. A FK
	// failure *could* be transient if the parent row is synced later by
	// another syncable, but committed has no cross-syncable ordering guarantee
	// to lean on, FKs on projection tables are an advanced opt-in, and both
	// dialects classify it the same way — flip both together if a deployment
	// needs FK-as-transient.
	return false
}

// BindArgs doubles the values: CreateSQL emits ? placeholders for both the
// INSERT VALUES list and the ON DUPLICATE KEY UPDATE clause, so each column
// value is bound twice.
func (d *MySQLDialect) BindArgs(values []any) []any {
	return append(values, values...)
}
