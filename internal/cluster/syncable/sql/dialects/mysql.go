package dialects

import (
	gosql "database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

type MySQLDialect struct{}

// CreateDDL implements Dialect
func (d *MySQLDialect) CreateDDL(c *sql.Config) string {
	return createDDL(c)
}

// DropDDL implements Dialect.
func (d *MySQLDialect) DropDDL(c *sql.Config) string {
	return dropDDL(c)
}

// CreateDeleteSQL implements Dialect. MySQL binds the WHERE value with a ?
// placeholder.
func (d *MySQLDialect) CreateDeleteSQL(c *sql.Config) string {
	return createDeleteSQL(c, "?")
}

// CreateClearSQL implements Dialect; MySQL binds the WHERE value with ?.
func (d *MySQLDialect) CreateClearSQL(c *sql.Config, columns []string) string {
	return createClearSQL(c, columns, "?")
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
			sql.SidecarElement, sql.SidecarElement, sql.SidecarElementKey, spec.Sidecar, sql.SidecarParentKey, ph, sort)
	}

	sort := "s." + sql.SidecarElementKey
	if spec.NumericSort {
		sort = fmt.Sprintf("CAST(s.%s AS DECIMAL)", sql.SidecarElementKey)
	}
	var joins, build strings.Builder
	for i, e := range spec.Enrichments {
		alias := fmt.Sprintf("d%d", i)
		fmt.Fprintf(&joins, " LEFT JOIN %s %s ON s.%s->>'$.%s' = %s.%s",
			e.Dimension, alias, sql.SidecarElement, e.OnField, alias, sql.LookupKey)
		for _, f := range e.Selects {
			fmt.Fprintf(&build, ",'%s',JSON_EXTRACT(%s.%s,'$.%s')", f.Output, alias, sql.LookupFields, f.Source)
		}
	}
	element := fmt.Sprintf("JSON_MERGE_PATCH(s.%s,JSON_OBJECT(%s))", sql.SidecarElement, strings.TrimPrefix(build.String(), ","))
	return fmt.Sprintf(
		"(SELECT COALESCE(JSON_ARRAYAGG(%s), JSON_ARRAY()) FROM (SELECT %s AS %s,s.%s FROM %s s%s WHERE s.%s = %s ORDER BY %s) AS ordered)",
		sql.SidecarElement, element, sql.SidecarElement, sql.SidecarElementKey, spec.Sidecar, joins.String(), sql.SidecarParentKey, ph, sort)
}

// CreateAggregateSidecarDDL implements Dialect; MySQL stores the element as
// JSON and the keys as VARCHAR(255) (a bounded type so child_key can be a
// PRIMARY KEY).
func (d *MySQLDialect) CreateAggregateSidecarDDL(spec sql.AggregateSpec) string {
	return createDDL(aggregateSidecarConfig(spec, "JSON", "VARCHAR(255)"))
}

// CreateAggregateMaterializeSQL implements Dialect; both ? placeholders bind
// the parent key (the inserted row's key and the subquery filter).
func (d *MySQLDialect) CreateAggregateMaterializeSQL(spec sql.AggregateSpec) string {
	return fmt.Sprintf(
		"INSERT INTO %s (%s,%s) VALUES (?,%s) ON DUPLICATE KEY UPDATE %s=VALUES(%s)",
		spec.Table, spec.PrimaryKey, spec.Column, mysqlAggSubquery(spec, "?"),
		spec.Column, spec.Column)
}

// CreateAggregateRebuildSQL implements Dialect; both ? placeholders bind the
// parent key (the subquery filter and the WHERE).
func (d *MySQLDialect) CreateAggregateRebuildSQL(spec sql.AggregateSpec) string {
	return fmt.Sprintf("UPDATE %s SET %s=%s WHERE %s=?",
		spec.Table, spec.Column, mysqlAggSubquery(spec, "?"), spec.PrimaryKey)
}

// CreateAggregateParentLookupSQL implements Dialect; MySQL binds the child key
// with ?.
func (d *MySQLDialect) CreateAggregateParentLookupSQL(spec sql.AggregateSpec) string {
	return createAggregateParentLookupSQL(spec, "?")
}

// CreateLookupDimensionDDL implements Dialect; MySQL stores the fields as JSON
// and the key as VARCHAR(255) (a bounded type so lookup_key can be a PRIMARY
// KEY).
func (d *MySQLDialect) CreateLookupDimensionDDL(spec sql.LookupSpec) string {
	return createDDL(lookupDimensionConfig(spec, "JSON", "VARCHAR(255)"))
}

// CreateAggregateAffectedParentsSQL implements Dialect; MySQL extracts the
// element field with `->>'$.field'` and binds the changed dimension key with ?.
func (d *MySQLDialect) CreateAggregateAffectedParentsSQL(spec sql.AggregateSpec, onField string) string {
	extract := fmt.Sprintf("%s->>'$.%s'", sql.SidecarElement, onField)
	return createAggregateAffectedParentsSQL(spec, extract, "?")
}

// CreateSQL implements Dialect
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

	fmt.Fprintf(&sqlb, "INSERT INTO %s(", config.Table)
	for i, item := range config.Mappings {
		if i == 0 {
			fmt.Fprintf(&sqlb, "%s", item.Column)
		} else {
			fmt.Fprintf(&sqlb, ",%s", item.Column)
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
			fmt.Fprintf(&sqlb, "%s=?", item.Column)
		} else {
			fmt.Fprintf(&sqlb, ",%s=?", item.Column)
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
func (d *MySQLDialect) EnsureGenerationColumn(db *gosql.DB, config *sql.Config) error {
	var n int
	err := db.QueryRow(
		"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?",
		config.Table, sql.GenerationColumn).Scan(&n)
	if err != nil {
		return fmt.Errorf("ensure generation column: introspect %s: %w", config.Table, err)
	}
	if n > 0 {
		return nil
	}
	//nolint:gosec // G201: table is a config identifier, GenerationColumn is a package constant — no user value interpolated
	stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s BIGINT NOT NULL DEFAULT 1",
		config.Table, sql.GenerationColumn)
	if _, err := db.Exec(stmt); err != nil {
		return fmt.Errorf("ensure generation column [%s]: %w", stmt, err)
	}
	return nil
}

// CreateGenerationSweepSQL implements Dialect; MySQL binds the epoch with ?.
func (d *MySQLDialect) CreateGenerationSweepSQL(config *sql.Config) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s >= 1 AND %s < ?",
		config.Table, sql.GenerationColumn, sql.GenerationColumn)
}

// CreateAppliedSidecarDDL implements Dialect: the dedup sidecar for a keyless
// (append) syncable — (committed_index, committed_seq) under a composite PK.
func (d *MySQLDialect) CreateAppliedSidecarDDL(config *sql.Config) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (%s BIGINT NOT NULL,%s INT NOT NULL,PRIMARY KEY (%s,%s));",
		sql.AppliedSidecarName(config.Table),
		sql.AppliedIndexColumn, sql.AppliedSeqColumn,
		sql.AppliedIndexColumn, sql.AppliedSeqColumn)
}

// CreateAppliedMarkSQL implements Dialect: INSERT IGNORE, so RowsAffected is 1 on
// a first apply and 0 on a replay of the same (index, seq).
func (d *MySQLDialect) CreateAppliedMarkSQL(config *sql.Config) string {
	return fmt.Sprintf(
		"INSERT IGNORE INTO %s (%s,%s) VALUES (?,?)",
		sql.AppliedSidecarName(config.Table), sql.AppliedIndexColumn, sql.AppliedSeqColumn)
}

func (d *MySQLDialect) Open(connectionString string) (*gosql.DB, error) {
	return gosql.Open("mysql", connectionString)
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
	// Schema / constraint: the proposal structurally cannot apply.
	case 1054, // Unknown column
		1062, // Duplicate entry (only reachable on the no-PK path; upsert masks it otherwise)
		1136, // Column count doesn't match value count
		1364, // Field has no default value
		1452, // FK constraint fails (matches PostgreSQL class 23; see the FK note below)
		3819, // Check constraint violated
		4025: // CHECK constraint is violated (column-level; MySQL 8.0.16+)
		return true
	}
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
