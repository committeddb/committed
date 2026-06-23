package dialects

import (
	gosql "database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib" // registers "pgx" with database/sql

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

type PostgreSQLDialect struct{}

// CreateDDL implements Dialect.
//
// PostgreSQL does not accept inline INDEX clauses inside CREATE TABLE, so we
// build CREATE TABLE without indexes and then append a separate
// CREATE INDEX IF NOT EXISTS for each declared index.
func (d *PostgreSQLDialect) CreateDDL(c *sql.Config) string {
	var ddl strings.Builder
	fmt.Fprintf(&ddl, "CREATE TABLE IF NOT EXISTS %s (", c.Table)
	for i, column := range c.Mappings {
		fmt.Fprintf(&ddl, "%s %s", column.Column, column.SQLType)
		if i < len(c.Mappings)-1 {
			ddl.WriteString(",")
		}
	}
	if c.PrimaryKey != "" {
		fmt.Fprintf(&ddl, ",PRIMARY KEY (%s)", c.PrimaryKey)
	}
	ddl.WriteString(");")

	for _, index := range c.Indexes {
		fmt.Fprintf(&ddl, "CREATE INDEX IF NOT EXISTS %s ON %s (%s);",
			index.IndexName, c.Table, index.ColumnNames)
	}

	return ddl.String()
}

// DropDDL implements Dialect. DROP TABLE cascades to the table's own indexes,
// so the separate CREATE INDEX statements CreateDDL emits need no separate
// drop.
func (d *PostgreSQLDialect) DropDDL(c *sql.Config) string {
	return dropDDL(c)
}

// CreateDeleteSQL implements Dialect. PostgreSQL binds the WHERE value with a
// $1 positional placeholder.
func (d *PostgreSQLDialect) CreateDeleteSQL(c *sql.Config) string {
	return createDeleteSQL(c, "$1")
}

// CreateClearSQL implements Dialect; PostgreSQL binds the WHERE value with $1.
func (d *PostgreSQLDialect) CreateClearSQL(c *sql.Config, columns []string) string {
	return createClearSQL(c, columns, "$1")
}

// pgAggSubquery is the scalar subquery that re-aggregates one parent's children
// from the sidecar into a JSON array: COALESCE(jsonb_agg(element ORDER BY
// element_key), '[]') so an empty set yields [] not NULL. Ordering by
// element_key::numeric (numeric sort) or element_key (lexical) makes the array
// a pure function of the set, independent of arrival order. <ph> binds the
// parent key.
func pgAggSubquery(spec sql.AggregateSpec, ph string) string {
	sort := sql.SidecarElementKey
	if spec.NumericSort {
		sort = sql.SidecarElementKey + "::numeric"
	}
	return fmt.Sprintf("(SELECT COALESCE(jsonb_agg(%s ORDER BY %s), '[]'::jsonb) FROM %s WHERE %s = %s)",
		sql.SidecarElement, sort, spec.Sidecar, sql.SidecarParentKey, ph)
}

// CreateAggregateSidecarDDL implements Dialect; PostgreSQL stores the element
// as JSONB and the keys as TEXT (unbounded, still indexable).
func (d *PostgreSQLDialect) CreateAggregateSidecarDDL(spec sql.AggregateSpec) string {
	return d.CreateDDL(aggregateSidecarConfig(spec, "JSONB", "TEXT"))
}

// CreateAggregateMaterializeSQL implements Dialect; both $1 (the inserted
// parent key) and $2 (the subquery's parent_key filter) bind the same parent
// key.
func (d *PostgreSQLDialect) CreateAggregateMaterializeSQL(spec sql.AggregateSpec) string {
	return fmt.Sprintf(
		"INSERT INTO %s (%s,%s) VALUES ($1,%s) ON CONFLICT (%s) DO UPDATE SET %s=EXCLUDED.%s",
		spec.Table, spec.PrimaryKey, spec.Column, pgAggSubquery(spec, "$2"),
		spec.PrimaryKey, spec.Column, spec.Column)
}

// CreateAggregateRebuildSQL implements Dialect; $1 (the subquery filter) and $2
// (the WHERE) both bind the parent key.
func (d *PostgreSQLDialect) CreateAggregateRebuildSQL(spec sql.AggregateSpec) string {
	return fmt.Sprintf("UPDATE %s SET %s=%s WHERE %s=$2",
		spec.Table, spec.Column, pgAggSubquery(spec, "$1"), spec.PrimaryKey)
}

// CreateAggregateParentLookupSQL implements Dialect; PostgreSQL binds the child
// key with $1.
func (d *PostgreSQLDialect) CreateAggregateParentLookupSQL(spec sql.AggregateSpec) string {
	return createAggregateParentLookupSQL(spec, "$1")
}

// CreateSQL implements Dialect.
//
// PostgreSQL upserts use ON CONFLICT (<pk>) DO UPDATE SET col = EXCLUDED.col,
// not the MySQL ON DUPLICATE KEY UPDATE syntax. EXCLUDED references the row
// that was proposed for insertion, so no extra placeholders are needed for
// the update clause.
func (d *PostgreSQLDialect) CreateSQL(config *sql.Config) string {
	var sql strings.Builder

	fmt.Fprintf(&sql, "INSERT INTO %s(", config.Table)
	for i, item := range config.Mappings {
		if i == 0 {
			fmt.Fprintf(&sql, "%s", item.Column)
		} else {
			fmt.Fprintf(&sql, ",%s", item.Column)
		}
	}
	fmt.Fprint(&sql, ") VALUES (")
	for i := range config.Mappings {
		if i == 0 {
			fmt.Fprintf(&sql, "$%d", i+1)
		} else {
			fmt.Fprintf(&sql, ",$%d", i+1)
		}
	}
	fmt.Fprint(&sql, ")")

	if config.PrimaryKey != "" {
		fmt.Fprintf(&sql, " ON CONFLICT (%s) DO UPDATE SET ", config.PrimaryKey)
		for i, item := range config.Mappings {
			if i == 0 {
				fmt.Fprintf(&sql, "%s=EXCLUDED.%s", item.Column, item.Column)
			} else {
				fmt.Fprintf(&sql, ",%s=EXCLUDED.%s", item.Column, item.Column)
			}
		}
	}

	return sql.String()
}

func (d *PostgreSQLDialect) Open(connectionString string) (*gosql.DB, error) {
	return gosql.Open("pgx", connectionString)
}

// IsPermanent classifies a PostgreSQL error as permanent (non-retryable) by
// its SQLSTATE class — only when it is unambiguously about the data or schema,
// so the bad proposal will never apply no matter how many times we retry:
//
//   - 22 data exception (bad value, numeric out of range, invalid encoding, …)
//   - 23 integrity constraint violation (not-null, unique, check, foreign key)
//   - 42 syntax error or access rule violation (undefined table/column,
//     datatype mismatch, malformed SQL)
//   - 0A feature not supported
//
// Everything else stays transient and retries forever (a wedged worker is
// visible and an operator can skip it; a wrongly-permanent error silently
// drops data past the dead letter). In particular the infrastructure classes
// stay transient: 08 connection, 40 transaction rollback / serialization
// failure / deadlock, 53 insufficient resources, 57 operator intervention,
// 58 system error. See the asymmetric-risk principle in the
// sync-permanent-error-classification ticket.
//
// One carve-out inside class 42: 42501 insufficient_privilege is a missing
// GRANT (access/config), not data or schema — fixable without touching the
// proposal and failing every row identically — so it stays transient (wedge
// visibly) rather than dead-lettering the whole stream.
func (d *PostgreSQLDialect) IsPermanent(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	code := pgErr.Code
	if code == "42501" { // insufficient_privilege — access, not data/schema
		return false
	}
	if len(code) < 2 {
		return false
	}
	switch code[:2] {
	case "22", "23", "42", "0A":
		return true
	}
	return false
}

// BindArgs binds the values once: CreateSQL's ON CONFLICT ... DO UPDATE SET
// col = EXCLUDED.col references the proposed row, so no extra placeholders
// (and no value doubling) are needed beyond the INSERT VALUES list.
func (d *PostgreSQLDialect) BindArgs(values []any) []any {
	return values
}
