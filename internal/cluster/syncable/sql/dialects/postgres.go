package dialects

import (
	gosql "database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib" // registers "pgx" with database/sql

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/sqlident"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

type PostgreSQLDialect struct{}

// pgIdent quotes config identifiers with PostgreSQL double-quote rules.
var pgIdent = sqlident.Postgres

// CreateDDL implements Dialect.
//
// PostgreSQL does not accept inline INDEX clauses inside CREATE TABLE, so we
// build CREATE TABLE without indexes and then append a separate
// CREATE INDEX IF NOT EXISTS for each declared index. Table, columns, primary
// key, index names and index columns are config identifiers quoted for
// PostgreSQL; only SQLType is interpolated raw (charset-validated at config time,
// since a type expression cannot be quoted).
func (d *PostgreSQLDialect) CreateDDL(c *sql.Config) string {
	var ddl strings.Builder
	fmt.Fprintf(&ddl, "CREATE TABLE IF NOT EXISTS %s (", pgIdent.Table(c.Table))
	for i, column := range c.Mappings {
		fmt.Fprintf(&ddl, "%s %s", pgIdent.Ident(column.Column), column.SQLType)
		if i < len(c.Mappings)-1 {
			ddl.WriteString(",")
		}
	}
	if c.PrimaryKey != "" {
		fmt.Fprintf(&ddl, ",PRIMARY KEY (%s)", pgIdent.Ident(c.PrimaryKey))
	}
	ddl.WriteString(");")

	for _, index := range c.Indexes {
		fmt.Fprintf(&ddl, "CREATE INDEX IF NOT EXISTS %s ON %s (%s);",
			pgIdent.Ident(index.IndexName), pgIdent.Table(c.Table), pgIdent.Columns(index.ColumnNames))
	}

	return ddl.String()
}

// DropDDL implements Dialect. DROP TABLE cascades to the table's own indexes,
// so the separate CREATE INDEX statements CreateDDL emits need no separate
// drop.
func (d *PostgreSQLDialect) DropDDL(c *sql.Config) string {
	return dropDDL(c, pgIdent)
}

// CreateDeleteSQL implements Dialect. PostgreSQL binds the WHERE value with a
// $1 positional placeholder.
func (d *PostgreSQLDialect) CreateDeleteSQL(c *sql.Config) string {
	return createDeleteSQL(c, "$1", pgIdent)
}

// CreateClearSQL implements Dialect; PostgreSQL binds the WHERE value with $1.
func (d *PostgreSQLDialect) CreateClearSQL(c *sql.Config, columns []string) string {
	return createClearSQL(c, columns, "$1", pgIdent)
}

// pgAggSubquery is the scalar subquery that re-aggregates one parent's children
// from the sidecar into a JSON array: COALESCE(jsonb_agg(element ORDER BY
// element_key), '[]') so an empty set yields [] not NULL. Ordering by
// element_key::numeric (numeric sort) or element_key (lexical) makes the array
// a pure function of the set, independent of arrival order. With enrichments
// each element is LEFT JOINed to its dimension and merged with the resolved
// fields (|| jsonb_build_object); a missing dimension row leaves nulls (LEFT
// JOIN), never drops the element. <ph> binds the parent key.
func pgAggSubquery(spec sql.AggregateSpec, ph string) string {
	if len(spec.Enrichments) == 0 {
		sort := sql.SidecarElementKey
		if spec.NumericSort {
			sort = sql.SidecarElementKey + "::numeric"
		}
		return fmt.Sprintf("(SELECT COALESCE(jsonb_agg(%s ORDER BY %s), '[]'::jsonb) FROM %s WHERE %s = %s)",
			sql.SidecarElement, sort, pgIdent.Table(spec.Sidecar), sql.SidecarParentKey, ph)
	}

	sort := "s." + sql.SidecarElementKey
	if spec.NumericSort {
		sort = "s." + sql.SidecarElementKey + "::numeric"
	}
	var joins, build strings.Builder
	for i, e := range spec.Enrichments {
		alias := fmt.Sprintf("d%d", i)
		// e.Dimension is a config-derived table (quote); e.OnField is a JSON key
		// landing inside a ->>'<key>' literal (escape the ' so it can't break out).
		// The alias and Sidecar*/Lookup* columns are fixed, so they stay raw.
		fmt.Fprintf(&joins, " LEFT JOIN %s %s ON s.%s->>'%s' = %s.%s",
			pgIdent.Table(e.Dimension), alias, sql.SidecarElement, sqlident.EscapeStringLiteral(e.OnField), alias, sql.LookupKey)
		for _, f := range e.Selects {
			// f.Output lands in an object-key literal, f.Source in a ->'<key>'
			// literal — escape both.
			fmt.Fprintf(&build, ",'%s',%s.%s->'%s'",
				sqlident.EscapeStringLiteral(f.Output), alias, sql.LookupFields, sqlident.EscapeStringLiteral(f.Source))
		}
	}
	element := fmt.Sprintf("s.%s || jsonb_build_object(%s)", sql.SidecarElement, strings.TrimPrefix(build.String(), ","))
	return fmt.Sprintf("(SELECT COALESCE(jsonb_agg(%s ORDER BY %s), '[]'::jsonb) FROM %s s%s WHERE s.%s = %s)",
		element, sort, pgIdent.Table(spec.Sidecar), joins.String(), sql.SidecarParentKey, ph)
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
	table, pk, col := pgIdent.Table(spec.Table), pgIdent.Ident(spec.PrimaryKey), pgIdent.Ident(spec.Column)
	return fmt.Sprintf(
		"INSERT INTO %s (%s,%s) VALUES ($1,%s) ON CONFLICT (%s) DO UPDATE SET %s=EXCLUDED.%s",
		table, pk, col, pgAggSubquery(spec, "$2"), pk, col, col)
}

// CreateAggregateRebuildSQL implements Dialect; $1 (the subquery filter) and $2
// (the WHERE) both bind the parent key.
func (d *PostgreSQLDialect) CreateAggregateRebuildSQL(spec sql.AggregateSpec) string {
	return fmt.Sprintf("UPDATE %s SET %s=%s WHERE %s=$2",
		pgIdent.Table(spec.Table), pgIdent.Ident(spec.Column), pgAggSubquery(spec, "$1"), pgIdent.Ident(spec.PrimaryKey))
}

// CreateAggregateParentLookupSQL implements Dialect; PostgreSQL binds the child
// key with $1.
func (d *PostgreSQLDialect) CreateAggregateParentLookupSQL(spec sql.AggregateSpec) string {
	return createAggregateParentLookupSQL(spec, "$1", pgIdent)
}

// CreateLookupDimensionDDL implements Dialect; PostgreSQL stores the fields as
// JSONB and the key as TEXT.
func (d *PostgreSQLDialect) CreateLookupDimensionDDL(spec sql.LookupSpec) string {
	return d.CreateDDL(lookupDimensionConfig(spec, "JSONB", "TEXT"))
}

// CreateAggregateAffectedParentsSQL implements Dialect; PostgreSQL extracts the
// element field with `->>'field'` and binds the changed dimension key with $1.
func (d *PostgreSQLDialect) CreateAggregateAffectedParentsSQL(spec sql.AggregateSpec, onField string) string {
	// onField is a JSON key inside a ->>'<key>' literal, so its single quotes are
	// escaped; SidecarElement is a fixed column.
	extract := fmt.Sprintf("%s->>'%s'", sql.SidecarElement, sqlident.EscapeStringLiteral(onField))
	return createAggregateAffectedParentsSQL(spec, extract, "$1", pgIdent)
}

// CreateSQL implements Dialect.
//
// PostgreSQL upserts use ON CONFLICT (<pk>) DO UPDATE SET col = EXCLUDED.col,
// not the MySQL ON DUPLICATE KEY UPDATE syntax. EXCLUDED references the row
// that was proposed for insertion, so no extra placeholders are needed for
// the update clause.
func (d *PostgreSQLDialect) CreateSQL(config *sql.Config) string {
	return pgUpsertSQL(config, false)
}

// CreateGenerationUpsertSQL implements Dialect: CreateSQL plus the
// committed-managed generation column (last column / placeholder / EXCLUDED
// assignment), used only by the keyed plain Syncable so a refresh sweep can find
// stale rows. Projections and keyless syncables keep the plain CreateSQL.
func (d *PostgreSQLDialect) CreateGenerationUpsertSQL(config *sql.Config) string {
	return pgUpsertSQL(config, true)
}

// pgUpsertSQL builds the INSERT ... ON CONFLICT upsert. withGeneration appends
// the GenerationColumn as the final column/placeholder and its EXCLUDED update
// assignment; it is only ever set for a keyed config (PrimaryKey != ""), so the
// extra assignment always lands inside the ON CONFLICT clause. With
// withGeneration=false the output is byte-identical to the pre-feature CreateSQL.
func pgUpsertSQL(config *sql.Config, withGeneration bool) string {
	var sqlb strings.Builder

	fmt.Fprintf(&sqlb, "INSERT INTO %s(", pgIdent.Table(config.Table))
	for i, item := range config.Mappings {
		if i == 0 {
			fmt.Fprintf(&sqlb, "%s", pgIdent.Ident(item.Column))
		} else {
			fmt.Fprintf(&sqlb, ",%s", pgIdent.Ident(item.Column))
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
			fmt.Fprintf(&sqlb, "$%d", i+1)
		} else {
			fmt.Fprintf(&sqlb, ",$%d", i+1)
		}
	}
	fmt.Fprint(&sqlb, ")")

	if config.PrimaryKey != "" {
		fmt.Fprintf(&sqlb, " ON CONFLICT (%s) DO UPDATE SET ", pgIdent.Ident(config.PrimaryKey))
		for i, item := range config.Mappings {
			col := pgIdent.Ident(item.Column)
			if i == 0 {
				fmt.Fprintf(&sqlb, "%s=EXCLUDED.%s", col, col)
			} else {
				fmt.Fprintf(&sqlb, ",%s=EXCLUDED.%s", col, col)
			}
		}
		if withGeneration {
			fmt.Fprintf(&sqlb, ",%s=EXCLUDED.%s", sql.GenerationColumn, sql.GenerationColumn)
		}
	}

	return sqlb.String()
}

// EnsureGenerationColumn implements Dialect. PostgreSQL supports ADD COLUMN IF
// NOT EXISTS, so a single idempotent statement covers both a freshly-created
// table (CreateDDL omits the column) and an upgraded pre-feature table. Existing
// rows baseline to generation 1.
func (d *PostgreSQLDialect) EnsureGenerationColumn(db *gosql.DB, config *sql.Config) error {
	// Table is a config identifier quoted for PostgreSQL; GenerationColumn is a
	// package constant. No user value is interpolated unquoted, so no gosec
	// suppression.
	stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s BIGINT NOT NULL DEFAULT 1",
		pgIdent.Table(config.Table), sql.GenerationColumn)
	if _, err := db.Exec(stmt); err != nil {
		return fmt.Errorf("ensure generation column [%s]: %w", stmt, err)
	}
	return nil
}

// CreateGenerationSweepSQL implements Dialect; PostgreSQL binds the epoch with $1.
func (d *PostgreSQLDialect) CreateGenerationSweepSQL(config *sql.Config) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s >= 1 AND %s < $1",
		pgIdent.Table(config.Table), sql.GenerationColumn, sql.GenerationColumn)
}

// CreateAppliedSidecarDDL implements Dialect: the dedup sidecar for a keyless
// (append) syncable — (committed_index, committed_seq) under a composite PK.
func (d *PostgreSQLDialect) CreateAppliedSidecarDDL(config *sql.Config) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (%s BIGINT NOT NULL,%s INT NOT NULL,PRIMARY KEY (%s,%s));",
		pgIdent.Table(sql.AppliedSidecarName(config.Table)),
		sql.AppliedIndexColumn, sql.AppliedSeqColumn,
		sql.AppliedIndexColumn, sql.AppliedSeqColumn)
}

// CreateAppliedMarkSQL implements Dialect: ON CONFLICT DO NOTHING, so RowsAffected
// is 1 on a first apply and 0 on a replay of the same (index, seq).
func (d *PostgreSQLDialect) CreateAppliedMarkSQL(config *sql.Config) string {
	return fmt.Sprintf(
		"INSERT INTO %s (%s,%s) VALUES ($1,$2) ON CONFLICT DO NOTHING",
		pgIdent.Table(sql.AppliedSidecarName(config.Table)), sql.AppliedIndexColumn, sql.AppliedSeqColumn)
}

func (d *PostgreSQLDialect) Open(connectionString string) (*gosql.DB, error) {
	// Connection strings are canonically postgres:// URLs everywhere; validate via
	// cluster.ParseConnString (which also keeps a malformed-URL error from echoing
	// the ${VAR}-resolved password onto GET /node/status — pgx would surface
	// url.Parse's raw *url.Error). Require a postgres/postgresql scheme so a bare
	// libpq keyword string is rejected up front rather than parsed by the driver
	// under different escaping rules. pgx opens the URL directly (no conversion).
	u, err := cluster.ParseConnString(connectionString)
	if err != nil {
		return nil, err
	}
	if s := strings.ToLower(u.Scheme); s != "postgres" && s != "postgresql" {
		return nil, errors.New("postgres connection string must be a postgres:// URL")
	}
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
	if len(code) < 2 {
		return false
	}
	// Permanent MUST mean ENTRY-SPECIFIC (see cluster.ErrPermanent): the error
	// is about THIS row's value and would NOT fail every other entry. Only two
	// SQLSTATE classes qualify:
	//   22 data exception       — out-of-range, bad format, truncation: the
	//                             row's own value.
	//   23 integrity constraint — not-null, unique, FK, check: the row violates
	//                             it (FK is treated permanent for parity — see
	//                             the note below).
	// Everything else stays TRANSIENT so the worker wedges visibly and resumes
	// once the operator fixes it, with ZERO data dead-lettered. In particular
	// class 42 (undefined_table 42P01, undefined_column 42703,
	// insufficient_privilege 42501, syntax_error 42601, …) and class 0A
	// (feature_not_supported) are SCHEMA / STATEMENT / ACCESS shaped — they
	// fail every entry identically (a dropped table, an operator ALTER, a
	// missing GRANT), exactly the mirror of the SQL-side auth carve-out the
	// webhook sink applies to 401/403.
	//
	// KNOWN LIMITATION (dialect asymmetry): Postgres raises the SAME SQLSTATE
	// 23502 not_null_violation for two different faults — (a) a mapped column
	// bound an explicit NULL for THIS row (entry-specific; permanent is right),
	// and (b) a NOT-NULL destination column absent from the config mapping, so
	// every INSERT omits it and EVERY row fails (schema-shaped). MySQL splits
	// these (1048 permanent vs 1364 transient) and we classify them oppositely,
	// but Postgres cannot — 23502 covers both, so case (b) dead-letters here
	// rather than wedging. Only reachable with an OPERATOR-managed table that has
	// a required column outside the mapping; when committed owns the table via
	// CreateDDL every NOT-NULL column is mapped, so 23502 is only the (a) case.
	// The 0.8 GTID/classification work does not fix this (it is inherent to
	// Postgres's SQLSTATE granularity); documented so it isn't mistaken for a bug.
	switch code[:2] {
	case "22", "23":
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
