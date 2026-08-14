package dialects

import (
	"fmt"
	"strings"

	"github.com/committeddb/committed/internal/cluster/sqlident"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

// spineIndexName is the deterministic name of an enrichment on-column's
// auto-index — stable across restarts so ensure calls are idempotent.
func spineIndexName(table, onColumn string) string {
	return table + "__enrich_" + onColumn
}

// keyWhere builds `<k1> = <p1> AND <k2> = <p2> ...` over the delete key
// columns — one clause for the single-key common case, ANDed equalities for a
// composite. mkPlaceholder is the only dialect-specific bit (MySQL always
// "?", PostgreSQL "$n" positional); the bound arguments are the entity Key's
// decoded per-column values, in primaryKey column order (the encoding
// contract — see cluster.DecodeCompositeKey).
func keyWhere(cols []string, mkPlaceholder func(i int) string, q sqlident.Quoter) string {
	var b strings.Builder
	for i, c := range cols {
		if i > 0 {
			b.WriteString(" AND ")
		}
		fmt.Fprintf(&b, "%s = %s", q.Ident(c), mkPlaceholder(i))
	}
	return b.String()
}

// createDeleteSQL builds `DELETE FROM <table> WHERE <keyWhere>`. Shared by
// every dialect so the delete shape stays identical across them. The table
// and key columns are config identifiers, so q quotes them for the dialect.
func createDeleteSQL(config *sql.Config, mkPlaceholder func(i int) string, q sqlident.Quoter) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s",
		q.Table(config.Table), keyWhere(config.DeleteKeyColumns(), mkPlaceholder, q))
}

// createClearSQL builds `UPDATE <table> SET <c1>=NULL,<c2>=NULL WHERE
// <keyWhere>`. The SET columns are all literal NULLs (no placeholders); the
// WHERE binds the entity Key's decoded values like createDeleteSQL. Shared so
// the clear shape stays identical across dialects. An UPDATE, not an upsert,
// so clearing an absent row is a no-op. Table and every SET/WHERE column are
// config identifiers quoted by q.
func createClearSQL(config *sql.Config, columns []string, mkPlaceholder func(i int) string, q sqlident.Quoter) string {
	var b strings.Builder
	fmt.Fprintf(&b, "UPDATE %s SET ", q.Table(config.Table))
	for i, c := range columns {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "%s=NULL", q.Ident(c))
	}
	fmt.Fprintf(&b, " WHERE %s", keyWhere(config.DeleteKeyColumns(), mkPlaceholder, q))
	return b.String()
}

// dropDDL builds `DROP TABLE IF EXISTS <table>;` — the destructive mirror of
// createDDL. DROP TABLE removes the table's indexes with it, so no separate
// index-drop is needed. IF EXISTS makes it idempotent: tearing down a table
// that is already gone (a re-run, or a node that never created it) is a no-op,
// not an error. Shared by every dialect so teardown is identical across them.
func dropDDL(config *sql.Config, q sqlident.Quoter) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s;", q.Table(config.Table))
}

// aggregateSidecarConfig synthesizes the plain Config that createDDL /
// CreateDDL turn into the aggregate sidecar's CREATE TABLE: child_key (PK),
// parent_key, element_key — all the dialect's key type — and element, the
// dialect's JSON type. A secondary index on parent_key keeps the per-parent
// re-aggregation (the materialize / rebuild subquery) from scanning the whole
// sidecar. Reusing the DDL builder this way keeps the sidecar shape identical
// to every other table the dialect creates.
func aggregateSidecarConfig(spec sql.AggregateSpec, jsonType, keyType string) *sql.Config {
	return &sql.Config{
		Table:      spec.Sidecar,
		PrimaryKey: []string{sql.SidecarChildKey},
		Mappings: []sql.Mapping{
			{Column: sql.SidecarChildKey, SQLType: keyType},
			{Column: sql.SidecarParentKey, SQLType: keyType},
			{Column: sql.SidecarElementKey, SQLType: keyType},
			{Column: sql.SidecarElement, SQLType: jsonType},
		},
		Indexes: []sql.Index{{IndexName: spec.Sidecar + "_parent", ColumnNames: sql.SidecarParentKey}},
	}
}

// createAggregateParentLookupSQL builds `SELECT parent_key FROM <sidecar> WHERE
// child_key = <placeholder>`, the delete-path recovery of a removed child's
// parent. The placeholder is the only dialect-specific bit; the single bound
// argument is the child Key. Shared so the shape stays identical across
// dialects.
func createAggregateParentLookupSQL(spec sql.AggregateSpec, placeholder string, q sqlident.Quoter) string {
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s = %s",
		sql.SidecarParentKey, q.Table(spec.Sidecar), sql.SidecarChildKey, placeholder)
}

// createAggregateAffectedParentsSQL builds `SELECT DISTINCT parent_key FROM
// <sidecar> WHERE <extract> = <placeholder>` — the fan-out query for a dimension
// change. The text-extraction syntax differs (PostgreSQL `element->>'k'`, MySQL
// `element->>'$.k'`), so the dialect passes the built expression; the single
// bound argument is the changed dimension key.
func createAggregateAffectedParentsSQL(spec sql.AggregateSpec, extract, placeholder string, q sqlident.Quoter) string {
	return fmt.Sprintf("SELECT DISTINCT %s FROM %s WHERE %s = %s",
		sql.SidecarParentKey, q.Table(spec.Sidecar), extract, placeholder)
}

// lookupDimensionConfig synthesizes the plain Config that createDDL / CreateDDL
// turn into an enrichment dimension's CREATE TABLE: lookup_key (PK, the dialect
// key type) and lookup_fields (the dialect JSON type). Reuses the DDL builder so
// the dimension shape matches every other table the dialect creates.
func lookupDimensionConfig(spec sql.LookupSpec, jsonType, keyType string) *sql.Config {
	return &sql.Config{
		Table:      spec.Dimension,
		PrimaryKey: []string{sql.LookupKey},
		Mappings: []sql.Mapping{
			{Column: sql.LookupKey, SQLType: keyType},
			{Column: sql.LookupFields, SQLType: jsonType},
		},
	}
}

// joinIdents quotes each column and joins with commas — the composite
// PRIMARY KEY (a, b, c) / ON CONFLICT (a, b, c) column-list form.
func joinIdents(cols []string, q sqlident.Quoter) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = q.Ident(c)
	}
	return strings.Join(quoted, ",")
}

// createDDL builds the MySQL `CREATE TABLE IF NOT EXISTS` (MySQL accepts inline
// INDEX clauses; PostgreSQL builds its own in postgres.go). Every identifier —
// table, column, primary key, index name, and each index column — is a config
// value quoted by q; only SQLType is interpolated raw, so it is charset-validated
// at config time (sqlident.ValidTypeExpr in the parser) since a type expression
// cannot be quoted.
func createDDL(config *sql.Config, q sqlident.Quoter) string {
	var ddl strings.Builder
	fmt.Fprintf(&ddl, "CREATE TABLE IF NOT EXISTS %s (", q.Table(config.Table))
	for i, column := range config.Mappings {
		fmt.Fprintf(&ddl, "%s %s", q.Ident(column.Column), column.SQLType)
		if i < len(config.Mappings)-1 {
			ddl.WriteString(",")
		}
	}
	if config.Keyed() {
		fmt.Fprintf(&ddl, ",PRIMARY KEY (%s)", joinIdents(config.PrimaryKey, q))
	}
	for _, index := range config.Indexes {
		fmt.Fprintf(&ddl, ",INDEX %s (%s)", q.Ident(index.IndexName), q.Columns(index.ColumnNames))
	}
	ddl.WriteString(");")

	return ddl.String()
}
