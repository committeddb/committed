package dialects

import (
	"fmt"
	"strings"

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

// createDeleteSQL builds `DELETE FROM <table> WHERE <keyCol> = <placeholder>`.
// The placeholder is the only dialect-specific bit (? for MySQL, $1 for
// PostgreSQL); the single bound argument is the entity Key. Shared by every
// dialect so the delete shape stays identical across them.
func createDeleteSQL(config *sql.Config, placeholder string) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s = %s",
		config.Table, config.DeleteKeyColumn(), placeholder)
}

// createClearSQL builds `UPDATE <table> SET <c1>=NULL,<c2>=NULL WHERE <keyCol>
// = <placeholder>`. Like createDeleteSQL the placeholder is the only
// dialect-specific bit and the single bound argument is the entity Key; the SET
// columns are all literal NULLs (no placeholders). Shared so the clear shape
// stays identical across dialects. An UPDATE, not an upsert, so clearing an
// absent row is a no-op.
func createClearSQL(config *sql.Config, columns []string, placeholder string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "UPDATE %s SET ", config.Table)
	for i, c := range columns {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "%s=NULL", c)
	}
	fmt.Fprintf(&b, " WHERE %s = %s", config.DeleteKeyColumn(), placeholder)
	return b.String()
}

// dropDDL builds `DROP TABLE IF EXISTS <table>;` — the destructive mirror of
// createDDL. DROP TABLE removes the table's indexes with it, so no separate
// index-drop is needed. IF EXISTS makes it idempotent: tearing down a table
// that is already gone (a re-run, or a node that never created it) is a no-op,
// not an error. Shared by every dialect so teardown is identical across them.
func dropDDL(config *sql.Config) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s;", config.Table)
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
		PrimaryKey: sql.SidecarChildKey,
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
func createAggregateParentLookupSQL(spec sql.AggregateSpec, placeholder string) string {
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s = %s",
		sql.SidecarParentKey, spec.Sidecar, sql.SidecarChildKey, placeholder)
}

// createAggregateAffectedParentsSQL builds `SELECT DISTINCT parent_key FROM
// <sidecar> WHERE <extract> = <placeholder>` — the fan-out query for a dimension
// change. The text-extraction syntax differs (PostgreSQL `element->>'k'`, MySQL
// `element->>'$.k'`), so the dialect passes the built expression; the single
// bound argument is the changed dimension key.
func createAggregateAffectedParentsSQL(spec sql.AggregateSpec, extract, placeholder string) string {
	return fmt.Sprintf("SELECT DISTINCT %s FROM %s WHERE %s = %s",
		sql.SidecarParentKey, spec.Sidecar, extract, placeholder)
}

// lookupDimensionConfig synthesizes the plain Config that createDDL / CreateDDL
// turn into an enrichment dimension's CREATE TABLE: lookup_key (PK, the dialect
// key type) and lookup_fields (the dialect JSON type). Reuses the DDL builder so
// the dimension shape matches every other table the dialect creates.
func lookupDimensionConfig(spec sql.LookupSpec, jsonType, keyType string) *sql.Config {
	return &sql.Config{
		Table:      spec.Dimension,
		PrimaryKey: sql.LookupKey,
		Mappings: []sql.Mapping{
			{Column: sql.LookupKey, SQLType: keyType},
			{Column: sql.LookupFields, SQLType: jsonType},
		},
	}
}

func createDDL(config *sql.Config) string {
	var ddl strings.Builder
	fmt.Fprintf(&ddl, "CREATE TABLE IF NOT EXISTS %s (", config.Table)
	for i, column := range config.Mappings {
		fmt.Fprintf(&ddl, "%s %s", column.Column, column.SQLType)
		if i < len(config.Mappings)-1 {
			ddl.WriteString(",")
		}
	}
	if config.PrimaryKey != "" {
		fmt.Fprintf(&ddl, ",PRIMARY KEY (%s)", config.PrimaryKey)
	}
	for _, index := range config.Indexes {
		fmt.Fprintf(&ddl, ",INDEX %s (%s)", index.IndexName, index.ColumnNames)
	}
	ddl.WriteString(");")

	return ddl.String()
}
