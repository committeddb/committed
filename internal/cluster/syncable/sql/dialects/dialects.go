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

// dropDDL builds `DROP TABLE IF EXISTS <table>;` — the destructive mirror of
// createDDL. DROP TABLE removes the table's indexes with it, so no separate
// index-drop is needed. IF EXISTS makes it idempotent: tearing down a table
// that is already gone (a re-run, or a node that never created it) is a no-op,
// not an error. Shared by every dialect so teardown is identical across them.
func dropDDL(config *sql.Config) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s;", config.Table)
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
