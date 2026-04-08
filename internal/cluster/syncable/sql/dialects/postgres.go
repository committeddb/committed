package dialects

import (
	gosql "database/sql"
	"fmt"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib" // registers "pgx" with database/sql

	"github.com/philborlin/committed/internal/cluster/syncable/sql"
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
