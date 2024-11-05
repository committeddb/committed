package dialects

import (
	gosql "database/sql"
	"fmt"
	"strings"

	_ "github.com/jackc/pgx/v5" // postgres driver

	"github.com/philborlin/committed/internal/cluster/syncable/sql"
)

type PostgreSQLDialect struct{}

// CreateDDL implements Dialect
func (d *PostgreSQLDialect) CreateDDL(c *sql.Config) string {
	return createDDL(c)
}

// CreateSQL implements Dialect
func (d *PostgreSQLDialect) CreateSQL(table string, sqlMappings []sql.Mapping) string {
	var sql strings.Builder

	fmt.Fprintf(&sql, "INSERT INTO %s(", table)
	for i, item := range sqlMappings {
		if i == 0 {
			fmt.Fprintf(&sql, "%s", item.Column)
		} else {
			fmt.Fprintf(&sql, ",%s", item.Column)
		}
	}
	fmt.Fprint(&sql, ") VALUES (")
	for i := range sqlMappings {
		if i == 0 {
			fmt.Fprintf(&sql, "$%d", i+1)
		} else {
			fmt.Fprintf(&sql, ",$%d", i+1)
		}
	}
	fmt.Fprint(&sql, ") ON DUPLICATE KEY UPDATE ")
	for i, item := range sqlMappings {
		if i == 0 {
			fmt.Fprintf(&sql, "%s=$%d", item.Column, i+1)
		} else {
			fmt.Fprintf(&sql, ",%s=$%d", item.Column, i+1)
		}
	}

	return sql.String()
}

func (d *PostgreSQLDialect) Open(connectionString string) (*gosql.DB, error) {
	return gosql.Open("pgx", connectionString)
}
