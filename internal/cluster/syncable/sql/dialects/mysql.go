package dialects

import (
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql" // mysql driver

	"github.com/philborlin/committed/internal/cluster/syncable/sql"
)

type MySQLDialect struct{}

// CreateDDL implements Dialect
func (d *MySQLDialect) CreateDDL(c *sql.SQLConfig) string {
	return createDDL(c)
}

// CreateSQL implements Dialect
func (d *MySQLDialect) CreateSQL(table string, sqlMappings []sql.SQLMapping) string {
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
