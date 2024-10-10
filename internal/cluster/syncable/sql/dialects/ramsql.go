package dialects

import (
	"fmt"
	"strings"

	_ "github.com/proullon/ramsql/driver"

	"github.com/philborlin/committed/internal/cluster/syncable/sql"
)

// ramsqlDialect is the dialect for ramsql, used for testing
type RamSQLDialect struct{}

// CreateDDL implements Dialect
func (d *RamSQLDialect) CreateDDL(c *sql.Config) string {
	c.Indexes = nil

	return createDDL(c)
}

// CreateSQL implements Dialect
func (d *RamSQLDialect) CreateSQL(table string, sqlMappings []sql.Mapping) string {
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
	fmt.Fprint(&sql, ")")

	return sql.String()
}
