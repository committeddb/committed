package dialects

import (
	"fmt"
	"strings"

	"github.com/philborlin/committed/internal/cluster/syncable/sql"
)

type SQLMockDialect struct{}

// CreateDDL implements Dialect
func (d *SQLMockDialect) CreateDDL(c *sql.Config) string {
	return createDDL(c)
}

// CreateSQL implements Dialect
func (d *SQLMockDialect) CreateSQL(table string, sqlMappings []sql.Mapping) string {
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
			fmt.Fprint(&sql, "?")
		} else {
			fmt.Fprint(&sql, ",?")
		}
	}
	fmt.Fprint(&sql, ")")

	return sql.String()
}
