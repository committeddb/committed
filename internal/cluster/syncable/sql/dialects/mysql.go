package dialects

import (
	gosql "database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/philborlin/committed/internal/cluster/syncable/sql"
)

type MySQLDialect struct{}

// CreateDDL implements Dialect
func (d *MySQLDialect) CreateDDL(c *sql.Config) string {
	return createDDL(c)
}

// CreateSQL implements Dialect
func (d *MySQLDialect) CreateSQL(config *sql.Config) string {
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
			fmt.Fprint(&sql, "?")
		} else {
			fmt.Fprint(&sql, ",?")
		}
	}
	fmt.Fprint(&sql, ") ON DUPLICATE KEY UPDATE ")
	for i, item := range config.Mappings {
		if i == 0 {
			fmt.Fprintf(&sql, "%s=?", item.Column)
		} else {
			fmt.Fprintf(&sql, ",%s=?", item.Column)
		}
	}

	return sql.String()
}

func (d *MySQLDialect) Open(connectionString string) (*gosql.DB, error) {
	return gosql.Open("mysql", connectionString)
}

// IsPermanent classifies MySQL errors as permanent (non-retryable) when
// they indicate constraint violations or data-type mismatches.
func (d *MySQLDialect) IsPermanent(err error) bool {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case 1048, // Column cannot be null
			1054, // Unknown column
			1062, // Duplicate entry (when not using upsert)
			1136, // Column count doesn't match
			1264, // Out of range value
			1265, // Data truncated
			1366: // Incorrect value for column
			return true
		}
	}
	return false
}
