package dialects

import (
	gosql "database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

type MySQLDialect struct{}

// CreateDDL implements Dialect
func (d *MySQLDialect) CreateDDL(c *sql.Config) string {
	return createDDL(c)
}

// CreateDeleteSQL implements Dialect. MySQL binds the WHERE value with a ?
// placeholder.
func (d *MySQLDialect) CreateDeleteSQL(c *sql.Config) string {
	return createDeleteSQL(c, "?")
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

// IsPermanent classifies a MySQL error as permanent (non-retryable) only when
// it is unambiguously about the data or schema — the bad proposal will never
// apply no matter how many times we retry. MySQL doesn't use SQLSTATE classes
// the way PostgreSQL does, so this is an explicit allowlist of error numbers.
//
// Everything NOT listed stays transient and retries forever, by design: a
// wrongly-permanent error silently drops data past the dead letter, while a
// wrongly-transient one only wedges the worker visibly for an operator to
// skip. So infrastructure errors are deliberately absent and stay transient —
// 1205 lock wait timeout, 1213 deadlock, 1040/1203 too many connections,
// 2006/2013 server gone / lost connection, 1317 query interrupted. See the
// asymmetric-risk principle in the sync-permanent-error-classification ticket.
func (d *MySQLDialect) IsPermanent(err error) bool {
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	switch mysqlErr.Number {
	// Data: a specific row's value is bad and will never apply.
	case 1048, // Column cannot be null
		1264, // Out of range value for column
		1265, // Data truncated for column
		1292, // Truncated incorrect value (bad date/number literal)
		1366, // Incorrect value for column (charset/type)
		1406, // Data too long for column
		1690: // Numeric value out of range (e.g. BIGINT overflow)
		return true
	// Schema / constraint: the proposal structurally cannot apply.
	case 1054, // Unknown column
		1062, // Duplicate entry (only reachable on the no-PK path; upsert masks it otherwise)
		1136, // Column count doesn't match value count
		1364, // Field has no default value
		1452, // FK constraint fails (matches PostgreSQL class 23; see the FK note below)
		3819, // Check constraint violated
		4025: // CHECK constraint is violated (column-level; MySQL 8.0.16+)
		return true
	}
	// FK note: 1452 / PostgreSQL 23503 are treated permanent for parity. A FK
	// failure *could* be transient if the parent row is synced later by
	// another syncable, but committed has no cross-syncable ordering guarantee
	// to lean on, FKs on projection tables are an advanced opt-in, and both
	// dialects classify it the same way — flip both together if a deployment
	// needs FK-as-transient.
	return false
}

// BindArgs doubles the values: CreateSQL emits ? placeholders for both the
// INSERT VALUES list and the ON DUPLICATE KEY UPDATE clause, so each column
// value is bound twice.
func (d *MySQLDialect) BindArgs(values []any) []any {
	return append(values, values...)
}
