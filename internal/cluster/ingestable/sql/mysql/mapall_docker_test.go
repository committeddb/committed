//go:build docker || integration

package mysql_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/mysql"
)

// TestMysqlSourceColumns validates the MySQL-specific half of map-all: the
// information_schema introspection returns a table's columns in ordinal order,
// scoped to the connection's database. The expansion/override/exclude logic on
// top is dialect-agnostic and covered by the unit tests.
func TestMysqlSourceColumns(t *testing.T) {
	db := createDB(t)
	defer db.Close()
	mk := func(q string) { _, err := db.Exec(q); require.Nil(t, err) }
	mk("DROP TABLE IF EXISTS mapall_t")
	mk("CREATE TABLE mapall_t (id INT PRIMARY KEY, name VARCHAR(50), qty INT)")

	cols, err := (&mysql.MySQLDialect{}).SourceColumns(&sql.Config{
		ConnectionString: ingestURL,
		Tables:           []string{"mapall_t"},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"id", "name", "qty"}, cols["mapall_t"])
}
