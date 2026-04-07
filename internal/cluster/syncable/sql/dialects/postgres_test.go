package dialects_test

import (
	"strings"
	"testing"

	"github.com/philborlin/committed/internal/cluster/syncable/sql"
	"github.com/philborlin/committed/internal/cluster/syncable/sql/dialects"
	"github.com/stretchr/testify/require"
)

func testConfig() *sql.Config {
	return &sql.Config{
		Table: "mytable",
		Mappings: []sql.Mapping{
			{Column: "id", SQLType: "VARCHAR(128)"},
			{Column: "name", SQLType: "TEXT"},
		},
		Indexes: []sql.Index{
			{IndexName: "idx_name", ColumnNames: "name"},
		},
		PrimaryKey: "id",
	}
}

func TestPostgreSQLDialect_CreateDDL(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	ddl := d.CreateDDL(testConfig())

	require.Contains(t, ddl, "CREATE TABLE IF NOT EXISTS mytable")
	require.Contains(t, ddl, "id VARCHAR(128)")
	require.Contains(t, ddl, "name TEXT")
	require.Contains(t, ddl, "PRIMARY KEY (id)")
	require.Contains(t, ddl, "INDEX idx_name (name)")
	require.True(t, strings.HasSuffix(ddl, ");"))
}

func TestPostgreSQLDialect_CreateDDL_NoIndexes(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	cfg := &sql.Config{
		Table: "simple",
		Mappings: []sql.Mapping{
			{Column: "col1", SQLType: "INT"},
		},
		PrimaryKey: "col1",
	}
	ddl := d.CreateDDL(cfg)

	require.Contains(t, ddl, "CREATE TABLE IF NOT EXISTS simple")
	require.Contains(t, ddl, "PRIMARY KEY (col1)")
	require.NotContains(t, ddl, "INDEX")
}

func TestPostgreSQLDialect_CreateSQL(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	mappings := []sql.Mapping{
		{Column: "id"},
		{Column: "name"},
	}
	result := d.CreateSQL("mytable", mappings)

	require.Contains(t, result, "INSERT INTO mytable(id,name)")
	require.Contains(t, result, "VALUES ($1,$2)")

	// BUG: PostgreSQL should use ON CONFLICT ... DO UPDATE SET, not ON DUPLICATE KEY UPDATE.
	// This is MySQL syntax. Documenting the current (incorrect) behavior.
	require.Contains(t, result, "ON DUPLICATE KEY UPDATE")
	require.Contains(t, result, "id=$1")
	require.Contains(t, result, "name=$2")

	// Verify it does NOT use MySQL ? placeholders
	require.NotContains(t, result, "?")
}

func TestPostgreSQLDialect_CreateSQL_SingleColumn(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	mappings := []sql.Mapping{{Column: "pk"}}
	result := d.CreateSQL("t", mappings)

	require.Contains(t, result, "INSERT INTO t(pk)")
	require.Contains(t, result, "VALUES ($1)")
	require.Contains(t, result, "pk=$1")
}

func TestDialect_CreateDDL_Identical(t *testing.T) {
	pg := &dialects.PostgreSQLDialect{}
	my := &dialects.MySQLDialect{}
	cfg := testConfig()

	// Both dialects use the shared createDDL function
	require.Equal(t, my.CreateDDL(cfg), pg.CreateDDL(cfg))
}

func TestDialect_PlaceholderDifference(t *testing.T) {
	pg := &dialects.PostgreSQLDialect{}
	my := &dialects.MySQLDialect{}
	mappings := []sql.Mapping{
		{Column: "a"},
		{Column: "b"},
	}

	pgSQL := pg.CreateSQL("t", mappings)
	mySQL := my.CreateSQL("t", mappings)

	// PostgreSQL uses positional $N placeholders
	require.Contains(t, pgSQL, "$1")
	require.Contains(t, pgSQL, "$2")
	require.NotContains(t, pgSQL, "?")

	// MySQL uses ? placeholders
	require.Contains(t, mySQL, "?")
	require.NotContains(t, mySQL, "$")
}
