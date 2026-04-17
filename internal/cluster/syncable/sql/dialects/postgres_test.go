package dialects_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster/syncable/sql"
	"github.com/philborlin/committed/internal/cluster/syncable/sql/dialects"
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
	// PostgreSQL emits a separate CREATE INDEX statement, not an inline INDEX clause.
	require.NotContains(t, ddl, "INDEX idx_name (name)")
	require.Contains(t, ddl, "CREATE INDEX IF NOT EXISTS idx_name ON mytable (name);")
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
	cfg := &sql.Config{
		Table: "mytable",
		Mappings: []sql.Mapping{
			{Column: "id"},
			{Column: "name"},
		},
		PrimaryKey: "id",
	}
	result := d.CreateSQL(cfg)

	require.Contains(t, result, "INSERT INTO mytable(id,name)")
	require.Contains(t, result, "VALUES ($1,$2)")

	// PostgreSQL upsert uses ON CONFLICT ... DO UPDATE SET col = EXCLUDED.col.
	require.Contains(t, result, "ON CONFLICT (id) DO UPDATE SET")
	require.Contains(t, result, "id=EXCLUDED.id")
	require.Contains(t, result, "name=EXCLUDED.name")
	require.NotContains(t, result, "ON DUPLICATE KEY UPDATE")

	// Verify it does NOT use MySQL ? placeholders
	require.NotContains(t, result, "?")
}

func TestPostgreSQLDialect_CreateSQL_SingleColumn(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	cfg := &sql.Config{
		Table:      "t",
		Mappings:   []sql.Mapping{{Column: "pk"}},
		PrimaryKey: "pk",
	}
	result := d.CreateSQL(cfg)

	require.Contains(t, result, "INSERT INTO t(pk)")
	require.Contains(t, result, "VALUES ($1)")
	require.Contains(t, result, "ON CONFLICT (pk) DO UPDATE SET pk=EXCLUDED.pk")
}

func TestDialect_PlaceholderDifference(t *testing.T) {
	pg := &dialects.PostgreSQLDialect{}
	my := &dialects.MySQLDialect{}
	cfg := &sql.Config{
		Table: "t",
		Mappings: []sql.Mapping{
			{Column: "a"},
			{Column: "b"},
		},
		PrimaryKey: "a",
	}

	pgSQL := pg.CreateSQL(cfg)
	mySQL := my.CreateSQL(cfg)

	// PostgreSQL uses positional $N placeholders
	require.Contains(t, pgSQL, "$1")
	require.Contains(t, pgSQL, "$2")
	require.NotContains(t, pgSQL, "?")

	// MySQL uses ? placeholders
	require.Contains(t, mySQL, "?")
	require.NotContains(t, mySQL, "$")
}
