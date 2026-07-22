package dialects_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
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

	// Config identifiers are quoted for PostgreSQL; SQLType is interpolated raw.
	require.Contains(t, ddl, `CREATE TABLE IF NOT EXISTS "mytable"`)
	require.Contains(t, ddl, `"id" VARCHAR(128)`)
	require.Contains(t, ddl, `"name" TEXT`)
	require.Contains(t, ddl, `PRIMARY KEY ("id")`)
	// PostgreSQL emits a separate CREATE INDEX statement, not an inline INDEX clause.
	require.NotContains(t, ddl, `INDEX "idx_name" ("name")`)
	require.Contains(t, ddl, `CREATE INDEX IF NOT EXISTS "idx_name" ON "mytable" ("name");`)
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

	require.Contains(t, ddl, `CREATE TABLE IF NOT EXISTS "simple"`)
	require.Contains(t, ddl, `PRIMARY KEY ("col1")`)
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

	require.Contains(t, result, `INSERT INTO "mytable"("id","name")`)
	require.Contains(t, result, "VALUES ($1,$2)")

	// PostgreSQL upsert uses ON CONFLICT ... DO UPDATE SET col = EXCLUDED.col.
	require.Contains(t, result, `ON CONFLICT ("id") DO UPDATE SET`)
	require.Contains(t, result, `"id"=EXCLUDED."id"`)
	require.Contains(t, result, `"name"=EXCLUDED."name"`)
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

	require.Contains(t, result, `INSERT INTO "t"("pk")`)
	require.Contains(t, result, "VALUES ($1)")
	require.Contains(t, result, `ON CONFLICT ("pk") DO UPDATE SET "pk"=EXCLUDED."pk"`)
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

// TestDialect_BindArgs_MatchesPlaceholders is the invariant that keeps the
// Syncable's dialect-agnostic Sync correct: the number of args a dialect binds
// per row MUST equal the number of placeholders its CreateSQL emits. Postgres
// (EXCLUDED) binds N; MySQL (ON DUPLICATE KEY UPDATE) binds 2N. Sync passes
// exactly dialect.BindArgs(values) to ExecContext, so a mismatch here is a
// runtime "expected N args, got M" against a real database — which is the bug
// the postgres syncable shipped with before BindArgs existed.
func TestDialect_BindArgs_MatchesPlaceholders(t *testing.T) {
	cfg := &sql.Config{
		Table:      "t",
		Mappings:   []sql.Mapping{{Column: "a"}, {Column: "b"}, {Column: "c"}},
		PrimaryKey: "a",
	}
	values := []any{"1", "2", "3"}

	t.Run("postgres", func(t *testing.T) {
		d := &dialects.PostgreSQLDialect{}
		placeholders := strings.Count(d.CreateSQL(cfg), "$")
		require.Len(t, d.BindArgs(values), placeholders,
			"postgres BindArgs count must equal its $N placeholder count")
	})

	t.Run("mysql", func(t *testing.T) {
		d := &dialects.MySQLDialect{}
		placeholders := strings.Count(d.CreateSQL(cfg), "?")
		require.Len(t, d.BindArgs(values), placeholders,
			"mysql BindArgs count must equal its ? placeholder count")
	})
}

// TestPostgreSQLDialect_IsPermanent pins the asymmetric-risk classification:
// only data/schema SQLSTATE classes (22, 23, 42, 0A) are permanent, and the
// infrastructure classes (08, 40, 53, 57, 58) plus the 42501 access carve-out
// stay transient. The negative cases are the load-bearing half — a
// wrongly-permanent error silently drops a proposal past the dead letter, so a
// regression that promotes a connection/deadlock/lock error to permanent must
// fail here.
func TestPostgreSQLDialect_IsPermanent(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}

	tests := []struct {
		name      string
		code      string
		permanent bool
	}{
		// Data (class 22): bad value, will never apply.
		{"numeric_value_out_of_range", "22003", true},
		{"invalid_datetime_format", "22007", true},
		{"invalid_text_representation", "22P02", true},
		// Integrity constraint (class 23): not-null, unique, check, FK.
		{"not_null_violation", "23502", true},
		{"foreign_key_violation", "23503", true},
		{"unique_violation", "23505", true},
		{"check_violation", "23514", true},
		// Schema / statement / access (class 42) is NOT entry-specific — it
		// fails EVERY row identically, so it is TRANSIENT (wedge, fix, resume;
		// zero dead-lettered). Same for feature_not_supported (class 0A).
		{"undefined_column_is_transient", "42703", false},
		{"undefined_table_is_transient", "42P01", false},
		{"datatype_mismatch_is_transient", "42804", false},
		{"syntax_error_is_transient", "42601", false},
		{"insufficient_privilege_is_transient", "42501", false},
		{"feature_not_supported_is_transient", "0A000", false},

		// Infrastructure classes stay transient — retry forever, wedge visibly.
		{"connection_exception", "08000", false},
		{"connection_failure", "08006", false},
		{"serialization_failure", "40001", false},
		{"deadlock_detected", "40P01", false},
		{"insufficient_resources", "53000", false},
		{"disk_full", "53100", false},
		{"too_many_connections", "53300", false},
		{"admin_shutdown", "57P01", false},
		{"query_canceled", "57014", false},
		{"io_error", "58030", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &pgconn.PgError{Code: tt.code}
			require.Equal(t, tt.permanent, d.IsPermanent(err),
				"SQLSTATE %s", tt.code)
		})
	}

	// A non-pg error (e.g. a transport/driver error with no SQLSTATE) is never
	// permanent — there is no data/schema signal to act on, so it retries.
	t.Run("non_pg_error_is_transient", func(t *testing.T) {
		require.False(t, d.IsPermanent(errors.New("dial tcp: connection refused")))
	})

	// errors.As must reach through a wrapped PgError.
	t.Run("wrapped_pg_error", func(t *testing.T) {
		wrapped := fmt.Errorf("exec failed: %w", &pgconn.PgError{Code: "23505"})
		require.True(t, d.IsPermanent(wrapped))
	})
}
