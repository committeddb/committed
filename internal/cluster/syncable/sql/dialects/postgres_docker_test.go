//go:build docker || integration

package dialects_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/philborlin/committed/internal/cluster/syncable/sql"
	"github.com/philborlin/committed/internal/cluster/syncable/sql/dialects"

	"github.com/stretchr/testify/require"
)

// pgConnString is the shared connection string for the Postgres container,
// initialised once by TestMain.
var pgConnString string

// TestMain starts a single Postgres container shared by every test in this
// file, so the ~5s container startup cost is paid once per `go test`
// invocation rather than once per test.
//
// These tests are activated by either the `docker` or `integration` build
// tag — `docker` selects the focused subset of tests that need a Docker
// daemon, while `integration` selects the broader category of all
// integration tests (which includes these).
//
// WithReuseByName additionally enables cross-invocation reuse when ryuk (the
// testcontainers reaper) is disabled. To take advantage of this for fast local
// iteration:
//
//	export TESTCONTAINERS_RYUK_DISABLED=true
//	go test -tags docker ./internal/cluster/syncable/sql/dialects/
//
// After the first run a long-lived container named
// "committed-postgres-dialect-tests" persists; subsequent runs find and attach
// to it in milliseconds. With ryuk enabled (the default) the container is
// reaped at the end of the test process, which is the safer behaviour for CI.
//
// Tests use uniqueTable(t) to derive a fresh table name, so reused containers
// never see stale state from previous runs.
func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("committed_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		postgres.BasicWaitStrategies(),
		testcontainers.WithReuseByName("committed-postgres-dialect-tests"),
	)
	if err != nil {
		log.Fatalf("failed to start postgres container: %v", err)
	}

	pgConnString, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Fatalf("failed to get connection string: %v", err)
	}

	code := m.Run()

	// Don't terminate the container on exit so the next `go test` run can
	// reuse it via WithReuseByName.
	os.Exit(code)
}

// uniqueTable returns a Postgres-safe, unique table name for the current test
// so parallel tests and reused containers never collide.
func uniqueTable(t *testing.T) string {
	t.Helper()
	base := strings.ToLower(t.Name())
	base = strings.ReplaceAll(base, "/", "_")
	return base + "_" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

// dropTable safely drops a table during test cleanup. It uses a fresh
// connection so the drop runs even if the test left a connection in a
// failed state.
func dropTable(t *testing.T, table string) {
	t.Helper()
	d := &dialects.PostgreSQLDialect{}
	db, err := d.Open(pgConnString)
	if err != nil {
		t.Logf("dropTable: open: %v", err)
		return
	}
	defer db.Close()
	if _, err := db.Exec("DROP TABLE IF EXISTS " + table); err != nil {
		t.Logf("dropTable: drop %s: %v", table, err)
	}
}

// TestPostgreSQLIntegration_OpenAndPing verifies the Open method establishes
// a working connection to a real Postgres instance.
func TestPostgreSQLIntegration_OpenAndPing(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := d.Open(pgConnString)
	require.Nil(t, err)
	defer db.Close()

	require.Nil(t, db.Ping())
}

// TestPostgreSQLIntegration_CreateDDL_NoIndexes verifies that the DDL produced
// by CreateDDL (without any indexes) is valid PostgreSQL syntax and actually
// creates a table that information_schema can see.
func TestPostgreSQLIntegration_CreateDDL_NoIndexes(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := d.Open(pgConnString)
	require.Nil(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTable(t, table)

	cfg := &sql.Config{
		Table: table,
		Mappings: []sql.Mapping{
			{Column: "id", SQLType: "VARCHAR(128)"},
			{Column: "name", SQLType: "TEXT"},
		},
		PrimaryKey: "id",
	}

	ddl := d.CreateDDL(cfg)
	_, err = db.Exec(ddl)
	require.Nil(t, err, "CreateDDL output should be valid PostgreSQL")

	var exists bool
	err = db.QueryRow(
		"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
		table,
	).Scan(&exists)
	require.Nil(t, err)
	require.True(t, exists)
}

// TestPostgreSQLIntegration_CreateDDL_WithIndex executes a DDL containing an
// inline INDEX clause against Postgres.
//
// Expected to FAIL: the shared createDDL helper in dialects.go produces
// `INDEX <name> (<cols>)` inside `CREATE TABLE`, which is MySQL-specific
// syntax. PostgreSQL requires a separate `CREATE INDEX` statement, so this
// test will fail until the PostgreSQL dialect overrides DDL generation to
// emit a separate CREATE INDEX (or to omit it from CREATE TABLE).
func TestPostgreSQLIntegration_CreateDDL_WithIndex(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := d.Open(pgConnString)
	require.Nil(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTable(t, table)

	cfg := &sql.Config{
		Table: table,
		Mappings: []sql.Mapping{
			{Column: "id", SQLType: "VARCHAR(128)"},
			{Column: "name", SQLType: "TEXT"},
		},
		PrimaryKey: "id",
		Indexes: []sql.Index{
			{IndexName: "idx_name", ColumnNames: "name"},
		},
	}

	ddl := d.CreateDDL(cfg)
	_, err = db.Exec(ddl)
	require.Nil(t, err,
		"BUG: PostgreSQL does not support inline INDEX in CREATE TABLE; "+
			"createDDL generates MySQL-only syntax. Fix: emit a separate CREATE INDEX.")
}

// TestPostgreSQLIntegration_CreateSQL_Insert executes the INSERT statement
// produced by CreateSQL against a real table.
//
// Expected to FAIL: postgres.go:CreateSQL emits `ON DUPLICATE KEY UPDATE`,
// which is MySQL syntax. PostgreSQL requires `ON CONFLICT (<pk>) DO UPDATE
// SET ...`. The Postgres parser will reject the statement at the DUPLICATE
// keyword.
func TestPostgreSQLIntegration_CreateSQL_Insert(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := d.Open(pgConnString)
	require.Nil(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTable(t, table)

	// Create the table directly so we isolate the bug to CreateSQL only.
	_, err = db.Exec(fmt.Sprintf(
		"CREATE TABLE %s (id VARCHAR(128) PRIMARY KEY, name TEXT)", table))
	require.Nil(t, err)

	cfg := &sql.Config{
		Table: table,
		Mappings: []sql.Mapping{
			{Column: "id"},
			{Column: "name"},
		},
		PrimaryKey: "id",
	}
	insertSQL := d.CreateSQL(cfg)

	_, err = db.Exec(insertSQL, "key1", "value1")
	require.Nil(t, err,
		"BUG: PostgreSQL CreateSQL produces MySQL ON DUPLICATE KEY UPDATE syntax. "+
			"Fix: emit ON CONFLICT (<pk>) DO UPDATE SET <col> = EXCLUDED.<col>, ...")
}

// TestPostgreSQLIntegration_CreateSQL_Upsert verifies end-to-end upsert
// behaviour: inserting twice with the same primary key should leave exactly
// one row containing the second value. This is the contract that the SQL
// syncable depends on.
//
// Expected to FAIL until the ON CONFLICT bug above is fixed.
func TestPostgreSQLIntegration_CreateSQL_Upsert(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := d.Open(pgConnString)
	require.Nil(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTable(t, table)

	_, err = db.Exec(fmt.Sprintf(
		"CREATE TABLE %s (id VARCHAR(128) PRIMARY KEY, name TEXT)", table))
	require.Nil(t, err)

	cfg := &sql.Config{
		Table: table,
		Mappings: []sql.Mapping{
			{Column: "id"},
			{Column: "name"},
		},
		PrimaryKey: "id",
	}
	insertSQL := d.CreateSQL(cfg)

	_, err = db.Exec(insertSQL, "key1", "first")
	require.Nil(t, err, "first insert should succeed")

	_, err = db.Exec(insertSQL, "key1", "second")
	require.Nil(t, err, "second insert with same PK should upsert, not error")

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count)
	require.Nil(t, err)
	require.Equal(t, 1, count, "upsert should leave exactly one row")

	var name string
	err = db.QueryRow("SELECT name FROM "+table+" WHERE id = $1", "key1").Scan(&name)
	require.Nil(t, err)
	require.Equal(t, "second", name, "upsert should leave the most recent value")
}

// TestPostgreSQLIntegration_FullSyncableFlow exercises the dialect end-to-end
// in the same shape the SQL Syncable uses it: CreateDDL → Exec, then
// CreateSQL → Prepare → Exec for each row.
//
// Expected to FAIL until the upstream bugs are fixed (uses both DDL and SQL
// generation).
func TestPostgreSQLIntegration_FullSyncableFlow(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := d.Open(pgConnString)
	require.Nil(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTable(t, table)

	cfg := &sql.Config{
		Table: table,
		Mappings: []sql.Mapping{
			{Column: "pk", SQLType: "VARCHAR(128)"},
			{Column: "value", SQLType: "TEXT"},
		},
		PrimaryKey: "pk",
	}

	ddl := d.CreateDDL(cfg)
	_, err = db.Exec(ddl)
	require.Nil(t, err, "DDL must be valid PostgreSQL")

	insertSQL := d.CreateSQL(cfg)
	stmt, err := db.Prepare(insertSQL)
	require.Nil(t, err, "INSERT must be valid PostgreSQL")
	defer stmt.Close()

	rows := []struct{ pk, value string }{
		{"a", "1"},
		{"b", "2"},
		{"a", "3"}, // upsert
	}
	for _, r := range rows {
		_, err := stmt.Exec(r.pk, r.value)
		require.Nil(t, err)
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count)
	require.Nil(t, err)
	require.Equal(t, 2, count, "two distinct keys after one upsert")

	var aValue string
	err = db.QueryRow("SELECT value FROM "+table+" WHERE pk = $1", "a").Scan(&aValue)
	require.Nil(t, err)
	require.Equal(t, "3", aValue)
}
