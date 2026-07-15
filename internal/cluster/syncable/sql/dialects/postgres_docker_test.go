//go:build docker || integration

package dialects_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"

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

// TestPostgreSQLIntegration_NumericPrecisionBinding confirms the real-DB half of
// the projection/syncable number-precision fix: coerceForColumn hands the driver
// a Go string for an exact-numeric (NUMERIC/DECIMAL) column and an int64 for an
// integer column, and pgx must store both losslessly. A float64 round trip (the
// former behavior) would corrupt a high-precision decimal and any integer above
// 2^53 — this proves binding the exact source digits as a string round-trips.
func TestPostgreSQLIntegration_NumericPrecisionBinding(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := d.Open(pgConnString)
	require.Nil(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTable(t, table)

	_, err = db.Exec(fmt.Sprintf(
		"CREATE TABLE %s (id BIGINT PRIMARY KEY, balance NUMERIC(30,2))", table))
	require.Nil(t, err)

	// Bind exactly what coerceForColumn now produces: an int64 for the BIGINT key,
	// the exact source digits as a string for the NUMERIC column.
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (id, balance) VALUES ($1, $2)", table),
		int64(9007199254740993), "7922816251426433.75")
	require.Nil(t, err, "pgx must accept a string bound to a NUMERIC column")

	var id int64
	var balance string
	err = db.QueryRow(fmt.Sprintf("SELECT id, balance FROM %s", table)).Scan(&id, &balance)
	require.Nil(t, err)
	require.Equal(t, int64(9007199254740993), id, "the 2^53+1 id survives as an exact int64")
	require.Equal(t, "7922816251426433.75", balance, "the high-precision decimal survives digit-for-digit")
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

// TestPostgreSQLIntegration_KeylessAppendIdempotentOnReplay is the no-primaryKey
// replay regression: an append/history syncable (no key to upsert on) must NOT
// duplicate rows when the same Actuals are re-synced — a crash mid-batch, a
// leader-change re-sync, or a corrupt-checkpoint restart. committed dedups on a
// sidecar keyed by (raft index, entity ordinal), and the user's own table stays
// pristine: the dedup columns live only in the sidecar.
func TestPostgreSQLIntegration_KeylessAppendIdempotentOnReplay(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := sql.NewDB(d, pgConnString)
	require.Nil(t, err)
	defer db.Close()

	table := "kless_append_replay" // short: sidecar name must fit the 63-char limit
	dropTable(t, table)
	dropTable(t, sql.AppliedSidecarName(table))
	defer dropTable(t, table)
	defer dropTable(t, sql.AppliedSidecarName(table))

	cfg := &sql.Config{
		Topic: eventType.ID,
		Table: table,
		Mappings: []sql.Mapping{
			{JsonPath: "$.k", Column: "k", SQLType: "TEXT"},
			{JsonPath: "$.v", Column: "v", SQLType: "TEXT"},
		},
		// no PrimaryKey — an append/history table
	}
	syncable := sql.New(db, cfg)
	require.Nil(t, syncable.Init())
	defer syncable.Close()

	up := func(k, v string) *cluster.Entity {
		return cluster.NewUpsertEntity(eventType, []byte(k), []byte(fmt.Sprintf(`{"k":%q,"v":%q}`, k, v)))
	}
	// 5 rows: index 1 and 3 share key "a" (an append table keeps BOTH), and index
	// 4 carries two entities in one Actual (seq 0 and 1).
	actuals := []*cluster.Actual{
		{Index: 1, Entities: []*cluster.Entity{up("a", "1")}},
		{Index: 2, Entities: []*cluster.Entity{up("b", "2")}},
		{Index: 3, Entities: []*cluster.Entity{up("a", "3")}},
		{Index: 4, Entities: []*cluster.Entity{up("c", "4a"), up("c", "4b")}},
	}
	apply := func() {
		for _, a := range actuals {
			_, serr := syncable.Sync(context.Background(), a)
			require.NoError(t, serr)
		}
	}

	apply() // first pass
	var count int
	require.Nil(t, db.DB.QueryRow("SELECT COUNT(*) FROM "+table).Scan(&count))
	require.Equal(t, 5, count, "append keeps one row per entity, including the repeated key 'a'")

	apply() // replay — every (index, seq) is already marked
	require.Nil(t, db.DB.QueryRow("SELECT COUNT(*) FROM "+table).Scan(&count))
	require.Equal(t, 5, count, "replay is a no-op: the dedup sidecar suppresses re-appends")

	var sidecarCount int
	require.Nil(t, db.DB.QueryRow("SELECT COUNT(*) FROM "+sql.AppliedSidecarName(table)).Scan(&sidecarCount))
	require.Equal(t, 5, sidecarCount, "one sidecar mark per applied row")

	// The user's table stays pristine — no committed_index/committed_seq leaked in.
	nameRows, err := db.DB.Query(
		"SELECT column_name FROM information_schema.columns WHERE table_name = $1 ORDER BY column_name", table)
	require.NoError(t, err)
	defer nameRows.Close()
	var names []string
	for nameRows.Next() {
		var n string
		require.NoError(t, nameRows.Scan(&n))
		names = append(names, n)
	}
	require.NoError(t, nameRows.Err())
	require.Equal(t, []string{"k", "v"}, names, "dedup metadata lives in the sidecar, not the user's table")
}

// --- Whole-payload ("$") mappings ---

var eventType = &cluster.Type{ID: "controlplane-event"}

// wholePayloadConfig is the event-log read-model shape the "$" mapping
// exists for: a scalar envelope column for indexing plus the entire payload
// document in one column of payloadType.
func wholePayloadConfig(table, payloadType string) *sql.Config {
	return &sql.Config{
		Topic: eventType.ID,
		Table: table,
		Mappings: []sql.Mapping{
			{JsonPath: "$.event_id", Column: "event_id", SQLType: "VARCHAR(64)"},
			{JsonPath: "$", Column: "payload", SQLType: payloadType},
		},
		PrimaryKey: "event_id",
	}
}

// decodeJSONSemantic parses with UseNumber so numbers compare by their exact
// digits rather than as float64 — the comparison must not hide the precision
// loss the raw-bytes binding exists to prevent.
func decodeJSONSemantic(t *testing.T, s string) any {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v any
	require.Nil(t, dec.Decode(&v))
	return v
}

func syncOne(t *testing.T, syncable *sql.Syncable, key, raw string) {
	t.Helper()
	ss, err := syncable.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(eventType, []byte(key), []byte(raw)),
	}})
	require.Nil(t, err)
	require.Equal(t, cluster.ShouldSnapshot(true), ss)
}

// TestPostgreSQLIntegration_WholePayloadJSONB drives the full Syncable with a
// mixed manifest — scalar envelope column plus "$" whole-payload JSONB column
// — against real Postgres. JSONB normalizes key order, duplicate keys, and
// number formatting, so the contract is semantic JSON equality, asserted with
// UseNumber so the integer above 2^53 still has to survive digit-for-digit.
func TestPostgreSQLIntegration_WholePayloadJSONB(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := sql.NewDB(d, pgConnString)
	require.Nil(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTable(t, table)

	syncable := sql.New(db, wholePayloadConfig(table, "JSONB"))
	require.Nil(t, syncable.Init())
	defer syncable.Close()

	raw := `{"event_id":"evt-1","zfirst":true,"big":9007199254740993,"nested":{"b":2,"a":1}}`
	syncOne(t, syncable, "evt-1", raw)

	var eventID, payload string
	require.Nil(t, db.DB.QueryRow(
		"SELECT event_id, payload::text FROM "+table+" WHERE event_id = $1", "evt-1",
	).Scan(&eventID, &payload))
	require.Equal(t, "evt-1", eventID)
	require.Equal(t, decodeJSONSemantic(t, raw), decodeJSONSemantic(t, payload))

	// A second Actual with the same event_id upserts: the payload column is
	// replaced, not duplicated.
	updated := `{"event_id":"evt-1","zfirst":false,"big":9007199254740995}`
	syncOne(t, syncable, "evt-1", updated)

	var count int
	require.Nil(t, db.DB.QueryRow("SELECT COUNT(*) FROM "+table).Scan(&count))
	require.Equal(t, 1, count)
	require.Nil(t, db.DB.QueryRow(
		"SELECT payload::text FROM "+table+" WHERE event_id = $1", "evt-1",
	).Scan(&payload))
	require.Equal(t, decodeJSONSemantic(t, updated), decodeJSONSemantic(t, payload))
}

// TestPostgreSQLIntegration_WholePayloadTextByteExact: a TEXT column keeps
// the document byte-for-byte as submitted — key order, formatting, and the
// integer above 2^53 all intact (a float64 round trip would yield …992).
func TestPostgreSQLIntegration_WholePayloadTextByteExact(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := sql.NewDB(d, pgConnString)
	require.Nil(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTable(t, table)

	syncable := sql.New(db, wholePayloadConfig(table, "TEXT"))
	require.Nil(t, syncable.Init())
	defer syncable.Close()

	raw := `{"event_id":"evt-1","big":9007199254740993,"keys":"in submitted order"}`
	syncOne(t, syncable, "evt-1", raw)

	var payload string
	require.Nil(t, db.DB.QueryRow(
		"SELECT payload FROM "+table+" WHERE event_id = $1", "evt-1",
	).Scan(&payload))
	require.Equal(t, raw, payload)
}

// TestPostgreSQLIntegration_RefreshBoundarySweepsGapDeletedRows is the Stage 1
// red→green for the reconciling-refresh primitive: a keyed sink stamps each
// upsert with the entity's generation, and a refresh-boundary marker sweeps the
// rows left at an older generation. It reproduces the broken promise — a row
// deleted at the source during a lost change-data window survives an
// upsert-only re-snapshot — and proves the marker reconciles it, while a
// re-emitted row and a generation-0 (direct user) row survive.
func TestPostgreSQLIntegration_RefreshBoundarySweepsGapDeletedRows(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := sql.NewDB(d, pgConnString)
	require.Nil(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTable(t, table)

	cfg := &sql.Config{
		Topic: eventType.ID,
		Table: table,
		Mappings: []sql.Mapping{
			{JsonPath: "$.k", Column: "k", SQLType: "TEXT"},
			{JsonPath: "$.v", Column: "v", SQLType: "TEXT"},
		},
		PrimaryKey: "k",
	}
	syncable := sql.New(db, cfg)
	require.Nil(t, syncable.Init())
	defer syncable.Close()

	up := func(k, v string, gen uint64) *cluster.Entity {
		e := cluster.NewUpsertEntity(eventType, []byte(k), []byte(fmt.Sprintf(`{"k":%q,"v":%q}`, k, v)))
		e.Generation = gen
		return e
	}
	sync := func(a *cluster.Actual) {
		_, serr := syncable.Sync(context.Background(), a)
		require.NoError(t, serr)
	}
	present := func(k string) bool {
		var n int
		require.Nil(t, db.DB.QueryRow("SELECT COUNT(*) FROM "+table+" WHERE k = $1", k).Scan(&n))
		return n == 1
	}

	// Epoch 1 — initial snapshot of a, b, c.
	sync(&cluster.Actual{Index: 1, Entities: []*cluster.Entity{up("a", "1", 1), up("b", "2", 1), up("c", "3", 1)}})
	// A direct user write carries no refresh epoch (generation 0); the sweep's
	// >= 1 floor must never touch it.
	sync(&cluster.Actual{Index: 2, Entities: []*cluster.Entity{up("u", "user", 0)}})
	// Epoch 2 — a gap-recovery re-snapshot re-emits the rows still live at the
	// source (a, c) at the new epoch. b was DELETED at the source during the lost
	// window, so it is not re-emitted — an upsert-only re-snapshot cannot signal
	// its removal.
	sync(&cluster.Actual{Index: 3, Entities: []*cluster.Entity{up("a", "1b", 2), up("c", "3b", 2)}})

	// The broken promise: before the refresh boundary, the gap-deleted row still
	// sits on the sink at its stale epoch.
	require.True(t, present("b"), "gap-deleted row is retained by an upsert-only re-snapshot (the bug this fixes)")

	// The fix: the refresh-boundary marker at epoch 2 sweeps every row left at an
	// older generation.
	sync(&cluster.Actual{Index: 4, Entities: []*cluster.Entity{cluster.NewRefreshBoundaryEntity(eventType, 2)}})

	require.False(t, present("b"), "the gap-deleted row must be swept by the refresh boundary")
	require.True(t, present("a"), "a re-emitted row (stamped the new epoch) survives")
	require.True(t, present("c"), "a re-emitted row (stamped the new epoch) survives")
	require.True(t, present("u"), "a direct user write (generation 0) is never swept")

	// The survivors carry the epoch that last wrote them; the user row keeps 0.
	var aGen, uGen int64
	require.Nil(t, db.DB.QueryRow("SELECT "+sql.GenerationColumn+" FROM "+table+" WHERE k = $1", "a").Scan(&aGen))
	require.Equal(t, int64(2), aGen, "a was re-emitted at epoch 2")
	require.Nil(t, db.DB.QueryRow("SELECT "+sql.GenerationColumn+" FROM "+table+" WHERE k = $1", "u").Scan(&uGen))
	require.Equal(t, int64(0), uGen, "the user row is never stamped with a refresh epoch")
}
