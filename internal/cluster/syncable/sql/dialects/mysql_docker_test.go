//go:build docker || integration

package dialects_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	networktypes "github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"

	"github.com/stretchr/testify/require"
)

const (
	mysqlDBName   = "committed_test"
	mysqlUser     = "root"
	mysqlPassword = "test"
)

// MySQL and Postgres are both first-class syncable targets, so the dialect gets
// the same end-to-end docker coverage. The dialects_test package already owns a
// Postgres container in TestMain (postgres_docker_test.go) and a package can have
// only one TestMain, so the MySQL container is started lazily on first use by a
// MySQL test instead — spun up once, shared by every MySQL test, and reaped by
// ryuk when the test process exits.
var (
	mysqlOnce       sync.Once
	mysqlConnString string // mysql:// URL for MySQLDialect.Open
	mysqlStartErr   error
)

// mysqlConn returns the shared MySQL container's mysql:// URL, starting the
// container on first call. WithReuseByName enables cross-invocation reuse for
// fast local iteration when ryuk is disabled (see the Postgres TestMain doc).
func mysqlConn(t *testing.T) string {
	t.Helper()
	mysqlOnce.Do(func() {
		ctx := context.Background()
		c, err := tcmysql.Run(ctx,
			"mysql:9",
			tcmysql.WithDatabase(mysqlDBName),
			tcmysql.WithUsername(mysqlUser),
			tcmysql.WithPassword(mysqlPassword),
			// Override the module's log-line readiness check, which is brittle on a
			// loaded CI host: ForSQL opens a real connection and runs a query,
			// retrying until it succeeds, so it returns only on true readiness (the
			// same override the ingest MySQL harness uses).
			testcontainers.WithWaitStrategy(
				wait.ForSQL("3306/tcp", "mysql", func(host string, port networktypes.Port) string {
					return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", mysqlUser, mysqlPassword, host, port.Port(), mysqlDBName)
				}).WithStartupTimeout(3*time.Minute),
			),
			testcontainers.WithReuseByName("committed-mysql-dialect-tests"),
		)
		if err != nil {
			mysqlStartErr = err
			return
		}
		host, herr := c.Host(ctx)
		if herr != nil {
			mysqlStartErr = herr
			return
		}
		port, perr := c.MappedPort(ctx, "3306/tcp")
		if perr != nil {
			mysqlStartErr = perr
			return
		}
		mysqlConnString = fmt.Sprintf("mysql://%s:%s@%s:%s/%s", mysqlUser, mysqlPassword, host, port.Port(), mysqlDBName)
	})
	require.NoError(t, mysqlStartErr, "failed to start MySQL container")
	return mysqlConnString
}

// dropTableMySQL tears a table down with a backtick-quoted DROP (the dialect's own
// DropDDL), so a special-character name cleans up correctly.
func dropTableMySQL(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	if _, err := db.DB.Exec((&dialects.MySQLDialect{}).DropDDL(&sql.Config{Table: table})); err != nil {
		t.Logf("dropTableMySQL %s: %v", table, err)
	}
}

// mysqlBacktick quotes an identifier for a raw verification query.
func mysqlBacktick(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

// TestMySQLIntegration_OpenAndPing verifies Open establishes a working connection
// to a real MySQL instance from a mysql:// URL.
func TestMySQLIntegration_OpenAndPing(t *testing.T) {
	d := &dialects.MySQLDialect{}
	db, err := d.Open(mysqlConn(t))
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.Ping())
}

// TestMySQLIntegration_CreateDDLAndUpsert verifies CreateDDL produces valid MySQL
// that creates the table, and CreateSQL upserts last-writer-wins on the primary
// key (the core syncable contract).
func TestMySQLIntegration_CreateDDLAndUpsert(t *testing.T) {
	d := &dialects.MySQLDialect{}
	db, err := sql.NewDB(d, mysqlConn(t))
	require.NoError(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTableMySQL(t, db, table)

	cfg := &sql.Config{
		Table: table,
		Mappings: []sql.Mapping{
			// MySQL cannot put a PRIMARY KEY on a TEXT column, so the key is VARCHAR.
			{Column: "id", SQLType: "VARCHAR(128)"},
			{Column: "name", SQLType: "TEXT"},
		},
		PrimaryKey: "id",
	}

	_, err = db.DB.Exec(d.CreateDDL(cfg))
	require.NoError(t, err, "CreateDDL output must be valid MySQL")

	insertSQL := d.CreateSQL(cfg)
	// BindArgs doubles the values for the ON DUPLICATE KEY UPDATE clause.
	_, err = db.DB.Exec(insertSQL, d.BindArgs([]any{"k1", "first"})...)
	require.NoError(t, err)
	_, err = db.DB.Exec(insertSQL, d.BindArgs([]any{"k1", "second"})...)
	require.NoError(t, err, "second insert with same PK must upsert, not error")

	var count int
	require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM "+mysqlBacktick(table)).Scan(&count))
	require.Equal(t, 1, count, "upsert leaves exactly one row")

	var name string
	require.NoError(t, db.DB.QueryRow("SELECT name FROM "+mysqlBacktick(table)+" WHERE id = ?", "k1").Scan(&name))
	require.Equal(t, "second", name, "upsert keeps the most recent value")
}

// TestMySQLIntegration_FullSyncableFlow drives the full Syncable against MySQL the
// way sync.go uses it: Init (DDL) then Sync per Actual, including an upsert.
func TestMySQLIntegration_FullSyncableFlow(t *testing.T) {
	d := &dialects.MySQLDialect{}
	db, err := sql.NewDB(d, mysqlConn(t))
	require.NoError(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTableMySQL(t, db, table)

	cfg := &sql.Config{
		Topic: eventType.ID,
		Table: table,
		Mappings: []sql.Mapping{
			{JsonPath: "$.pk", Column: "pk", SQLType: "VARCHAR(128)"},
			{JsonPath: "$.value", Column: "value", SQLType: "TEXT"},
		},
		PrimaryKey: "pk",
	}
	syncable := sql.New(db, cfg)
	require.NoError(t, syncable.Init(), "quoted CreateDDL must be valid MySQL")
	defer syncable.Close()

	up := func(pk, value string) *cluster.Actual {
		return &cluster.Actual{Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(eventType, []byte(pk), []byte(fmt.Sprintf(`{"pk":%q,"value":%q}`, pk, value))),
		}}
	}
	for _, a := range []*cluster.Actual{up("a", "1"), up("b", "2"), up("a", "3")} {
		_, serr := syncable.Sync(context.Background(), a)
		require.NoError(t, serr)
	}

	var count int
	require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM "+mysqlBacktick(table)).Scan(&count))
	require.Equal(t, 2, count, "two distinct keys after one upsert")

	var aValue string
	require.NoError(t, db.DB.QueryRow("SELECT value FROM "+mysqlBacktick(table)+" WHERE pk = ?", "a").Scan(&aValue))
	require.Equal(t, "3", aValue)
}

// TestMySQLIntegration_SpecialIdentifiersRoundTrip is the MySQL mirror of the
// Postgres identifier-quoting proof: a syncable whose table and columns are a
// reserved word, a hyphen, a space, and mixed case — each a syntax error if
// interpolated raw — creates its table, upserts on its quoted primary key, and
// honors a delete on its quoted key column against real MySQL.
func TestMySQLIntegration_SpecialIdentifiersRoundTrip(t *testing.T) {
	d := &dialects.MySQLDialect{}
	db, err := sql.NewDB(d, mysqlConn(t))
	require.NoError(t, err)
	defer db.Close()

	table := fmt.Sprintf("Order-Items %d", time.Now().UnixNano())
	defer dropTableMySQL(t, db, table)

	cfg := &sql.Config{
		Topic: eventType.ID,
		Table: table,
		Mappings: []sql.Mapping{
			// VARCHAR (not TEXT) so the space-named column can be the primary key.
			{JsonPath: "$.id", Column: "User Id", SQLType: "VARCHAR(64)"},
			{JsonPath: "$.sel", Column: "select", SQLType: "TEXT"},
		},
		PrimaryKey: "User Id",
		KeyColumn:  "User Id",
	}
	syncable := sql.New(db, cfg)
	require.NoError(t, syncable.Init(), "quoted CreateDDL must be valid MySQL")
	defer syncable.Close()

	up := func(id, sel string) *cluster.Actual {
		return &cluster.Actual{Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(eventType, []byte(id), []byte(fmt.Sprintf(`{"id":%q,"sel":%q}`, id, sel))),
		}}
	}
	_, err = syncable.Sync(context.Background(), up("a", "one"))
	require.NoError(t, err)
	_, err = syncable.Sync(context.Background(), up("a", "two"))
	require.NoError(t, err)

	var sel string
	require.NoError(t, db.DB.QueryRow(
		"SELECT "+mysqlBacktick("select")+" FROM "+mysqlBacktick(table)+" WHERE "+mysqlBacktick("User Id")+" = ?", "a").Scan(&sel))
	require.Equal(t, "two", sel, "upsert on the quoted primary key replaced the value")

	var count int
	require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM "+mysqlBacktick(table)).Scan(&count))
	require.Equal(t, 1, count, "upsert left exactly one row")

	_, err = syncable.Sync(context.Background(), &cluster.Actual{
		Entities: []*cluster.Entity{cluster.NewDeleteEntity(eventType, []byte("a"))},
	})
	require.NoError(t, err)

	require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM "+mysqlBacktick(table)).Scan(&count))
	require.Equal(t, 0, count, "delete on the quoted key column removed the row")
}

// TestMySQLIntegration_WholePayloadJSON drives the "$" whole-payload mapping into
// a MySQL JSON column: a scalar envelope column plus the entire document. MySQL
// JSON normalizes key order and whitespace, so the contract is semantic JSON
// equality, and a same-key second Actual upserts rather than duplicating.
func TestMySQLIntegration_WholePayloadJSON(t *testing.T) {
	d := &dialects.MySQLDialect{}
	db, err := sql.NewDB(d, mysqlConn(t))
	require.NoError(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTableMySQL(t, db, table)

	syncable := sql.New(db, wholePayloadConfig(table, "JSON"))
	require.NoError(t, syncable.Init())
	defer syncable.Close()

	raw := `{"event_id":"evt-1","zfirst":true,"big":9007199254740993,"nested":{"b":2,"a":1}}`
	syncOne(t, syncable, "evt-1", raw)

	var eventID, payload string
	require.NoError(t, db.DB.QueryRow(
		"SELECT event_id, payload FROM "+mysqlBacktick(table)+" WHERE event_id = ?", "evt-1",
	).Scan(&eventID, &payload))
	require.Equal(t, "evt-1", eventID)
	require.Equal(t, decodeJSONSemantic(t, raw), decodeJSONSemantic(t, payload))

	updated := `{"event_id":"evt-1","zfirst":false,"big":9007199254740995}`
	syncOne(t, syncable, "evt-1", updated)

	var count int
	require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM "+mysqlBacktick(table)).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.DB.QueryRow(
		"SELECT payload FROM "+mysqlBacktick(table)+" WHERE event_id = ?", "evt-1",
	).Scan(&payload))
	require.Equal(t, decodeJSONSemantic(t, updated), decodeJSONSemantic(t, payload))
}

// TestMySQLIntegration_KeylessAppendIdempotentOnReplay is the MySQL half of the
// no-primaryKey replay regression: an append syncable dedups on a sidecar keyed by
// (raft index, entity ordinal), so re-syncing the same Actuals never duplicates
// rows, and the user's table stays free of the dedup columns.
func TestMySQLIntegration_KeylessAppendIdempotentOnReplay(t *testing.T) {
	d := &dialects.MySQLDialect{}
	db, err := sql.NewDB(d, mysqlConn(t))
	require.NoError(t, err)
	defer db.Close()

	table := "kless_append_replay_my" // short: sidecar name must fit the 64-char limit
	dropTableMySQL(t, db, table)
	dropTableMySQL(t, db, sql.AppliedSidecarName(table))
	defer dropTableMySQL(t, db, table)
	defer dropTableMySQL(t, db, sql.AppliedSidecarName(table))

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
	require.NoError(t, syncable.Init())
	defer syncable.Close()

	up := func(k, v string) *cluster.Entity {
		return cluster.NewUpsertEntity(eventType, []byte(k), []byte(fmt.Sprintf(`{"k":%q,"v":%q}`, k, v)))
	}
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

	apply()
	var count int
	require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM "+mysqlBacktick(table)).Scan(&count))
	require.Equal(t, 5, count, "append keeps one row per entity, including the repeated key 'a'")

	apply() // replay — every (index, seq) is already marked
	require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM "+mysqlBacktick(table)).Scan(&count))
	require.Equal(t, 5, count, "replay is a no-op: the dedup sidecar suppresses re-appends")
}
