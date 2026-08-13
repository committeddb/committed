//go:build docker

package sqlserver_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tcmssql "github.com/testcontainers/testcontainers-go/modules/mssql"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/sqlserver"
)

// The container runs SQL Server 2022 Developer. The image is amd64-only —
// Apple Silicon runs it under Rosetta emulation, so startup is slow; the
// wait strategy and per-test deadlines are sized generously.
const (
	saPassword = "Committed-T3st!"
	testDBName = "committed_test"
)

var ingestURL string

func TestMain(m *testing.M) {
	// Surface the dialect's zap logging (enable/snapshot/poll/retry lines) in
	// test output — the worker swallows errors into its retry loop, so
	// without this a misbehaving dialect times out silently.
	zap.ReplaceGlobals(zap.Must(zap.NewDevelopment()))

	ctx := context.Background()

	// COMMITTED_TEST_MSSQL_IMAGE overrides the server image so the suite can
	// run against a customer's exact major version (e.g.
	// mcr.microsoft.com/mssql/server:2019-latest — the first pilot's
	// production version). Default stays the newest supported.
	image := os.Getenv("COMMITTED_TEST_MSSQL_IMAGE")
	if image == "" {
		image = "mcr.microsoft.com/mssql/server:2022-latest"
	}
	container, err := tcmssql.Run(ctx,
		image,
		tcmssql.WithAcceptEULA(),
		tcmssql.WithPassword(saPassword),
	)
	if err != nil {
		log.Fatalf("could not start SQL Server container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		log.Fatalf("could not get host: %v", err)
	}
	port, err := container.MappedPort(ctx, "1433/tcp")
	if err != nil {
		log.Fatalf("could not get port: %v", err)
	}

	// Change Tracking cannot be enabled on system databases: create the user
	// database the tests (and the dialect's ALTER DATABASE CURRENT) target.
	masterURL := fmt.Sprintf("sqlserver://sa:%s@%s:%s?database=master", saPassword, host, port.Port())
	db, err := gosql.Open("sqlserver", masterURL)
	if err != nil {
		log.Fatalf("open master: %v", err)
	}
	if _, err := db.Exec("CREATE DATABASE " + testDBName); err != nil {
		log.Fatalf("create test database: %v", err)
	}
	_ = db.Close()

	ingestURL = fmt.Sprintf("sqlserver://sa:%s@%s:%s?database=%s", saPassword, host, port.Port(), testDBName)

	code := m.Run()
	// Ryuk (the testcontainers reaper) handles container cleanup.
	os.Exit(code)
}

func createDB(t *testing.T) *gosql.DB {
	t.Helper()
	db, err := gosql.Open("sqlserver", ingestURL)
	require.NoError(t, err)
	return db
}

// drainEntities collects user entities from pr (skipping refresh-boundary
// markers) until want entities arrived or the deadline passes.
func drainEntities(t *testing.T, pr <-chan *cluster.Proposal, po <-chan cluster.Position, want int, deadline time.Duration) []*cluster.Entity {
	t.Helper()
	var got []*cluster.Entity
	timeout := time.After(deadline)
	for len(got) < want {
		select {
		case p := <-pr:
			for _, e := range p.Entities {
				if e.IsRefreshBoundary() {
					continue
				}
				got = append(got, e)
			}
		case <-po:
		case <-timeout:
			t.Fatalf("timed out with %d of %d entities", len(got), want)
		}
	}
	return got
}

// TestSQLServerChangeTrackingEndToEnd is the smoke pass for the whole loop:
// snapshot existing rows, then live insert/update/delete through the CT poll
// — the delete arriving as a PK-keyed tombstone — plus the byte-parity pin:
// an update that rewrites a row to the same values must produce payload
// bytes identical to that row's snapshot entity (one read path, one decode
// path).
func TestSQLServerChangeTrackingEndToEnd(t *testing.T) {
	db := createDB(t)
	defer db.Close()
	_, err := db.Exec(`DROP TABLE IF EXISTS dbo.ct_e2e`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE dbo.ct_e2e (pk INT NOT NULL PRIMARY KEY, val NVARCHAR(100), n DECIMAL(10,2))`)
	require.NoError(t, err)
	for i := 1; i <= 3; i++ {
		_, err = db.Exec(fmt.Sprintf("INSERT INTO dbo.ct_e2e (pk, val, n) VALUES (%d, 'v%d', %d.50)", i, i, i))
		require.NoError(t, err)
	}

	typ := &cluster.Type{ID: "ct-e2e", Name: "ct-e2e"}
	config := &sql.Config{
		Type: typ,
		Mappings: []sql.Mapping{
			{JsonName: "pk", SQLColumn: "pk"},
			{JsonName: "val", SQLColumn: "val"},
			{JsonName: "n", SQLColumn: "n"},
		},
		PrimaryKey:       []string{"pk"},
		ConnectionString: ingestURL,
		Tables:           []string{"ct_e2e"},
		Options:          map[string]string{"poll_interval": "300ms"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pr := make(chan *cluster.Proposal, 256)
	po := make(chan cluster.Position, 256)
	d := &sqlserver.SQLServerDialect{}
	go func() { _ = d.Ingest(ctx, config, nil, 0, pr, po) }()

	// Snapshot: the three pre-existing rows.
	snap := drainEntities(t, pr, po, 3, 2*time.Minute)
	snapByKey := map[string]*cluster.Entity{}
	for _, e := range snap {
		require.False(t, e.IsDelete())
		snapByKey[string(e.Key)] = e
	}
	require.Len(t, snapByKey, 3)
	require.JSONEq(t, `{"pk":1,"val":"v1","n":1.50}`, string(snapByKey["1"].Data),
		"decimal must render as an exact unquoted number")

	// Live changes through the poll: insert, update, delete, and the parity
	// rewrite (row 3 to its same values).
	_, err = db.Exec("INSERT INTO dbo.ct_e2e (pk, val, n) VALUES (4, 'v4', 4.50)")
	require.NoError(t, err)
	_, err = db.Exec("UPDATE dbo.ct_e2e SET val = 'v1-updated' WHERE pk = 1")
	require.NoError(t, err)
	_, err = db.Exec("DELETE FROM dbo.ct_e2e WHERE pk = 2")
	require.NoError(t, err)
	_, err = db.Exec("UPDATE dbo.ct_e2e SET val = 'v3', n = 3.50 WHERE pk = 3")
	require.NoError(t, err)

	live := drainEntities(t, pr, po, 4, 2*time.Minute)
	var sawInsert, sawUpdate, sawDelete, sawParity bool
	for _, e := range live {
		switch string(e.Key) {
		case "4":
			require.False(t, e.IsDelete())
			require.JSONEq(t, `{"pk":4,"val":"v4","n":4.50}`, string(e.Data))
			sawInsert = true
		case "1":
			require.False(t, e.IsDelete())
			require.JSONEq(t, `{"pk":1,"val":"v1-updated","n":1.50}`, string(e.Data))
			sawUpdate = true
		case "2":
			require.True(t, e.IsDelete(), "a source DELETE must arrive as a keyed tombstone")
			sawDelete = true
		case "3":
			require.False(t, e.IsDelete())
			require.Equal(t, string(snapByKey["3"].Data), string(e.Data),
				"a same-value rewrite must be byte-identical to the snapshot payload (one read path)")
			sawParity = true
		}
	}
	require.True(t, sawInsert, "live insert")
	require.True(t, sawUpdate, "live update")
	require.True(t, sawDelete, "live delete tombstone")
	require.True(t, sawParity, "snapshot/CT byte parity")
}

// TestSQLServerEnablementOwnership pins the source-mutation etiquette: CT
// committed enabled (with its ownership marker) is disabled on teardown; CT
// that pre-existed the ingestable is NEVER touched.
func TestSQLServerEnablementOwnership(t *testing.T) {
	db := createDB(t)
	defer db.Close()

	// Table A: CT off — committed will enable and mark it.
	_, err := db.Exec(`DROP TABLE IF EXISTS dbo.ct_owned`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE dbo.ct_owned (pk INT NOT NULL PRIMARY KEY, v NVARCHAR(10))`)
	require.NoError(t, err)

	// Table B: CT pre-enabled by the "DBA" — committed must leave it alone.
	_, err = db.Exec(`DROP TABLE IF EXISTS dbo.ct_preexisting`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE dbo.ct_preexisting (pk INT NOT NULL PRIMARY KEY, v NVARCHAR(10))`)
	require.NoError(t, err)
	// Database-level CT may already be on from a prior test — enable-if-off.
	var dbCT int
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM sys.change_tracking_databases WHERE database_id = DB_ID()").Scan(&dbCT))
	if dbCT == 0 {
		_, err = db.Exec("ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)")
		require.NoError(t, err)
	}
	_, err = db.Exec("ALTER TABLE dbo.ct_preexisting ENABLE CHANGE_TRACKING")
	require.NoError(t, err)

	typ := &cluster.Type{ID: "ct-own", Name: "ct-own"}
	config := &sql.Config{
		Type:             typ,
		Mappings:         []sql.Mapping{{JsonName: "pk", SQLColumn: "pk"}, {JsonName: "v", SQLColumn: "v"}},
		PrimaryKey:       []string{"pk"},
		ConnectionString: ingestURL,
		Tables:           []string{"ct_owned", "ct_preexisting"},
		Options:          map[string]string{"poll_interval": "300ms"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	pr := make(chan *cluster.Proposal, 64)
	po := make(chan cluster.Position, 64)
	d := &sqlserver.SQLServerDialect{}
	go func() { _ = d.Ingest(ctx, config, nil, 0, pr, po) }()

	// Wait for the worker to reach streaming (both empty tables snapshotted →
	// a position lands), which guarantees enablement ran.
	select {
	case <-po:
	case <-time.After(2 * time.Minute):
		t.Fatal("worker never checkpointed")
	}
	cancel()

	ctEnabled := func(table string) bool {
		var n int
		require.NoError(t, db.QueryRow(
			"SELECT COUNT(*) FROM sys.change_tracking_tables WHERE object_id = OBJECT_ID(@p1)",
			"dbo."+table).Scan(&n))
		return n > 0
	}
	marked := func(table string) bool {
		var n int
		require.NoError(t, db.QueryRow(`
			SELECT COUNT(*) FROM sys.extended_properties
			WHERE class = 1 AND major_id = OBJECT_ID(@p1) AND minor_id = 0 AND name = 'committed_ct_enabled'`,
			"dbo."+table).Scan(&n))
		return n > 0
	}

	require.True(t, ctEnabled("ct_owned"), "committed must enable CT it needs")
	require.True(t, marked("ct_owned"), "committed-enabled CT must carry the ownership marker")
	require.True(t, ctEnabled("ct_preexisting"))
	require.False(t, marked("ct_preexisting"), "pre-existing CT must never be marked as ours")

	require.NoError(t, d.TeardownSource(config))

	require.False(t, ctEnabled("ct_owned"), "teardown must disable committed-enabled CT")
	require.False(t, marked("ct_owned"), "teardown must drop the ownership marker")
	require.True(t, ctEnabled("ct_preexisting"),
		"teardown must NEVER disable CT committed did not enable — the etiquette rule")
}
