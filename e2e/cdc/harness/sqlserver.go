//go:build docker

package harness

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	tcmssql "github.com/testcontainers/testcontainers-go/modules/mssql"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	_ "github.com/microsoft/go-mssqldb" // database/sql driver for the harness's own source connection
)

// The SQL Server e2e fixture wires the CUSTOMER pipeline shape end to end:
// a SQL Server source (Change Tracking capture) → committed ingest → raft log
// → topic → the POSTGRES syncable → an external Postgres sink. Two containers,
// deliberately: committed has no SQL Server syncable (ingest-only by scope),
// and SQL Server → Postgres read models is the shape the dialect was built
// for. One source table keeps setup minimal — the dialect's breadth (resume
// states, composite PKs, the type matrix, enablement etiquette) is covered by
// internal/cluster/ingestable/sql/sqlserver's docker suite against real SQL
// Server; this harness proves the end-to-end committed pipeline, which
// nothing else exercises for SQL Server.
const (
	mssqlSAPassword = "Committed-T3st!"
	mssqlDB         = "cdc"
	mssqlTopic      = "widget"      // doubles as ingestable id, type id, and topic
	mssqlSource     = "widget"      // source table (dbo-scoped) the ingestable watches
	mssqlSink       = "widget_sink" // Postgres sink table the syncable projects into
	mssqlPK         = "wid"
	mssqlSinkDB     = "sssink" // sink database config id (the harness's Postgres)
)

// SQLServerHarness is the SQL Server counterpart to MySQLHarness: a SQL Server
// container as the CDC source, a Postgres container as the syncable sink, a
// committed child process, a sqlserver ingestable on the source table, and a
// postgres syncable projecting the topic into the sink. Reuses the
// engine-agnostic committed-process / HTTP helpers.
type SQLServerHarness struct {
	ms        *tcmssql.MSSQLServerContainer
	pg        *tcpostgres.PostgresContainer
	src       *gosql.DB // host-side SQL Server connection: drive source mutations
	sink      *pgx.Conn // host-side Postgres connection: read the sink
	connURL   string    // sqlserver://sa:pass@host:port?database=cdc — what the ingest config parses
	pgConnStr string    // the Postgres sink's connection string
	committed *committedProcess
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewSQLServer brings up SQL Server + Postgres + committed, creates an (empty)
// source table, wires the sqlserver ingestable and the postgres syncable, and
// blocks until the ingestable is streaming. The source table starts empty so
// all data flows through the Change Tracking poll path rather than the
// snapshot path.
func NewSQLServer(t *testing.T) *SQLServerHarness {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	h := &SQLServerHarness{ctx: ctx, cancel: cancel}

	// 1. SQL Server. The image is amd64-only (Rosetta on Apple Silicon), so
	// startup gets the module's generous default wait. Change Tracking needs
	// a user database — created here; the dialect enables CT itself (the
	// enablement etiquette is pinned by the dialect's docker suite).
	ms, err := tcmssql.Run(ctx,
		"mcr.microsoft.com/mssql/server:2022-latest",
		tcmssql.WithAcceptEULA(),
		tcmssql.WithPassword(mssqlSAPassword),
	)
	require.NoError(t, err, "start sql server container")
	h.ms = ms

	msHost, err := ms.Host(ctx)
	require.NoError(t, err, "sqlserver host")
	msPort, err := ms.MappedPort(ctx, "1433/tcp")
	require.NoError(t, err, "sqlserver port")

	masterURL := fmt.Sprintf("sqlserver://sa:%s@%s:%s?database=master", mssqlSAPassword, msHost, msPort.Port())
	master, err := gosql.Open("sqlserver", masterURL)
	require.NoError(t, err, "open master")
	_, err = master.ExecContext(ctx, "CREATE DATABASE "+mssqlDB)
	require.NoError(t, err, "create source database")
	require.NoError(t, master.Close())

	h.connURL = fmt.Sprintf("sqlserver://sa:%s@%s:%s?database=%s", mssqlSAPassword, msHost, msPort.Port(), mssqlDB)
	src, err := gosql.Open("sqlserver", h.connURL)
	require.NoError(t, err, "open sqlserver source")
	h.src = src

	// 2. Source table — empty, created BEFORE the ingestable so the (empty)
	// snapshot finds it and transitions straight to streaming.
	_, err = src.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE dbo.%s (%s NVARCHAR(32) NOT NULL PRIMARY KEY, name NVARCHAR(255))",
		mssqlSource, mssqlPK))
	require.NoError(t, err, "create source table")

	// 3. Postgres sink.
	pgC, pgConnStr := startPostgres(t)
	h.pg = pgC
	h.pgConnStr = pgConnStr
	sink, err := pgx.Connect(ctx, pgConnStr)
	require.NoError(t, err, "connect postgres sink")
	h.sink = sink

	// 4. committed.
	h.committed = startCommitted(t)

	// 5. Type + ingestable + readiness.
	postType(t, mssqlTopic)
	postSQLServerIngestable(t, h.connURL)
	h.waitForStreaming(t, mssqlTopic, 90*time.Second)

	// 6. Sink database + syncable, posted after the ingestable so the topic
	// already has a producer when the syncable starts.
	postSQLServerSinkDatabase(t, pgConnStr)
	postSQLServerSyncable(t)

	t.Cleanup(h.Close)
	return h
}

// postSQLServerIngestable registers the sqlserver ingestable on the source
// table: dialect, topic, the ?database= URL, primary key, table, mappings,
// and a fast poll cadence so tests see changes promptly.
func postSQLServerIngestable(t *testing.T, url string) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "[ingestable]\nname = %q\ntype = \"sql\"\n\n", mssqlTopic)
	fmt.Fprintf(&b, "[sql]\ndialect = \"sqlserver\"\n")
	fmt.Fprintf(&b, "topic = %q\n", mssqlTopic)
	fmt.Fprintf(&b, "connectionString = %q\n", url)
	fmt.Fprintf(&b, "primaryKey = %q\n", mssqlPK)
	fmt.Fprintf(&b, "tables = [%q]\n\n", mssqlSource)
	fmt.Fprintf(&b, "[sql.options]\npoll_interval = \"500ms\"\n\n")
	fmt.Fprintf(&b, "[[sql.mappings]]\njsonName = %q\ncolumn = %q\n\n", mssqlPK, mssqlPK)
	fmt.Fprintf(&b, "[[sql.mappings]]\njsonName = \"name\"\ncolumn = \"name\"\n\n")
	postConfig(t, "/v1/ingestable/"+mssqlTopic, b.String())
}

// postSQLServerSinkDatabase registers the Postgres sink database config.
func postSQLServerSinkDatabase(t *testing.T, connStr string) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "[database]\nname = %q\ntype = \"sql\"\n\n", mssqlSinkDB)
	fmt.Fprintf(&b, "[sql]\ndialect = \"postgres\"\n")
	fmt.Fprintf(&b, "connectionString = %q\n", connStr)
	postConfig(t, "/v1/database/"+mssqlSinkDB, b.String())
}

// postSQLServerSyncable projects the topic into the Postgres sink table with
// TEXT columns (the topic JSON values coerce).
func postSQLServerSyncable(t *testing.T) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "[syncable]\nname = %q\ntype = \"sql\"\n\n", mssqlTopic)
	fmt.Fprintf(&b, "[sql]\ntopic = %q\ndb = %q\ntable = %q\nprimaryKey = %q\n\n",
		mssqlTopic, mssqlSinkDB, mssqlSink, mssqlPK)
	fmt.Fprintf(&b, "[[sql.mappings]]\njsonPath = \"$.%s\"\ncolumn = %q\ntype = \"TEXT\"\n\n", mssqlPK, mssqlPK)
	fmt.Fprintf(&b, "[[sql.mappings]]\njsonPath = \"$.name\"\ncolumn = \"name\"\ntype = \"TEXT\"\n\n")
	postConfig(t, "/v1/syncable/"+mssqlTopic, b.String())
}

// waitForStreaming polls the ingestable status until phase=="streaming" — the
// same gate the MySQL harness uses (no slot system view to watch).
func (h *SQLServerHarness) waitForStreaming(t *testing.T, id string, timeout time.Duration) {
	t.Helper()
	if !waitIngestableStreaming(h.ctx, id, timeout) {
		t.Fatalf("ingestable %q never reached phase=streaming", id)
	}
}

// Exec runs a statement against the SQL Server SOURCE (use @p1, @p2, …
// placeholders). Tests drive the INSERT/UPDATE/DELETE that Change Tracking
// captures.
func (h *SQLServerHarness) Exec(t *testing.T, query string, args ...any) {
	t.Helper()
	_, err := h.src.ExecContext(h.ctx, query, args...)
	require.NoError(t, err, "source exec: %s", query)
}

// SinkValue reads column col of the Postgres sink row keyed by pk, and whether
// such a row exists. Missing table/row reports ("", false) — callers poll.
func (h *SQLServerHarness) SinkValue(pk, col string) (string, bool) {
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1", col, mssqlSink, mssqlPK)
	var v *string
	if err := h.sink.QueryRow(h.ctx, q, pk).Scan(&v); err != nil || v == nil {
		return "", false
	}
	return *v, true
}

// WaitForSinkValue polls until the sink row pk has col == want, or fails —
// the assertion that committed projected a change all the way to Postgres.
func (h *SQLServerHarness) WaitForSinkValue(t *testing.T, pk, col, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last, seen := "", false
	for time.Now().Before(deadline) {
		if v, ok := h.SinkValue(pk, col); ok {
			last, seen = v, true
			if v == want {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !seen {
		t.Fatalf("sink %s: no row with %s=%q within %s (syncable never wrote it)", mssqlSink, mssqlPK, pk, timeout)
	}
	t.Fatalf("sink %s: row %s=%q has %s=%q, wanted %q", mssqlSink, mssqlPK, pk, col, last, want)
}

// WaitForSinkAbsent polls until the sink row pk no longer exists — the
// delete-honored-end-to-end assertion. Confirm presence first so absence
// proves removal, not non-arrival.
func (h *SQLServerHarness) WaitForSinkAbsent(t *testing.T, pk string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, ok := h.SinkValue(pk, mssqlPK); !ok {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("sink %s: row %s=%q still present after %s (delete was not honored)", mssqlSink, mssqlPK, pk, timeout)
}

// SinkCount returns the number of rows in the sink table, or 0 before it exists.
func (h *SQLServerHarness) SinkCount(t *testing.T) int {
	t.Helper()
	var n int
	if err := h.sink.QueryRow(h.ctx, fmt.Sprintf("SELECT count(*) FROM %s", mssqlSink)).Scan(&n); err != nil {
		return 0
	}
	return n
}

// RestartCommitted stops committed and starts a fresh process over the same
// data dir, then waits for the ingestable to stream again: the sqlserver
// ingestable must resume from its persisted Change Tracking version (not
// re-snapshot) and the syncable worker must respawn from its SyncableIndex.
// SQL Server and Postgres are untouched.
func (h *SQLServerHarness) RestartCommitted(t *testing.T) {
	t.Helper()
	dataDir := h.committed.dataDir
	h.committed.Stop()
	h.committed = startCommittedAt(t, dataDir)
	h.waitForStreaming(t, mssqlTopic, 90*time.Second)
}

// Close releases all resources owned by the harness. Idempotent.
func (h *SQLServerHarness) Close() {
	if h.src != nil {
		_ = h.src.Close()
		h.src = nil
	}
	if h.sink != nil {
		_ = h.sink.Close(context.Background())
		h.sink = nil
	}
	if h.committed != nil {
		h.committed.Stop()
	}
	if h.ms != nil {
		_ = h.ms.Terminate(context.Background())
		h.ms = nil
	}
	if h.pg != nil {
		_ = h.pg.Terminate(context.Background())
		h.pg = nil
	}
	h.cancel()
}
