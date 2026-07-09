//go:build docker

package harness

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql" // database/sql driver for the harness's own source/sink connection
	networktypes "github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"
)

// The harness opens *sql.DB pools against MySQL containers that Ryuk tears down
// at test end. As each stopped server drops its pooled connections, go-sql-driver
// logs "unexpected EOF" for every one — thousands of benign lines per run that
// bury real failures in CI output. Filter that single message at the driver's
// package logger; forward anything else unchanged so genuine driver errors stay
// visible.
func init() {
	_ = mysql.SetLogger(quietMySQLLog{out: log.New(os.Stderr, "[mysql] ", log.LstdFlags)})
}

type quietMySQLLog struct{ out *log.Logger }

func (q quietMySQLLog) Print(v ...any) {
	if strings.Contains(fmt.Sprint(v...), "unexpected EOF") {
		return
	}
	q.out.Print(v...)
}

// mysqlReadyWait is the container readiness strategy both MySQL harnesses use in
// place of the mysql module's default log-line check. The default watches for a
// specific "port: 3306  MySQL Community Server" line that a slow CI host can
// leave unmatched inside the timeout, and a log line can't tell "logged ready"
// from "accepts a query" — the entrypoint's init server and a mid-startup
// restart both drop early connections with EOF. ForSQL opens a real connection
// and runs a query, retrying until it succeeds, so it returns only on true
// readiness; the generous timeout absorbs slow container starts on loaded CI.
func mysqlReadyWait() testcontainers.CustomizeRequestOption {
	return testcontainers.WithWaitStrategy(
		wait.ForSQL("3306/tcp", "mysql", func(host string, port networktypes.Port) string {
			return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", mysqlUser, mysqlPass, host, port.Port(), mysqlDB)
		}).WithStartupTimeout(3 * time.Minute),
	)
}

// The MySQL e2e fixture wires a single source table → topic → sink table. One
// table keeps setup minimal: the MySQL ingest dialect's breadth (snapshot
// chunking, position resume, transaction grouping, typed binlog values) is
// already covered by internal/cluster/ingestable/sql/mysql/mysql_test.go against
// real MySQL. This harness proves the *end-to-end committed pipeline* — MySQL
// CDC → ingest → raft log → topic → syncable → external sink — which nothing
// else exercises for MySQL.
const (
	mysqlDB     = "cdc"
	mysqlUser   = "root" // canal needs the REPLICATION grants; root has them
	mysqlPass   = "secret"
	mysqlTopic  = "widget" // doubles as the ingestable id, the type id, and the topic
	mysqlSource = "widget" // source table the ingestable watches
	mysqlSink   = "widget_sink"
	mysqlPK     = "wid"
	mysqlSinkDB = "mysink" // sink database config id (distinct from the postgres harness's "sink")
)

// MySQLHarness is the MySQL counterpart to Harness: one MySQL container acting
// as BOTH the CDC source and the syncable sink, a committed child process, a
// MySQL ingestable on the source table, and a MySQL syncable projecting the
// topic into a sink table. It reuses the engine-agnostic committed-process / HTTP
// helpers (startCommitted, postConfig, postType, committedURL) and adds only the
// MySQL-specific source+sink wiring.
//
// Source and sink share one MySQL instance but different tables; only the source
// table is in the ingestable's `tables` list, so the syncable's writes to the
// sink never loop back — the binlog carries them but the dialect filters by
// table (the same isolation the Postgres harness gets from a publication that
// names only the source table).
type MySQLHarness struct {
	my        *tcmysql.MySQLContainer
	db        *gosql.DB // host-side connection: drive source mutations, read the sink
	ingestURL string    // mysql://root:secret@host:port/db — URL form the ingest dialect parses
	sinkDSN   string    // root:secret@tcp(host:port)/db — Go-driver DSN the syncable opens
	committed *committedProcess
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewMySQL brings up MySQL + committed, creates an (empty) source table, wires a
// MySQL ingestable and a MySQL syncable, and blocks until the ingestable is
// streaming. The source table starts empty so all data flows through the binlog
// streaming path (typed values) rather than the snapshot path.
func NewMySQL(t *testing.T) *MySQLHarness {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	h := &MySQLHarness{ctx: ctx, cancel: cancel}

	// 1. MySQL. mysql:9 ships binlog enabled by default (log_bin,
	// binlog_format=ROW, binlog_row_image=FULL, a non-zero server_id). Only
	// binlog_row_metadata needs setting: it defaults to MINIMAL, but committed
	// decodes each row against the column names carried in its binlog
	// TableMapEvent, which the server emits only under FULL — the ingest
	// preflight requires it.
	my, err := tcmysql.Run(ctx, "mysql:9",
		tcmysql.WithDatabase(mysqlDB),
		tcmysql.WithUsername(mysqlUser),
		tcmysql.WithPassword(mysqlPass),
		testcontainers.WithCmdArgs("--binlog-row-metadata=FULL"),
		mysqlReadyWait(),
	)
	require.NoError(t, err, "start mysql container")
	h.my = my

	host, err := my.Host(ctx)
	require.NoError(t, err, "mysql host")
	port, err := my.MappedPort(ctx, "3306/tcp")
	require.NoError(t, err, "mysql port")
	h.ingestURL = fmt.Sprintf("mysql://%s:%s@%s:%s/%s", mysqlUser, mysqlPass, host, port.Port(), mysqlDB)
	h.sinkDSN = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", mysqlUser, mysqlPass, host, port.Port(), mysqlDB)

	db, err := gosql.Open("mysql", h.sinkDSN)
	require.NoError(t, err, "open mysql")
	h.db = db

	// 2. Source table — empty, created BEFORE the ingestable so the (empty)
	// snapshot finds it and transitions straight to streaming.
	_, err = db.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s (%s VARCHAR(32) NOT NULL, name VARCHAR(255), PRIMARY KEY (%s))",
		mysqlSource, mysqlPK, mysqlPK))
	require.NoError(t, err, "create source table")

	// 3. committed.
	h.committed = startCommitted(t)

	// 4. Type + ingestable + readiness.
	postType(t, mysqlTopic)
	postMySQLIngestable(t, h.ingestURL)
	h.waitForStreaming(t, mysqlTopic, 60*time.Second)

	// 5. Sink database + syncable (project topic → sink table). Posted after the
	// ingestable so the topic already has a producer when the syncable starts.
	postMySQLSinkDatabase(t, h.sinkDSN)
	postMySQLSyncable(t)

	t.Cleanup(h.Close)
	return h
}

// postMySQLIngestable registers a MySQL ingestable on the source table. Unlike
// Postgres (which needs a [sql.postgres] slot_name/publication subsection),
// MySQL needs only the dialect, topic, connection URL, primary key, table, and
// column mappings.
func postMySQLIngestable(t *testing.T, url string) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "[ingestable]\nname = %q\ntype = \"sql\"\n\n", mysqlTopic)
	fmt.Fprintf(&b, "[sql]\ndialect = \"mysql\"\n")
	fmt.Fprintf(&b, "topic = %q\n", mysqlTopic)
	fmt.Fprintf(&b, "connectionString = %q\n", url)
	fmt.Fprintf(&b, "primaryKey = %q\n", mysqlPK)
	fmt.Fprintf(&b, "tables = [%q]\n\n", mysqlSource)
	fmt.Fprintf(&b, "[[sql.mappings]]\njsonName = %q\ncolumn = %q\n\n", mysqlPK, mysqlPK)
	fmt.Fprintf(&b, "[[sql.mappings]]\njsonName = \"name\"\ncolumn = \"name\"\n\n")
	postConfig(t, "/v1/ingestable/"+mysqlTopic, b.String())
}

// postMySQLSinkDatabase registers the sink database — the same MySQL instance,
// reached via the Go-driver DSN the mysql syncable dialect opens with
// gosql.Open("mysql", ...).
func postMySQLSinkDatabase(t *testing.T, dsn string) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "[database]\nname = %q\ntype = \"sql\"\n\n", mysqlSinkDB)
	fmt.Fprintf(&b, "[sql]\ndialect = \"mysql\"\n")
	fmt.Fprintf(&b, "connectionString = %q\n", dsn)
	postConfig(t, "/v1/database/"+mysqlSinkDB, b.String())
}

// postMySQLSyncable projects the topic into the sink table. The PK column is
// VARCHAR(64) — MySQL forbids a TEXT/BLOB primary key without a prefix length —
// and other columns are VARCHAR(255); topic JSON values coerce into them.
func postMySQLSyncable(t *testing.T) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "[syncable]\nname = %q\ntype = \"sql\"\n\n", mysqlTopic)
	fmt.Fprintf(&b, "[sql]\ntopic = %q\ndb = %q\ntable = %q\nprimaryKey = %q\n\n",
		mysqlTopic, mysqlSinkDB, mysqlSink, mysqlPK)
	fmt.Fprintf(&b, "[[sql.mappings]]\njsonPath = \"$.%s\"\ncolumn = %q\ntype = \"VARCHAR(64)\"\n\n", mysqlPK, mysqlPK)
	fmt.Fprintf(&b, "[[sql.mappings]]\njsonPath = \"$.name\"\ncolumn = \"name\"\ntype = \"VARCHAR(255)\"\n\n")
	postConfig(t, "/v1/syncable/"+mysqlTopic, b.String())
}

// waitForStreaming polls GET /v1/ingestable/{id}/status until phase=="streaming"
// — the MySQL analogue of the Postgres harness's pg_replication_slots gate
// (MySQL exposes no slot system view). Streaming means the snapshot is complete
// and the dialect is tailing the binlog, so mutations issued after this can't
// race the ingestable's startup and double-count as snapshot+stream events.
func (h *MySQLHarness) waitForStreaming(t *testing.T, id string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	lastPhase := "<none>"
	for time.Now().Before(deadline) {
		resp, err := http.Get(committedURL("/v1/ingestable/" + id + "/status")) //nolint:gosec // G107: fixed in-process URL
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			var st struct {
				Phase string `json:"phase"`
			}
			if json.Unmarshal(body, &st) == nil {
				lastPhase = st.Phase
				if st.Phase == "streaming" {
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("ingestable %q never reached phase=streaming (last observed %q)", id, lastPhase)
}

// Exec runs a statement against the MySQL SOURCE table. Tests use it to drive
// the INSERT/UPDATE/DELETE that the binlog captures.
func (h *MySQLHarness) Exec(t *testing.T, query string, args ...any) {
	t.Helper()
	_, err := h.db.ExecContext(h.ctx, query, args...)
	require.NoError(t, err, "source exec: %s", query)
}

// SinkValue reads column `col` of the sink row keyed by pk, and whether such a
// row exists. A missing table/row reports ("", false) — callers poll.
func (h *MySQLHarness) SinkValue(pk, col string) (string, bool) {
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", col, mysqlSink, mysqlPK)
	var v gosql.NullString
	if err := h.db.QueryRowContext(h.ctx, q, pk).Scan(&v); err != nil {
		return "", false
	}
	return v.String, true
}

// WaitForSinkValue polls until the sink row pk has col == want, or fails after
// timeout — the MySQL analogue of Harness.WaitForSinkValue. It is how a test
// asserts committed projected a proposal all the way out to the external DB.
func (h *MySQLHarness) WaitForSinkValue(t *testing.T, pk, col, want string, timeout time.Duration) {
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
		t.Fatalf("sink %s: no row with %s=%q within %s (syncable never wrote it)", mysqlSink, mysqlPK, pk, timeout)
	}
	t.Fatalf("sink %s: row %s=%q has %s=%q, wanted %q", mysqlSink, mysqlPK, pk, col, last, want)
}

// WaitForSinkAbsent polls until the sink row pk no longer exists, or fails after
// timeout. This asserts a delete was honored end-to-end: the syncable translated
// the delete Actual into a DELETE that removed the row. Callers should confirm
// the row was present first so absence proves removal, not non-arrival.
func (h *MySQLHarness) WaitForSinkAbsent(t *testing.T, pk string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, ok := h.SinkValue(pk, mysqlPK); !ok {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("sink %s: row %s=%q still present after %s (delete was not honored)", mysqlSink, mysqlPK, pk, timeout)
}

// SinkCount returns the number of rows in the sink table, or 0 if it does not
// exist yet.
func (h *MySQLHarness) SinkCount(t *testing.T) int {
	t.Helper()
	var n int
	if err := h.db.QueryRowContext(h.ctx, fmt.Sprintf("SELECT count(*) FROM %s", mysqlSink)).Scan(&n); err != nil {
		return 0
	}
	return n
}

// RestartCommitted stops committed and starts a fresh process over the same data
// dir, then waits for the ingestable to stream again — the MySQL analogue of
// Harness.RestartCommitted. On restart committed re-reads the persisted
// IngestablePosition (the checkpointed binlog file+offset) and canal resumes
// from it rather than re-snapshotting, while the syncable worker respawns and
// resumes from its persisted SyncableIndex. MySQL itself is untouched.
func (h *MySQLHarness) RestartCommitted(t *testing.T) {
	t.Helper()
	dataDir := h.committed.dataDir
	h.committed.Stop()
	h.committed = startCommittedAt(t, dataDir)
	h.waitForStreaming(t, mysqlTopic, 60*time.Second)
}

// Close releases all resources owned by the harness. Idempotent.
func (h *MySQLHarness) Close() {
	if h.db != nil {
		_ = h.db.Close()
		h.db = nil
	}
	if h.committed != nil {
		h.committed.Stop()
	}
	if h.my != nil {
		_ = h.my.Terminate(context.Background())
		h.my = nil
	}
	if h.cancel != nil {
		h.cancel()
	}
}
