//go:build docker

package harness

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql" // database/sql driver for the source/sink connection
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/committeddb/committed/e2e/cdc/dataset"
	"github.com/committeddb/committed/e2e/cdc/mutation"
)

// mysqlEngine is the MySQL backing of Engine: one MySQL container acting as both
// CDC source and syncable sink, driven via database/sql. It mirrors
// postgresEngine but with MySQL specifics — no replication slots (readiness gates
// on the ingestable status endpoint), `?` placeholders, MySQL DDL, and
// binlog_row_image=FULL (the mysql:9 default) in place of REPLICA IDENTITY. The
// source table for each topic is in the ingestable's `tables` filter, so the
// syncable's writes to <table>_sink never loop back (the same isolation Postgres
// gets from a publication that names only the source table).
type mysqlEngine struct {
	container *tcmysql.MySQLContainer
	ctx       context.Context
	db        *gosql.DB
	connURL   string // mysql://user:pass@host:port/db — URL both the ingest and sink configs parse
	hostDSN   string // user:pass@tcp(host:port)/db — the Go-driver DSN the engine's own *gosql.DB opens
}

func newMySQLEngine() *mysqlEngine { return &mysqlEngine{} }

func (*mysqlEngine) Dialect() string { return "mysql" }

// SlotName is empty — MySQL has no replication slots.
func (*mysqlEngine) SlotName(string) string { return "" }

func (e *mysqlEngine) ConnString() string { return e.connURL }

// Start brings up the MySQL container (binlog ROW/FULL on by default in mysql:9),
// opens the source/sink connection, and applies the MySQL TPC-H schema.
func (e *mysqlEngine) Start(ctx context.Context, t *testing.T) {
	t.Helper()
	e.ctx = ctx
	c, err := tcmysql.Run(ctx, "mysql:9",
		tcmysql.WithDatabase(mysqlDB),
		tcmysql.WithUsername(mysqlUser),
		tcmysql.WithPassword(mysqlPass),
		// GTID positioning (Phase B) needs gtid_mode=ON; enforce_gtid_consistency
		// is its required companion. binlog_row_metadata=FULL makes each binlog
		// TableMapEvent carry column names (default MINIMAL omits them), which the
		// ingest preflight requires. Set at startup so the snapshot's
		// @@gtid_executed and the binlog GTID events are populated.
		testcontainers.WithCmdArgs("--gtid-mode=ON", "--enforce-gtid-consistency=ON",
			"--binlog-row-metadata=FULL"),
		mysqlReadyWait(),
	)
	require.NoError(t, err, "start mysql container")
	e.container = c
	e.refreshConn(ctx, t)
	for _, stmt := range dataset.MySQLStatements() {
		_, err := e.db.ExecContext(ctx, stmt)
		require.NoError(t, err, "apply mysql schema")
	}
}

// refreshConn (re)reads the mapped host:port and opens a fresh connection.
func (e *mysqlEngine) refreshConn(ctx context.Context, t *testing.T) {
	t.Helper()
	host, err := e.container.Host(ctx)
	require.NoError(t, err, "mysql host")
	port, err := e.container.MappedPort(ctx, "3306/tcp")
	require.NoError(t, err, "mysql port")
	e.connURL = fmt.Sprintf("mysql://%s:%s@%s:%s/%s", mysqlUser, mysqlPass, host, port.Port(), mysqlDB)
	e.hostDSN = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", mysqlUser, mysqlPass, host, port.Port(), mysqlDB)
	db, err := gosql.Open("mysql", e.hostDSN)
	require.NoError(t, err, "open mysql")
	e.db = db
}

func (e *mysqlEngine) Close() {
	if e.db != nil {
		_ = e.db.Close()
		e.db = nil
	}
	if e.container != nil {
		_ = e.container.Terminate(context.Background())
		e.container = nil
	}
}

// RestartContainer restarts MySQL and reconnects (the mapped port can change).
func (e *mysqlEngine) RestartContainer(ctx context.Context) error {
	if e.db != nil {
		_ = e.db.Close()
		e.db = nil
	}
	if err := e.container.Stop(ctx, nil); err != nil {
		return fmt.Errorf("stop mysql: %w", err)
	}
	if err := e.container.Start(ctx); err != nil {
		return fmt.Errorf("start mysql: %w", err)
	}
	host, err := e.container.Host(ctx)
	if err != nil {
		return fmt.Errorf("mysql host after restart: %w", err)
	}
	port, err := e.container.MappedPort(ctx, "3306/tcp")
	if err != nil {
		return fmt.Errorf("mysql port after restart: %w", err)
	}
	e.connURL = fmt.Sprintf("mysql://%s:%s@%s:%s/%s", mysqlUser, mysqlPass, host, port.Port(), mysqlDB)
	e.hostDSN = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", mysqlUser, mysqlPass, host, port.Port(), mysqlDB)
	db, err := gosql.Open("mysql", e.hostDSN)
	if err != nil {
		return fmt.Errorf("reopen mysql after restart: %w", err)
	}
	e.db = db
	return nil
}

func (e *mysqlEngine) PostIngestable(t *testing.T, table string) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "[ingestable]\nname = %q\ntype = \"sql\"\n\n", table)
	fmt.Fprintf(&b, "[sql]\ndialect = \"mysql\"\n")
	fmt.Fprintf(&b, "topic = %q\n", table)
	fmt.Fprintf(&b, "connectionString = %q\n", e.connURL)
	fmt.Fprintf(&b, "primaryKey = %q\n", dataset.PrimaryKey(table))
	fmt.Fprintf(&b, "tables = [%q]\n\n", table)
	for _, col := range dataset.Columns(table) {
		fmt.Fprintf(&b, "[[sql.mappings]]\njsonName = %q\ncolumn = %q\n\n", col, col)
	}
	postConfig(t, "/v1/ingestable/"+table, b.String())
}

func (e *mysqlEngine) PostSinkDatabase(t *testing.T) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "[database]\nname = %q\ntype = \"sql\"\n\n", mysqlSinkDB)
	fmt.Fprintf(&b, "[sql]\ndialect = \"mysql\"\n")
	fmt.Fprintf(&b, "connectionString = %q\n", e.connURL)
	postConfig(t, "/v1/database/"+mysqlSinkDB, b.String())
}

// PostSyncable projects the topic into <table>_sink. The PK column is VARCHAR(64)
// (MySQL forbids a TEXT/BLOB primary key without a prefix length); other columns
// are VARCHAR(255) and the topic JSON values coerce into them.
func (*mysqlEngine) PostSyncable(t *testing.T, table string) {
	t.Helper()
	sink := SinkTable(table)
	pk := dataset.PrimaryKey(table)
	var b strings.Builder
	fmt.Fprintf(&b, "[syncable]\nname = %q\ntype = \"sql\"\n\n", table)
	fmt.Fprintf(&b, "[sql]\ntopic = %q\ndb = %q\ntable = %q\nprimaryKey = %q\n\n",
		table, mysqlSinkDB, sink, pk)
	for _, col := range dataset.Columns(table) {
		typ := "VARCHAR(255)"
		if col == pk {
			typ = "VARCHAR(64)"
		}
		fmt.Fprintf(&b, "[[sql.mappings]]\njsonPath = \"$.%s\"\ncolumn = %q\ntype = %q\n\n", col, col, typ)
	}
	postConfig(t, "/v1/syncable/"+table, b.String())
}

// WaitReady gates until the table's ingestable is streaming (snapshot complete),
// polling GET /v1/ingestable/{id}/status — the MySQL analogue of the Postgres
// slot gate (MySQL exposes no slot system view).
func (e *mysqlEngine) WaitReady(t *testing.T, table string) {
	t.Helper()
	if !waitIngestableStreaming(e.ctx, table, 60*time.Second) {
		t.Fatalf("ingestable %q never reached phase=streaming", table)
	}
}

func (e *mysqlEngine) WaitReadyCtx(ctx context.Context, table string, timeout time.Duration) {
	_ = waitIngestableStreaming(ctx, table, timeout)
}

// waitIngestableStreaming polls the ingestable status endpoint until
// phase=="streaming", returning false on timeout/cancel.
func waitIngestableStreaming(ctx context.Context, id string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(committedURL("/v1/ingestable/" + id + "/status")) //nolint:gosec // G107: fixed in-process URL
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			var st struct {
				Phase string `json:"phase"`
			}
			if json.Unmarshal(body, &st) == nil && st.Phase == "streaming" {
				return true
			}
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(100 * time.Millisecond):
		}
	}
	return false
}

func (e *mysqlEngine) SinkValue(table, pk, col string) (string, bool) {
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", col, SinkTable(table), dataset.PrimaryKey(table))
	var v gosql.NullString
	if err := e.db.QueryRowContext(e.ctx, q, pk).Scan(&v); err != nil {
		return "", false
	}
	return v.String, true
}

func (e *mysqlEngine) SinkCount(t *testing.T, table string) int {
	t.Helper()
	var n int
	if err := e.db.QueryRowContext(e.ctx,
		fmt.Sprintf("SELECT count(*) FROM %s", SinkTable(table))).Scan(&n); err != nil {
		return 0
	}
	return n
}

// Load is not yet implemented for MySQL: the scenario suite drives all data
// through the mutation Script (streaming path), never the bulk loader, so this
// stays a clear error until a snapshot-path test needs it.
func (*mysqlEngine) Load(context.Context, dataset.Dataset) error {
	return fmt.Errorf("mysqlEngine.Load: bulk load not implemented (scenarios mutate via Script)")
}

// Txn implements mutation.Execer over database/sql.
func (e *mysqlEngine) Txn(ctx context.Context, fn func(q mutation.Querier) error) error {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(mysqlQuerier{tx: tx}); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// mysqlQuerier adapts a *sql.Tx to mutation.Querier.
type mysqlQuerier struct{ tx *gosql.Tx }

func (q mysqlQuerier) Exec(ctx context.Context, query string, args ...any) error {
	_, err := q.tx.ExecContext(ctx, query, args...)
	return err
}

func (mysqlQuerier) Placeholder(int) string { return "?" }
