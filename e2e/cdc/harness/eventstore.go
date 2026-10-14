//go:build docker

package harness

import (
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcmssql "github.com/testcontainers/testcontainers-go/modules/mssql"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
)

// EventStore is the fixture for docs/recipes/event-store-read-models.md: an
// events table of the recipe's shape in a source engine, a Postgres database
// for the read model (the recipe's [database] is Postgres whatever the
// source), and a committed node. The test posts the recipe's own configs.
type EventStore struct {
	Engine string // "postgres", "mysql", or "sqlserver"
	// SrcURL is the source connection string for the ingestable, and ReadURL
	// the read-model database's (search_path=read). Real passwords: Post
	// externalizes them before they reach committed.
	SrcURL, ReadURL string

	src       sourceDB
	read      *pgx.Conn
	committed *committedProcess
	cleanup   []func()
}

// EventStoreVariant picks the source engine and its payload column type.
type EventStoreVariant struct {
	Engine      string // "postgres", "mysql", "sqlserver"
	PayloadType string // "jsonb"; MySQL "JSON" or "TEXT"; SQL Server "NVARCHAR(MAX)"
}

// sourceDB runs statements against the source with its own placeholder
// syntax: Postgres $1, MySQL ?, SQL Server @p1.
type sourceDB interface {
	exec(ctx context.Context, q string, args ...any) error
	txn(ctx context.Context, fn func(exec func(q string, args ...any) error) error) error
	ph(i int) string
	close()
}

type pgSource struct{ c *pgx.Conn }

func (s pgSource) exec(ctx context.Context, q string, args ...any) error {
	_, err := s.c.Exec(ctx, q, args...)
	return err
}

func (s pgSource) txn(ctx context.Context, fn func(func(string, ...any) error) error) error {
	tx, err := s.c.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(func(q string, args ...any) error { _, err := tx.Exec(ctx, q, args...); return err }); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}
func (pgSource) ph(i int) string { return fmt.Sprintf("$%d", i) }
func (s pgSource) close()        { _ = s.c.Close(context.Background()) }

type sqlSource struct {
	db     *gosql.DB
	prefix string // "?" for MySQL, "@p" for SQL Server
}

func (s sqlSource) exec(ctx context.Context, q string, args ...any) error {
	_, err := s.db.ExecContext(ctx, q, args...)
	return err
}

func (s sqlSource) txn(ctx context.Context, fn func(func(string, ...any) error) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(func(q string, args ...any) error { _, err := tx.ExecContext(ctx, q, args...); return err }); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s sqlSource) ph(i int) string {
	if s.prefix == "?" {
		return "?"
	}
	return fmt.Sprintf("%s%d", s.prefix, i)
}
func (s sqlSource) close() { _ = s.db.Close() }

// NewEventStore starts the source engine with an empty events table, a
// Postgres read-model database (the same instance as the source when the
// source is Postgres), and a committed node. Nothing is posted.
func NewEventStore(t *testing.T, v EventStoreVariant) *EventStore {
	t.Helper()
	ctx := context.Background()
	e := &EventStore{Engine: v.Engine}
	t.Cleanup(e.close)

	pgC, pgConnStr := startPostgres(t)
	e.cleanup = append(e.cleanup, func() { _ = pgC.Terminate(context.Background()) })
	read, err := pgx.Connect(ctx, pgConnStr)
	require.NoError(t, err, "connect read-model postgres")
	e.read = read
	_, err = read.Exec(ctx, "CREATE SCHEMA read")
	require.NoError(t, err)
	e.ReadURL = withQuery(pgConnStr, "search_path=read")

	switch v.Engine {
	case "postgres":
		src, err := pgx.Connect(ctx, pgConnStr)
		require.NoError(t, err)
		e.src = pgSource{c: src}
		e.SrcURL = pgConnStr
		require.NoError(t, e.src.exec(ctx, recipeEventsDDL(t)), "the recipe's CREATE TABLE")
	case "mysql":
		my, err := tcmysql.Run(ctx, "mysql:9",
			tcmysql.WithDatabase(mysqlDB), tcmysql.WithUsername(mysqlUser), tcmysql.WithPassword(mysqlPass),
			testcontainers.WithCmdArgs("--binlog-row-metadata=FULL"), mysqlReadyWait())
		require.NoError(t, err, "start mysql")
		e.cleanup = append(e.cleanup, func() { _ = my.Terminate(context.Background()) })
		host, err := my.Host(ctx)
		require.NoError(t, err)
		port, err := my.MappedPort(ctx, "3306/tcp")
		require.NoError(t, err)
		e.SrcURL = fmt.Sprintf("mysql://%s:%s@%s:%s/%s", mysqlUser, mysqlPass, host, port.Port(), mysqlDB)
		db, err := gosql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", mysqlUser, mysqlPass, host, port.Port(), mysqlDB))
		require.NoError(t, err)
		e.src = sqlSource{db: db, prefix: "?"}
		require.NoError(t, e.src.exec(ctx, fmt.Sprintf(`CREATE TABLE events (
  seq_id    BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
  stream_id CHAR(36)     NOT NULL,
  version   INT          NOT NULL,
  type      VARCHAR(64)  NOT NULL,
  data      %s           NOT NULL,
  timestamp TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  UNIQUE (stream_id, version))`, v.PayloadType)))
	case "sqlserver":
		ms, err := tcmssql.Run(ctx, "mcr.microsoft.com/mssql/server:2022-latest",
			tcmssql.WithAcceptEULA(), tcmssql.WithPassword(mssqlSAPassword))
		require.NoError(t, err, "start sql server")
		e.cleanup = append(e.cleanup, func() { _ = ms.Terminate(context.Background()) })
		host, err := ms.Host(ctx)
		require.NoError(t, err)
		port, err := ms.MappedPort(ctx, "1433/tcp")
		require.NoError(t, err)
		master, err := gosql.Open("sqlserver", fmt.Sprintf("sqlserver://sa:%s@%s:%s?database=master", mssqlSAPassword, host, port.Port()))
		require.NoError(t, err)
		_, err = master.ExecContext(ctx, "CREATE DATABASE "+mssqlDB)
		require.NoError(t, err)
		require.NoError(t, master.Close())
		e.SrcURL = fmt.Sprintf("sqlserver://sa:%s@%s:%s?database=%s", mssqlSAPassword, host, port.Port(), mssqlDB)
		db, err := gosql.Open("sqlserver", e.SrcURL)
		require.NoError(t, err)
		e.src = sqlSource{db: db, prefix: "@p"}
		require.NoError(t, e.src.exec(ctx, fmt.Sprintf(`CREATE TABLE dbo.events (
  seq_id      BIGINT           NOT NULL IDENTITY PRIMARY KEY,
  stream_id   UNIQUEIDENTIFIER NOT NULL,
  version     INT              NOT NULL,
  type        NVARCHAR(64)     NOT NULL,
  data        %s               NOT NULL,
  [timestamp] DATETIME2        NOT NULL DEFAULT SYSUTCDATETIME(),
  UNIQUE (stream_id, version))`, v.PayloadType)))
	default:
		t.Fatalf("unknown engine %q", v.Engine)
	}

	e.committed = startCommitted(t)
	return e
}

func (e *EventStore) close() {
	if e.committed != nil {
		e.committed.Stop()
	}
	if e.src != nil {
		e.src.close()
	}
	if e.read != nil {
		_ = e.read.Close(context.Background())
	}
	for i := len(e.cleanup) - 1; i >= 0; i-- {
		e.cleanup[i]()
	}
}

// withQuery adds q to a URL's query string.
func withQuery(url, q string) string {
	if strings.Contains(url, "?") {
		return url + "&" + q
	}
	return url + "?" + q
}

// Event is one row of the recipe's events table.
type Event struct {
	Stream  string
	Version int
	Type    string
	Data    string
}

func (e *EventStore) table() string {
	if e.Engine == "sqlserver" {
		return "dbo.events"
	}
	return "events"
}

// Append inserts each event in its own source transaction.
func (e *EventStore) Append(t *testing.T, events ...Event) {
	t.Helper()
	for _, ev := range events {
		require.NoError(t, e.src.exec(context.Background(), e.insertSQL(), ev.Stream, ev.Version, ev.Type, ev.Data), "append %+v", ev)
	}
}

func (e *EventStore) insertSQL() string {
	return fmt.Sprintf("INSERT INTO %s (stream_id, version, type, data) VALUES (%s, %s, %s, %s)",
		e.table(), e.src.ph(1), e.src.ph(2), e.src.ph(3), e.src.ph(4))
}

// SetData rewrites one event's payload in place (a correction or a mask).
func (e *EventStore) SetData(t *testing.T, stream string, version int, data string) {
	t.Helper()
	require.NoError(t, e.src.exec(context.Background(),
		fmt.Sprintf("UPDATE %s SET data = %s WHERE stream_id = %s AND version = %s", e.table(), e.src.ph(1), e.src.ph(2), e.src.ph(3)),
		data, stream, version))
}

// DeleteEvent deletes one event.
func (e *EventStore) DeleteEvent(t *testing.T, stream string, version int) {
	t.Helper()
	require.NoError(t, e.src.exec(context.Background(),
		fmt.Sprintf("DELETE FROM %s WHERE stream_id = %s AND version = %s", e.table(), e.src.ph(1), e.src.ph(2)),
		stream, version))
}

// DeleteStream deletes every event of a stream in one transaction.
func (e *EventStore) DeleteStream(t *testing.T, stream string) {
	t.Helper()
	require.NoError(t, e.src.exec(context.Background(),
		fmt.Sprintf("DELETE FROM %s WHERE stream_id = %s", e.table(), e.src.ph(1)), stream))
}

// Compact deletes a stream's events up to and including version upTo and
// inserts snapshot in their place, in one source transaction.
func (e *EventStore) Compact(t *testing.T, stream string, upTo int, snapshot Event) {
	t.Helper()
	require.NoError(t, e.src.txn(context.Background(), func(exec func(string, ...any) error) error {
		if err := exec(fmt.Sprintf("DELETE FROM %s WHERE stream_id = %s AND version <= %s", e.table(), e.src.ph(1), e.src.ph(2)), stream, upTo); err != nil {
			return err
		}
		return exec(e.insertSQL(), snapshot.Stream, snapshot.Version, snapshot.Type, snapshot.Data)
	}))
}

// Rows returns every row of read.<table>, ordered by order_id, rendered as the
// recipe's psql output renders them: "id | customer | status | items | total | version".
func (e *EventStore) Rows(t *testing.T, table string) []string {
	t.Helper()
	rows, err := e.read.Query(context.Background(), fmt.Sprintf(
		`SELECT order_id::text, coalesce(customer, ''), coalesce(status, ''), coalesce(items::text, ''), coalesce(total::text, ''), coalesce(version::text, '')
		   FROM read.%s ORDER BY order_id`, pgx.Identifier{table}.Sanitize()))
	if err != nil {
		return nil // the projection creates the table on its first write
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		cols := make([]string, 6)
		require.NoError(t, rows.Scan(&cols[0], &cols[1], &cols[2], &cols[3], &cols[4], &cols[5]))
		out = append(out, strings.Join(cols, " | "))
	}
	return out
}

// WaitRows waits until read.<table> holds exactly want.
func (e *EventStore) WaitRows(t *testing.T, table string, want []string, timeout time.Duration) {
	t.Helper()
	sort.Strings(want)
	deadline := time.Now().Add(timeout)
	var got []string
	for time.Now().Before(deadline) {
		got = e.Rows(t, table)
		if strings.Join(got, "\n") == strings.Join(want, "\n") {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Equal(t, want, got, "read.%s never reached the expected rows", table)
}

// Post POSTs a TOML config, externalizing the containers' passwords.
func (e *EventStore) Post(t *testing.T, path, body string) {
	t.Helper()
	postConfig(t, path, body)
}

// Delete DELETEs a config resource.
func (e *EventStore) Delete(t *testing.T, path string) {
	t.Helper()
	deleteConfig(t, path)
}

// Do sends a request to the node and returns the status and body.
func (e *EventStore) Do(t *testing.T, method, path, contentType, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, committedURL(path), strings.NewReader(body))
	require.NoError(t, err)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(b)
}

// WaitStreaming waits until the ingestable id is streaming.
func (e *EventStore) WaitStreaming(t *testing.T, id string, timeout time.Duration) {
	t.Helper()
	require.True(t, waitIngestableStreaming(context.Background(), id, timeout), "ingestable %s never reached streaming", id)
}

// Recipe is docs/recipes/event-store-read-models.md's text.
func Recipe(t *testing.T) string {
	t.Helper()
	root, err := projectRoot()
	require.NoError(t, err)
	b, err := os.ReadFile(filepath.Join(root, "docs", "recipes", "event-store-read-models.md"))
	require.NoError(t, err)
	return string(b)
}

var fenceRE = regexp.MustCompile("(?s)```([a-z]*)\n(.*?)```")

// RecipeBlocks returns the recipe's fenced code blocks of the given language,
// in document order.
func RecipeBlocks(t *testing.T, lang string) []string {
	t.Helper()
	var out []string
	for _, m := range fenceRE.FindAllStringSubmatch(Recipe(t), -1) {
		if m[1] == lang {
			out = append(out, m[2])
		}
	}
	return out
}

func recipeEventsDDL(t *testing.T) string {
	t.Helper()
	for _, b := range RecipeBlocks(t, "sql") {
		if strings.Contains(b, "CREATE TABLE events") {
			return b
		}
	}
	t.Fatal("the recipe has no CREATE TABLE events block")
	return ""
}
