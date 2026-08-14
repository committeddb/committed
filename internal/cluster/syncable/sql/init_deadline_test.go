package sql

import (
	"context"
	gosql "database/sql"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// hangSinkDriver simulates the field wedge's destination: every operation
// blocks until its context is done — an ALTER waiting on an analyst's table
// lock, a prepare against a stalled server. Exec and Prepare both hang.
type hangSinkDriver struct{}

func (hangSinkDriver) Open(string) (driver.Conn, error) { return hangSinkConn{}, nil }

type hangSinkConn struct{}

func (hangSinkConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (hangSinkConn) Close() error                        { return nil }
func (hangSinkConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }

func (hangSinkConn) ExecContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (hangSinkConn) PrepareContext(ctx context.Context, _ string) (driver.Stmt, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func init() { gosql.Register("committed-test-hang-sink", hangSinkDriver{}) }

// TestInitBoundedOnHungDestination pins the build-path half of the
// apply-liveness fix: a syncable Init whose destination never answers (a
// locked table, a stalled server) must fail within InitTimeout — loudly,
// into the degraded-config path — never stall its caller indefinitely. The
// field incident: an undeadlined EnsureGenerationColumn ALTER waited forever
// on an analyst query's table lock and the stall propagated into the raft
// apply loop, freezing appliedIndex cluster-wide.
func TestInitBoundedOnHungDestination(t *testing.T) {
	orig := InitTimeout
	InitTimeout = 200 * time.Millisecond
	t.Cleanup(func() { InitTimeout = orig })

	gdb, err := gosql.Open("committed-test-hang-sink", "ignored")
	require.NoError(t, err)
	defer gdb.Close()

	cfg := &Config{
		Topic:      "t",
		Table:      "wedged",
		Mappings:   []Mapping{{JsonPath: "$.pk", Column: "pk", SQLType: "TEXT"}},
		PrimaryKey: []string{"pk"},
	}
	s := &Syncable{db: gdb, config: cfg, dialect: hangTestDialect{}}

	done := make(chan error, 1)
	go func() { done <- s.Init() }()

	select {
	case err := <-done:
		require.Error(t, err, "a hung destination must fail the build, not hang it")
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(5 * time.Second):
		t.Fatal("Init hung on a dead destination — the InitTimeout deadline is missing (the apply-freeze field incident)")
	}
}

// hangTestDialect is the minimal Dialect for the deadline test: real SQL text
// (never executed — the driver hangs), no-op ensures that respect ctx. The
// embedded nil Dialect panics on anything Init doesn't touch — a change to
// Init's operation set fails this test loudly rather than silently passing.
type hangTestDialect struct{ Dialect }

func (hangTestDialect) CreateDDL(c *Config) string { return "CREATE TABLE x (pk TEXT)" }
func (hangTestDialect) DropDDL(c *Config) string   { return "DROP TABLE IF EXISTS x" }
func (hangTestDialect) CreateSQL(c *Config) string { return "INSERT INTO x (pk) VALUES (?)" }
func (hangTestDialect) CreateGenerationUpsertSQL(c *Config) string {
	return "INSERT INTO x (pk, g) VALUES (?, ?)"
}

func (hangTestDialect) EnsureGenerationColumn(ctx context.Context, db *gosql.DB, c *Config) error {
	_, err := db.ExecContext(ctx, "ALTER TABLE x ADD COLUMN g BIGINT")
	return err
}

func (hangTestDialect) CreateGenerationSweepSQL(c *Config) string {
	return "DELETE FROM x WHERE g < ?"
}

func (hangTestDialect) CreateEnrichedUpsertSQL(c *Config, e map[string]SpineEnrichment) string {
	return "INSERT INTO x (pk) VALUES (?)"
}

func (hangTestDialect) CreateSpineFanOutSQL(c *Config, col, on string) string {
	return "UPDATE x SET a = ? WHERE b = ?"
}

func (hangTestDialect) EnsureSpineIndex(ctx context.Context, db *gosql.DB, c *Config, on string) error {
	return nil
}
