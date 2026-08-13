package sqlserver

import (
	"context"
	gosql "database/sql"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// hangDriver simulates the half-open-socket condition: every query blocks
// until its context is done — exactly what a read on a dead-but-unclosed
// connection does. Registered once under a test-only name.
type hangDriver struct{}

func (hangDriver) Open(string) (driver.Conn, error) { return hangConn{}, nil }

type hangConn struct{}

func (hangConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (hangConn) Close() error                        { return nil }
func (hangConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }

func (hangConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func init() { gosql.Register("committed-test-hang", hangDriver{}) }

// TestReadCTState_DeadConnectionBoundedByDeadline pins the poll loop's
// half-open-socket guard: readCTState against a connection that never
// answers (the dead-socket shape) must return within its own deadline —
// NEVER hang on the caller's unbounded worker context. Without the guard
// this test hangs and fails on its watchdog; the field incident's MySQL
// sibling froze an ingest for two days this way.
func TestReadCTState_DeadConnectionBoundedByDeadline(t *testing.T) {
	orig := ctLivenessTimeout
	ctLivenessTimeout = 200 * time.Millisecond
	t.Cleanup(func() { ctLivenessTimeout = orig })

	db, err := gosql.Open("committed-test-hang", "ignored")
	require.NoError(t, err)
	defer db.Close()

	cfg := &sql.Config{Tables: []string{"t"}}

	done := make(chan error, 1)
	go func() {
		// The worker context: unbounded, like the real poll loop's.
		_, _, err := readCTState(context.Background(), db, cfg)
		done <- err
	}()

	select {
	case err := <-done:
		require.Error(t, err, "a dead connection must surface as an error, not success")
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(5 * time.Second):
		t.Fatal("readCTState hung on a dead connection — the liveness deadline is missing (the silent-freeze bug class)")
	}
}
