package db_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// newScrubDB is newWalDB with extra options — the scrub cadence is resolved
// at construction, so the option must be present then.
func newScrubDB(t *testing.T, opts ...db.Option) *db.DB {
	t.Helper()
	p := parser.New()
	s, err := wal.Open(t.TempDir(), p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	opts = append([]db.Option{db.WithTickInterval(testTickInterval)}, opts...)
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, nil, nil, opts...)
	t.Cleanup(func() { _ = d.Close(); _ = s.Close() })
	return d
}

// The automatic RTBF scrub cadence is an operator switch: a zero interval
// DISABLES the scheduler (the manual POST /v1/scrub lever still works), and
// an unset one takes the default. Pinned because the disable is on the
// erasure path and was unreachable from the environment until 0.8.0 — the
// option honored zero all along, the env parser refused to pass it through.
func TestScrubInterval_ZeroDisablesTheScheduler(t *testing.T) {
	t.Run("zero disables", func(t *testing.T) {
		d := newScrubDB(t, db.WithScrubInterval(0))
		require.Zero(t, d.ScrubIntervalForTest(),
			"a zero interval must reach the engine, not be replaced by the default")
	})
	t.Run("unset takes the default", func(t *testing.T) {
		d := newScrubDB(t)
		require.Equal(t, db.DefaultScrubInterval, d.ScrubIntervalForTest())
	})
	t.Run("positive passes through", func(t *testing.T) {
		d := newScrubDB(t, db.WithScrubInterval(30*time.Minute))
		require.Equal(t, 30*time.Minute, d.ScrubIntervalForTest())
	})
}
