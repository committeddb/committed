package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
)

// The PartialBackfill mark must survive progress cloning: a crashed backfill's
// SECOND crash persists checkpoints built from the cloned progress, and a
// dropped flag there would make the third resume run as a full refresh whose
// marker sweeps the sibling rows the backfill never re-emits. The single-crash
// path is covered end-to-end by TestPostgresAddedTableBackfillResumesAcrossRestart;
// this pins the clone step that test cannot reach.
func TestNewSnapshotProgressPreservesPartialBackfill(t *testing.T) {
	seed := &dialectpb.SnapshotProgress{
		LastPkByTable:   map[string]string{"b": "cursor"},
		CompletedTables: []string{"a"},
		PartialBackfill: true,
	}
	got := newSnapshotProgress(seed)
	require.True(t, got.PartialBackfill, "cloning must carry the partial-backfill mark")
	require.Equal(t, []string{"a"}, got.CompletedTables)
	require.Equal(t, "cursor", got.LastPkByTable["b"])
	require.False(t, newSnapshotProgress(nil).PartialBackfill)
}
