package sql_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
)

// TestSnapshotTableStatus covers the shared per-table snapshot accounting: a
// table is complete iff it is in CompletedTables, otherwise it carries its
// keyset cursor; and once the snapshot is done (nil progress) every configured
// table reads complete.
func TestSnapshotTableStatus(t *testing.T) {
	cfg := &sql.Config{Tables: []string{"region", "nation", "supplier"}}

	// Mid-snapshot: region finished, nation has a cursor, supplier untouched.
	progress := &dialectpb.SnapshotProgress{
		CompletedTables: []string{"region"},
		LastPkByTable:   map[string]string{"nation": "42"},
	}
	got := sql.SnapshotTableStatus(cfg, progress)
	require.Equal(t, []cluster.TableSnapshotStatus{
		{Table: "region", Complete: true},
		{Table: "nation", LastKey: "42"},
		{Table: "supplier"},
	}, got)

	// Snapshot complete (progress == nil): every table complete, no cursor.
	require.Equal(t, []cluster.TableSnapshotStatus{
		{Table: "region", Complete: true},
		{Table: "nation", Complete: true},
		{Table: "supplier", Complete: true},
	}, sql.SnapshotTableStatus(cfg, nil))

	// No configured tables → empty, non-nil slice.
	require.Empty(t, sql.SnapshotTableStatus(&sql.Config{}, nil))
}
