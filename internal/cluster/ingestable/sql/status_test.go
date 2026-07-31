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

	// Mid-snapshot: region finished, nation in progress, supplier untouched. The
	// keyset cursor (PK) is deliberately not surfaced — it is often source PII.
	progress := &dialectpb.SnapshotProgress{
		CompletedTables: []string{"region"},
		LastPkByTable:   map[string]string{"nation": "42"},
	}
	got := sql.SnapshotTableStatus(cfg, progress)
	require.Equal(t, []cluster.TableSnapshotStatus{
		{Table: "region", Complete: true},
		{Table: "nation"},
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

// TestSnapshotTableStatus_TagsTopic tags each table with the topic it feeds, so a
// multi-topic ingestable's per-topic snapshot progress is readable off the flat
// table list.
func TestSnapshotTableStatus_TagsTopic(t *testing.T) {
	cfg := &sql.Config{
		Topics: []sql.TopicSpec{
			{Type: &cluster.Type{ID: "orders"}, Tables: []string{"orders_us", "orders_eu"}},
			{Type: &cluster.Type{ID: "customers"}, Tables: []string{"customers"}},
		},
		Tables: []string{"orders_us", "orders_eu", "customers"},
	}
	require.Equal(t, []cluster.TableSnapshotStatus{
		{Table: "orders_us", Topic: "orders", Complete: true},
		{Table: "orders_eu", Topic: "orders", Complete: true},
		{Table: "customers", Topic: "customers", Complete: true},
	}, sql.SnapshotTableStatus(cfg, nil))
}
