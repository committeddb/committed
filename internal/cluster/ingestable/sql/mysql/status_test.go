package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
)

// TestMysqlStatus covers MySQL Status entirely offline (it makes no source
// query — lag is a deliberate follow-on): phase from the snapshot-progress
// presence, the binlog coordinate as "file:pos", and Lag always nil (so
// CaughtUp is never true).
func TestMysqlStatus(t *testing.T) {
	d := &MySQLDialect{}
	cfg := &sql.Config{Tables: []string{"region", "nation"}}

	// Streaming: a bare binlog position, no snapshot progress.
	streaming, err := proto.Marshal(&dialectpb.MySQLBinLogPosition{Name: "binlog.000004", Pos: 1547})
	require.NoError(t, err)
	st, err := d.Status(context.Background(), cfg, streaming)
	require.NoError(t, err)
	require.Equal(t, "streaming", st.Phase)
	require.Equal(t, "binlog.000004:1547", st.Position)
	require.Nil(t, st.Lag, "MySQL lag is out of scope")
	require.False(t, st.CaughtUp, "no caught-up without a known lag")
	require.Equal(t, []cluster.TableSnapshotStatus{
		{Table: "region", Complete: true},
		{Table: "nation", Complete: true},
	}, st.SnapshotProgress)

	// Mid-snapshot: progress present → phase=snapshot, per-table cursor.
	snap, err := proto.Marshal(&dialectpb.MySQLBinLogPosition{
		Name: "binlog.000004", Pos: 1547,
		SnapshotProgress: &dialectpb.SnapshotProgress{
			CompletedTables: []string{"region"},
			LastPkByTable:   map[string]string{"nation": "9"},
		},
	})
	require.NoError(t, err)
	st, err = d.Status(context.Background(), cfg, snap)
	require.NoError(t, err)
	require.Equal(t, "snapshot", st.Phase)
	// nation is mid-snapshot (not complete); the keyset PK is redacted (source PII).
	require.Equal(t, "nation", st.SnapshotProgress[1].Table)
	require.False(t, st.SnapshotProgress[1].Complete)

	// Empty position (never checkpointed): streaming, no coordinate.
	st, err = d.Status(context.Background(), cfg, nil)
	require.NoError(t, err)
	require.Equal(t, "streaming", st.Phase)
	require.Empty(t, st.Position)
}
