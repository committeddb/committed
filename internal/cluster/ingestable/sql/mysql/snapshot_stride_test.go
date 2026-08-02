package mysql

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
)

// TestHandOffSnapshotWindow_CheckpointStride is the MySQL mirror of the
// Postgres stride test: within one read window, an inline resume checkpoint
// (Proposal.Position) rides every stride-th row AND the final row, each carrying
// THAT row's key as the cursor — so a freeze mid-window resumes from the last
// committed checkpoint instead of re-proposing the whole window. Only the
// checkpoint encoding differs from Postgres.
func TestHandOffSnapshotWindow_CheckpointStride(t *testing.T) {
	old := sql.SnapshotCheckpointStride
	sql.SnapshotCheckpointStride = 10
	defer func() { sql.SnapshotCheckpointStride = old }()

	const n = 25
	rows := make([]*cluster.Entity, n)
	for i := range rows {
		rows[i] = &cluster.Entity{
			Type: &cluster.Type{ID: "t"},
			Key:  []byte(fmt.Sprintf("k%02d", i)),
			Data: []byte(fmt.Sprintf(`{"i":%d}`, i)),
		}
	}
	progress := &dialectpb.SnapshotProgress{LastPkByTable: map[string]string{}}
	pr := make(chan *cluster.Proposal, n)

	pos := mysql.Position{Name: "binlog.000004", Pos: 1547}
	require.NoError(t, handOffSnapshotWindow(
		context.Background(), rows, "orders", progress, pos, "gtid-set", 3, pr))
	close(pr)

	var got []*cluster.Proposal
	for p := range pr {
		got = append(got, p)
	}
	require.Len(t, got, n, "one proposal per row")

	wantCheckpoints := map[int]bool{9: true, 19: true, 24: true}
	for i, p := range got {
		require.Len(t, p.Entities, 1, "single-row proposals")
		require.Equal(t, rows[i].Key, p.Entities[0].Key, "row order preserved")
		require.Equal(t, rows[i].Data, p.Entities[0].Data, "payload untouched")

		if !wantCheckpoints[i] {
			require.Emptyf(t, p.Position, "row %d must be bare (pipelined)", i)
			continue
		}
		require.NotEmptyf(t, p.Position, "row %d must carry the inline checkpoint", i)
		pp := &dialectpb.MySQLBinLogPosition{}
		require.NoError(t, proto.Unmarshal(p.Position, pp))
		require.Equal(t, "binlog.000004", pp.Name)
		require.Equal(t, uint32(1547), pp.Pos)
		require.Equal(t, "gtid-set", pp.GtidSet)
		require.Equal(t, uint64(3), pp.RefreshEpoch)
		require.Equal(t, string(rows[i].Key), pp.SnapshotProgress.LastPkByTable["orders"],
			"row %d checkpoint cursor must be THAT row's key, not the window's last", i)
	}

	require.Equal(t, "k24", progress.LastPkByTable["orders"])
}
