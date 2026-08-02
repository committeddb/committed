package postgres

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
)

// TestHandOffSnapshotWindow_CheckpointStride pins the checkpoint stride: within
// one read window, an inline resume checkpoint (Proposal.Position) rides every
// stride-th row AND the window's final row, each carrying THAT row's key as the
// cursor — not the window's last. Before the stride, only the final row carried
// a checkpoint, making the window an all-or-nothing durability cliff: a freeze
// mid-window restarted from scratch, re-proposing committed SourceSeq-0 rows
// nothing dedups, and the supervisor counted the never-advancing position toward
// give-up. Red proof: reverting to final-row-only fails the stride rows here.
func TestHandOffSnapshotWindow_CheckpointStride(t *testing.T) {
	old := sql.SnapshotCheckpointStride
	sql.SnapshotCheckpointStride = 10
	defer func() { sql.SnapshotCheckpointStride = old }()

	const n = 25
	entities := make([]*cluster.Entity, n)
	for i := range entities {
		entities[i] = &cluster.Entity{
			Type: &cluster.Type{ID: "t"},
			Key:  []byte(fmt.Sprintf("k%02d", i)),
			Data: []byte(fmt.Sprintf(`{"i":%d}`, i)),
		}
	}
	progress := &dialectpb.SnapshotProgress{LastPkByTable: map[string]string{}}
	pr := make(chan *cluster.Proposal, n)

	require.NoError(t, handOffSnapshotWindow(
		context.Background(), entities, "orders", progress, pglogrepl.LSN(0x1A2B), 3, pr))
	close(pr)

	var got []*cluster.Proposal
	for p := range pr {
		got = append(got, p)
	}
	require.Len(t, got, n, "one proposal per row")

	// Stride 10 over 25 rows: checkpoints on rows 9, 19 (stride) and 24 (final).
	wantCheckpoints := map[int]bool{9: true, 19: true, 24: true}
	for i, p := range got {
		require.Len(t, p.Entities, 1, "single-row proposals")
		require.Equal(t, entities[i].Key, p.Entities[0].Key, "row order preserved")
		require.Equal(t, entities[i].Data, p.Entities[0].Data, "payload untouched")

		if !wantCheckpoints[i] {
			require.Emptyf(t, p.Position, "row %d must be bare (pipelined)", i)
			continue
		}
		require.NotEmptyf(t, p.Position, "row %d must carry the inline checkpoint", i)
		lsn, prog, epoch, err := decodePosition(p.Position)
		require.NoError(t, err)
		require.Equal(t, pglogrepl.LSN(0x1A2B), lsn)
		require.Equal(t, uint64(3), epoch)
		require.Equal(t, string(entities[i].Key), prog.LastPkByTable["orders"],
			"row %d checkpoint cursor must be THAT row's key, not the window's last", i)
	}

	// The live cursor ends at the window's last key — the caller's read cursor.
	require.Equal(t, "k24", progress.LastPkByTable["orders"])
}

// TestHandOffSnapshotWindow_ShortWindow: a window at or below the stride
// degenerates to exactly the pre-stride behavior — one checkpoint, on the final
// row (byte-compat for small windows, e.g. docker suites with batch_size=10).
func TestHandOffSnapshotWindow_ShortWindow(t *testing.T) {
	old := sql.SnapshotCheckpointStride
	sql.SnapshotCheckpointStride = 10
	defer func() { sql.SnapshotCheckpointStride = old }()

	entities := make([]*cluster.Entity, 4)
	for i := range entities {
		entities[i] = &cluster.Entity{Type: &cluster.Type{ID: "t"}, Key: []byte(fmt.Sprintf("k%d", i))}
	}
	progress := &dialectpb.SnapshotProgress{LastPkByTable: map[string]string{}}
	pr := make(chan *cluster.Proposal, 4)
	require.NoError(t, handOffSnapshotWindow(
		context.Background(), entities, "orders", progress, 0, 1, pr))
	close(pr)

	i := 0
	for p := range pr {
		if i == 3 {
			require.NotEmpty(t, p.Position, "final row carries the checkpoint")
		} else {
			require.Emptyf(t, p.Position, "row %d bare", i)
		}
		i++
	}
}
