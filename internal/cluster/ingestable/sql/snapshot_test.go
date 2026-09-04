package sql

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
)

// encodeProgressForTest is the checkpoint encoder the hand-off tests drive:
// the progress proto itself, so a decoded checkpoint shows exactly the cursor
// state that rode the row. Each dialect's real encoder wraps this progress in
// its own position proto (pinned by that dialect's encode/decode tests).
func encodeProgressForTest(p *dialectpb.SnapshotProgress) ([]byte, error) {
	return proto.Marshal(p)
}

func decodeProgressForTest(t *testing.T, b []byte) *dialectpb.SnapshotProgress {
	t.Helper()
	p := &dialectpb.SnapshotProgress{}
	require.NoError(t, proto.Unmarshal(b, p))
	return p
}

func stressRows(n int) []*cluster.Entity {
	rows := make([]*cluster.Entity, n)
	for i := range rows {
		rows[i] = &cluster.Entity{
			Type: &cluster.Type{ID: "t"},
			Key:  []byte(fmt.Sprintf("k%02d", i)),
			Data: []byte(fmt.Sprintf(`{"i":%d}`, i)),
		}
	}
	return rows
}

// TestHandOffSnapshotWindow_CheckpointStride pins the checkpoint stride: within
// one read window, an inline resume checkpoint (Proposal.Position) rides every
// stride-th row AND the window's final row, each carrying THAT row's key as the
// cursor — not the window's last. Before the stride, only the final row carried
// a checkpoint, making the window an all-or-nothing durability cliff: a freeze
// mid-window restarted from scratch, re-proposing committed SourceSeq-0 rows
// nothing dedups, and the supervisor counted the never-advancing position toward
// give-up. Red proof: reverting to final-row-only fails the stride rows here.
func TestHandOffSnapshotWindow_CheckpointStride(t *testing.T) {
	old := SnapshotCheckpointStride
	SnapshotCheckpointStride = 10
	defer func() { SnapshotCheckpointStride = old }()

	const n = 25
	entities := stressRows(n)
	progress := &dialectpb.SnapshotProgress{LastPkByTable: map[string]string{}}
	pr := make(chan *cluster.Proposal, n)

	require.NoError(t, handOffSnapshotWindow(context.Background(), entities, "orders", progress, encodeProgressForTest, pr))
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
		prog := decodeProgressForTest(t, p.Position)
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
	old := SnapshotCheckpointStride
	SnapshotCheckpointStride = 10
	defer func() { SnapshotCheckpointStride = old }()

	entities := stressRows(4)
	progress := &dialectpb.SnapshotProgress{LastPkByTable: map[string]string{}}
	pr := make(chan *cluster.Proposal, 4)
	require.NoError(t, handOffSnapshotWindow(context.Background(), entities, "orders", progress, encodeProgressForTest, pr))
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

// TestHandOffChunkWindow_CursorSafety pins the chunked emitter's invariant: an
// inline checkpoint rides every stride-th row AND the final row, its encoded
// chunk cursor is exactly THAT row's key, and other chunks' cursors are
// untouched — so a persisted blob never claims rows that weren't handed off
// ahead of it.
func TestHandOffChunkWindow_CursorSafety(t *testing.T) {
	old := SnapshotCheckpointStride
	SnapshotCheckpointStride = 10
	defer func() { SnapshotCheckpointStride = old }()

	const n = 25
	rows := stressRows(n)
	plan := &dialectpb.TableChunkProgress{Chunks: []*dialectpb.ChunkCursor{
		{Upper: "b0"},
		{Lower: "b0", Upper: "b1", LastPk: "other-cursor"},
		{Lower: "b1"},
	}}
	progress := &dialectpb.SnapshotProgress{
		LastPkByTable: map[string]string{},
		ChunksByTable: map[string]*dialectpb.TableChunkProgress{"photos": plan},
	}
	pr := make(chan *cluster.Proposal, n)

	require.NoError(t, handOffChunkWindow(context.Background(), rows, 2, plan, progress, encodeProgressForTest, pr))
	close(pr)

	var checkpoints []struct{ rowKey, cursor, other string }
	i := 0
	for p := range pr {
		require.Len(t, p.Entities, 1)
		if p.Position != nil {
			cp := decodeProgressForTest(t, p.Position).ChunksByTable["photos"]
			require.NotNil(t, cp)
			checkpoints = append(checkpoints, struct{ rowKey, cursor, other string }{
				string(p.Entities[0].Key), cp.Chunks[2].LastPk, cp.Chunks[1].LastPk,
			})
		}
		i++
	}
	require.Equal(t, n, i, "every row handed off")
	require.Len(t, checkpoints, 3, "stride 10 over 25 rows: rows 10, 20, and the final 25th")
	for _, cp := range checkpoints {
		require.Equal(t, cp.rowKey, cp.cursor, "the checkpoint's chunk cursor is the row it rides")
		require.Equal(t, "other-cursor", cp.other, "sibling chunks' cursors are untouched")
	}
	require.Equal(t, "k24", checkpoints[len(checkpoints)-1].cursor, "the window's final row checkpoints")
}

// TestChunkStartCursor pins where a chunk's read begins on fresh start vs
// resume: the durable cursor wins, else the exclusive lower bound, else
// unbounded (the table-start chunk).
func TestChunkStartCursor(t *testing.T) {
	lastPK, have := chunkStartCursor(&dialectpb.ChunkCursor{Lower: "100", Upper: "200", LastPk: "150"})
	require.True(t, have)
	require.Equal(t, "150", lastPK, "a checkpointed chunk resumes from its cursor")

	lastPK, have = chunkStartCursor(&dialectpb.ChunkCursor{Lower: "100", Upper: "200"})
	require.True(t, have)
	require.Equal(t, "100", lastPK, "a fresh chunk starts at its lower bound")

	lastPK, have = chunkStartCursor(&dialectpb.ChunkCursor{Upper: "200"})
	require.False(t, have)
	require.Empty(t, lastPK, "the first chunk starts unbounded below")
}

// The PartialBackfill mark must survive progress cloning: a crashed backfill's
// SECOND crash persists checkpoints built from the cloned progress, and a
// dropped flag there would make the third resume run as a full refresh whose
// marker sweeps the sibling rows the backfill never re-emits. A chunked
// table's frozen plan clones too — it resumes verbatim, never re-planned.
func TestNewSnapshotProgressPreservesPartialBackfillAndPlans(t *testing.T) {
	seed := &dialectpb.SnapshotProgress{
		LastPkByTable:   map[string]string{"b": "cursor"},
		CompletedTables: []string{"a"},
		PartialBackfill: true,
		ChunksByTable:   map[string]*dialectpb.TableChunkProgress{"c": {Chunks: []*dialectpb.ChunkCursor{{Upper: "x"}}}},
	}
	got := NewSnapshotProgress(seed)
	require.True(t, got.PartialBackfill, "cloning must carry the partial-backfill mark")
	require.Equal(t, []string{"a"}, got.CompletedTables)
	require.Equal(t, "cursor", got.LastPkByTable["b"])
	require.Same(t, seed.ChunksByTable["c"], got.ChunksByTable["c"], "the frozen plan resumes verbatim")
	fresh := NewSnapshotProgress(nil)
	require.False(t, fresh.PartialBackfill)
	require.NotNil(t, fresh.LastPkByTable)
	require.NotNil(t, fresh.ChunksByTable)
}

// AddedTables drives the added-table backfill: configured-but-never-
// snapshotted tables, in config order (deterministic scan order). A pure
// diff — a dialect that grandfathers an empty registry applies that first.
func TestAddedTables(t *testing.T) {
	cases := []struct {
		name        string
		configured  []string
		snapshotted []string
		want        []string
	}{
		{"nothing added", []string{"a", "b"}, []string{"a", "b"}, nil},
		{"one added", []string{"a", "b"}, []string{"a"}, []string{"b"}},
		{"added preserves config order", []string{"c", "a", "b"}, []string{"a"}, []string{"c", "b"}},
		{"all new (fresh registry, no position) ", []string{"a"}, nil, []string{"a"}},
		{"registry superset tolerated", []string{"a"}, []string{"a", "gone"}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, AddedTables(tc.configured, tc.snapshotted))
		})
	}
}

func TestParseBatchSize(t *testing.T) {
	const def = 10000
	tests := []struct {
		name string
		opts map[string]string
		want int
	}{
		{"nil_options", nil, def},
		{"missing_key", map[string]string{"other": "v"}, def},
		{"valid", map[string]string{"batch_size": "500"}, 500},
		{"invalid_non_numeric", map[string]string{"batch_size": "xyz"}, def},
		{"zero_falls_back", map[string]string{"batch_size": "0"}, def},
		{"negative_falls_back", map[string]string{"batch_size": "-42"}, def},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, ParseBatchSize(tt.opts, def))
		})
	}
}

func TestParseSnapshotReaders(t *testing.T) {
	require.Equal(t, 1, ParseSnapshotReaders(nil), "missing option = single stream")
	require.Equal(t, 1, ParseSnapshotReaders(map[string]string{"snapshot_readers": "bogus"}))
	require.Equal(t, 1, ParseSnapshotReaders(map[string]string{"snapshot_readers": "0"}))
	require.Equal(t, 4, ParseSnapshotReaders(map[string]string{"snapshot_readers": "4"}))
	require.Equal(t, MaxSnapshotReaders, ParseSnapshotReaders(map[string]string{"snapshot_readers": "1000"}), "clamped to the source-protecting cap")
}
