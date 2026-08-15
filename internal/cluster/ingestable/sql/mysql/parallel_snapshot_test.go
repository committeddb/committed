package mysql

import (
	"context"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
)

func TestParseSnapshotReaders(t *testing.T) {
	require.Equal(t, 1, parseSnapshotReaders(nil), "missing option = single stream")
	require.Equal(t, 1, parseSnapshotReaders(map[string]string{"snapshot_readers": "bogus"}))
	require.Equal(t, 1, parseSnapshotReaders(map[string]string{"snapshot_readers": "0"}))
	require.Equal(t, 4, parseSnapshotReaders(map[string]string{"snapshot_readers": "4"}))
	require.Equal(t, maxSnapshotReaders, parseSnapshotReaders(map[string]string{"snapshot_readers": "1000"}), "clamped to the source-protecting cap")
}

func TestBatchPredicate(t *testing.T) {
	// No cursor, no upper: full-table start.
	q, args, err := batchPredicate("photos", []string{"id"}, "", false, "", 100)
	require.NoError(t, err)
	require.Equal(t, "FROM `photos` ORDER BY `id` ASC LIMIT 100", q)
	require.Empty(t, args)

	// Cursor only (the pre-chunk single-stream shape).
	q, args, err = batchPredicate("photos", []string{"id"}, "42", true, "", 100)
	require.NoError(t, err)
	require.Equal(t, "FROM `photos` WHERE (`id`) > (?) ORDER BY `id` ASC LIMIT 100", q)
	require.Equal(t, []any{"42"}, args)

	// Chunk start: upper bound only.
	q, args, err = batchPredicate("photos", []string{"id"}, "", false, "500", 100)
	require.NoError(t, err)
	require.Equal(t, "FROM `photos` WHERE (`id`) <= (?) ORDER BY `id` ASC LIMIT 100", q)
	require.Equal(t, []any{"500"}, args)

	// Mid-chunk: cursor AND upper.
	q, args, err = batchPredicate("photos", []string{"id"}, "250", true, "500", 100)
	require.NoError(t, err)
	require.Equal(t, "FROM `photos` WHERE (`id`) > (?) AND (`id`) <= (?) ORDER BY `id` ASC LIMIT 100", q)
	require.Equal(t, []any{"250", "500"}, args)

	// Composite PK: row-value comparisons on both bounds.
	q, args, err = batchPredicate("t", []string{"a", "b"}, `["1","x"]`, true, `["9","z"]`, 10)
	require.NoError(t, err)
	require.Equal(t, "FROM `t` WHERE (`a`, `b`) > (?, ?) AND (`a`, `b`) <= (?, ?) ORDER BY `a` ASC, `b` ASC LIMIT 10", q)
	require.Equal(t, []any{"1", "x", "9", "z"}, args)
}

func TestIntegerSplitPoints(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// 1..1000 in 4 chunks: interior boundaries at 250, 500, 749 (integer division).
	mock.ExpectQuery("SELECT CAST").WillReturnRows(
		sqlmock.NewRows([]string{"min", "max"}).AddRow("1", "1000"))
	bounds, err := integerSplitPoints(context.Background(), db, "t", "id", 4)
	require.NoError(t, err)
	require.Len(t, bounds, 3)
	require.Equal(t, []string{"250", "500", "750"}, bounds[:3])

	// Full unsigned BIGINT domain: no overflow.
	mock.ExpectQuery("SELECT CAST").WillReturnRows(
		sqlmock.NewRows([]string{"min", "max"}).AddRow("0", "18446744073709551615"))
	bounds, err = integerSplitPoints(context.Background(), db, "t", "id", 2)
	require.NoError(t, err)
	require.Equal(t, []string{"9223372036854775807"}, bounds)

	// Negative range (signed keys).
	mock.ExpectQuery("SELECT CAST").WillReturnRows(
		sqlmock.NewRows([]string{"min", "max"}).AddRow("-1000", "1000"))
	bounds, err = integerSplitPoints(context.Background(), db, "t", "id", 2)
	require.NoError(t, err)
	require.Equal(t, []string{"0"}, bounds)

	// Empty table: no plan.
	mock.ExpectQuery("SELECT CAST").WillReturnRows(
		sqlmock.NewRows([]string{"min", "max"}).AddRow(nil, nil))
	bounds, err = integerSplitPoints(context.Background(), db, "t", "id", 4)
	require.NoError(t, err)
	require.Nil(t, bounds)

	// Fewer distinct values than chunks: no plan.
	mock.ExpectQuery("SELECT CAST").WillReturnRows(
		sqlmock.NewRows([]string{"min", "max"}).AddRow("10", "12"))
	bounds, err = integerSplitPoints(context.Background(), db, "t", "id", 4)
	require.NoError(t, err)
	require.Nil(t, bounds)
}

func TestBinarySplitPoints(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	lo := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xaa, 0xbb}
	hi := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x11}
	mock.ExpectQuery("SELECT MIN").WillReturnRows(
		sqlmock.NewRows([]string{"min", "max"}).AddRow(lo, hi))
	bounds, err := binarySplitPoints(context.Background(), db, "t", "id", 2)
	require.NoError(t, err)
	require.Len(t, bounds, 1)
	b := []byte(bounds[0])
	require.Len(t, b, 10, "bounds carry the column width")
	// The midpoint's 8-byte prefix sits strictly between lo and hi bytewise.
	require.Greater(t, string(b), string(lo))
	require.Less(t, string(b), string(hi))

	// Boundaries must round-trip the single-column composite-key encoding
	// (bare value): DecodeCompositeCursor returns the raw bytes for binding.
	vals, err := sql.DecodeCompositeCursor(bounds[0], 1)
	require.NoError(t, err)
	require.Equal(t, string(b), vals[0])
}

// TestHandOffChunkWindow_CursorSafety pins the emitter's core invariant: an
// inline checkpoint rides every stride-th row AND the final row, its encoded
// chunk cursor is exactly THAT row's key, and other chunks' cursors are
// untouched — so a persisted blob never claims rows that weren't handed off
// ahead of it.
func TestHandOffChunkWindow_CursorSafety(t *testing.T) {
	old := sql.SnapshotCheckpointStride
	sql.SnapshotCheckpointStride = 10
	defer func() { sql.SnapshotCheckpointStride = old }()

	const n = 25
	rows := make([]*cluster.Entity, n)
	for i := range rows {
		rows[i] = &cluster.Entity{
			Type: &cluster.Type{ID: "t"},
			Key:  []byte(fmt.Sprintf("k%02d", i)),
			Data: []byte(`{}`),
		}
	}
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
	pos := mysql.Position{Name: "binlog.000004", Pos: 1547}

	require.NoError(t, handOffChunkWindow(context.Background(), rows, 2, plan, progress, pos, "", 3, pr))
	close(pr)

	var checkpoints []struct {
		rowKey string
		cursor string
		other  string
	}
	i := 0
	for p := range pr {
		require.Len(t, p.Entities, 1)
		if p.Position != nil {
			decoded := &dialectpb.MySQLBinLogPosition{}
			require.NoError(t, proto.Unmarshal(p.Position, decoded))
			cp := decoded.SnapshotProgress.ChunksByTable["photos"]
			require.NotNil(t, cp)
			checkpoints = append(checkpoints, struct {
				rowKey string
				cursor string
				other  string
			}{string(p.Entities[0].Key), cp.Chunks[2].LastPk, cp.Chunks[1].LastPk})
			require.Equal(t, uint64(3), decoded.RefreshEpoch)
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
