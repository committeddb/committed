package mysql

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

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
