package sql_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	sql "github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects/testdialects"
)

// nulClassifierDialect wraps the mock dialect with the PG-style NUL
// classifier so the hint path is testable without a real Postgres: every
// exec error counts as the 22021 class.
type nulClassifierDialect struct {
	sql.Dialect
}

func (nulClassifierDialect) IsNulByteViolation(error) bool { return true }

// A NUL-class destination error must name the offending payload FIELD(s) in
// the dead-letter message — the field incident hand-hunted one U+0000 across
// every string column of 14 tables — and must NEVER carry the field values
// (row data; the message becomes a permanent replicated record).
func TestSyncNulByteDeadLetterNamesTheField(t *testing.T) {
	dialect, mock, err := testdialects.NewSQLMockDialect()
	require.NoError(t, err)
	db, err := sql.NewDB(nulClassifierDialect{dialect}, "")
	require.NoError(t, err)
	defer db.Close()
	syncable, insertPrepare, _ := newSimpleSyncable(t, mock, dialect, db)

	mock.ExpectBegin()
	insertPrepare.ExpectExec().WillReturnError(errors.New(`ERROR: invalid byte sequence for encoding "UTF8": 0x00 (SQLSTATE 22021)`))
	mock.ExpectRollback()

	_, err = syncable.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(simpleType, []byte("k1"),
			[]byte(`{"key":"k1","one":"legacy-padding\u0000\u0000-user@example.com"}`)),
	}})
	require.Error(t, err)
	require.ErrorContains(t, err, `"one"`, "the hint must name the NUL-bearing field")
	require.ErrorContains(t, err, "U+0000")
	require.NotContains(t, err.Error(), "legacy-padding", "field VALUES must never appear in the message")
	require.NotContains(t, err.Error(), `"key"`, "clean fields must not be named")
	require.NoError(t, mock.ExpectationsWereMet())
}

// Without the classifier (a dialect that can store NUL — MySQL) or without
// any NUL-bearing field, the error passes through untouched.
func TestSyncNulHintScopedToClassAndEvidence(t *testing.T) {
	t.Run("no classifier: untouched", func(t *testing.T) {
		dialect, mock, err := testdialects.NewSQLMockDialect()
		require.NoError(t, err)
		db, err := sql.NewDB(dialect, "")
		require.NoError(t, err)
		defer db.Close()
		syncable, insertPrepare, _ := newSimpleSyncable(t, mock, dialect, db)

		mock.ExpectBegin()
		insertPrepare.ExpectExec().WillReturnError(errors.New("boom"))
		mock.ExpectRollback()
		_, err = syncable.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(simpleType, []byte("k1"), []byte(`{"key":"k1","name":"has\x00nul"}`)),
		}})
		require.Error(t, err)
		require.NotContains(t, err.Error(), "U+0000")
	})

	t.Run("classifier but clean payload: untouched", func(t *testing.T) {
		dialect, mock, err := testdialects.NewSQLMockDialect()
		require.NoError(t, err)
		db, err := sql.NewDB(nulClassifierDialect{dialect}, "")
		require.NoError(t, err)
		defer db.Close()
		syncable, insertPrepare, _ := newSimpleSyncable(t, mock, dialect, db)

		mock.ExpectBegin()
		insertPrepare.ExpectExec().WillReturnError(errors.New("some 22021-class error with no NUL evidence"))
		mock.ExpectRollback()
		_, err = syncable.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(simpleType, []byte("k1"), []byte(`{"key":"k1","one":"clean"}`)),
		}})
		require.Error(t, err)
		require.NotContains(t, err.Error(), "U+0000",
			"no NUL-bearing field found → no hint, even for the error class")
	})
}
