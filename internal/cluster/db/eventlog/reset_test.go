package eventlog_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

func TestResetClearsHistoryAndInvalidatesCursors(t *testing.T) {
	for _, backend := range backends() {
		t.Run(backend.name, func(t *testing.T) {
			path := t.TempDir()
			log, err := backend.create(path)
			require.NoError(t, err)
			t.Cleanup(func() { _ = log.Close() })
			require.NoError(t, log.Append([]eventlog.Record{{ID: 10, Payload: bytes.Repeat([]byte("ten"), 80)}, {ID: 20, Payload: bytes.Repeat([]byte("twenty"), 80)}, {ID: 30, Payload: bytes.Repeat([]byte("thirty"), 80)}}))
			_, err = log.Rewrite(t.Context(), 7, func(r eventlog.Record) ([]byte, bool, error) { return r.Payload, true, nil })
			require.NoError(t, err)
			cursor := log.NewCursor()
			defer func() { _ = cursor.Close() }()
			record, err := cursor.Seek(19)
			require.NoError(t, err)
			require.Equal(t, uint64(20), record.ID)
			require.NoError(t, log.Reset())
			_, has, err := log.LastAppended()
			require.NoError(t, err)
			require.False(t, has)
			generation, err := log.Generation()
			require.NoError(t, err)
			require.Equal(t, uint64(7), generation)
			require.NoError(t, log.Append([]eventlog.Record{{ID: 5, Payload: []byte("five")}, {ID: 10, Payload: []byte("new ten")}, {ID: 30, Payload: []byte("new thirty")}}))
			record, err = cursor.Seek(19)
			require.NoError(t, err)
			require.Equal(t, eventlog.Record{ID: 30, Payload: []byte("new thirty")}, record)
			// Erasure preserves progress, but reset must clear even that progress.
			_, err = log.Rewrite(t.Context(), 8, func(eventlog.Record) ([]byte, bool, error) { return nil, false, nil })
			require.NoError(t, err)
			require.NoError(t, log.Reset())
			require.NoError(t, log.Close())
			restored, err := backend.open(path)
			require.NoError(t, err)
			defer func() { _ = restored.Close() }()
			_, has, err = restored.LastAppended()
			require.NoError(t, err)
			require.False(t, has)
			generation, err = restored.Generation()
			require.NoError(t, err)
			require.Equal(t, uint64(8), generation)
			_, err = restored.Read(10)
			require.ErrorIs(t, err, eventlog.ErrNotFound)
			_, err = restored.Reclaim(t.Context())
			require.NoError(t, err)
			require.NoError(t, restored.Append([]eventlog.Record{{ID: 1, Payload: []byte("refetched")}}))
			require.NoError(t, restored.Reset())
			require.NoError(t, restored.Reset(), "empty reset can be retried")
		})
	}
}
