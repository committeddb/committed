package segmentlog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRestoreSegmentRejectsRetiredRevision(t *testing.T) {
	l := newLog(t, 128)
	for id := uint64(0); id < 12; id++ {
		require.NoError(t, l.Append([]Record{{ID: id, Payload: []byte("private payload")}}))
	}
	catalog, err := l.InspectCatalog()
	require.NoError(t, err)
	ref := catalog.Segments[0]
	data, err := os.ReadFile(filepath.Join(l.path, ref.File))
	require.NoError(t, err)
	require.ErrorIs(t, RestoreSegment(t.Context(), l.path, ref, data, true), ErrLocked)
	_, err = l.Rewrite(t.Context(), 1, func(r Record) ([]byte, bool, error) { return r.Payload, r.ID%2 == 0, nil })
	require.NoError(t, err)
	require.NoError(t, l.Close())
	before := inspectionFiles(t, l.path)
	for _, commit := range []bool{false, true} {
		require.ErrorIs(t, RestoreSegment(t.Context(), l.path, ref, data, commit), ErrCatalogConflict)
		require.Equal(t, before, inspectionFiles(t, l.path))
	}
}
