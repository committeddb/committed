package segmentlog

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func inspectionFiles(t *testing.T, path string) map[string][]byte {
	t.Helper()
	entries, err := os.ReadDir(path)
	require.NoError(t, err)
	files := make(map[string][]byte, len(entries))
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(path, entry.Name()))
		require.NoError(t, err)
		files[entry.Name()] = data
	}
	return files
}

func TestInspectDirectoryReadOnly(t *testing.T) {
	for _, damage := range []string{"none", "incomplete-tail", "corrupt-tail", "missing-segment", "corrupt-segment", "corrupt-catalog"} {
		t.Run(damage, func(t *testing.T) {
			l := newLog(t, 128)
			for i := uint64(0); i < 12; i++ {
				require.NoError(t, l.Append([]Record{{ID: i, Payload: []byte("record payload")}}))
			}
			// Publish indexed compressed segments and erased records as well as a
			// rewritten tail checkpoint; inspect only the selected generation.
			_, err := l.Rewrite(t.Context(), 7, func(r Record) ([]byte, bool, error) {
				return r.Payload, r.ID%2 == 0, nil
			})
			require.NoError(t, err)
			c, err := l.InspectCatalog()
			require.NoError(t, err)
			require.NotEmpty(t, c.Segments)
			require.NoError(t, l.Close())
			tailPath := filepath.Join(l.path, c.Active.File)
			switch damage {
			case "incomplete-tail":
				f, err := os.OpenFile(tailPath, os.O_WRONLY|os.O_APPEND, 0)
				require.NoError(t, err)
				_, err = f.Write([]byte{1})
				require.NoError(t, err)
				require.NoError(t, f.Close())
			case "corrupt-tail":
				data, err := os.ReadFile(tailPath)
				require.NoError(t, err)
				data[0] ^= 1
				require.NoError(t, os.WriteFile(tailPath, data, 0o600))
			case "corrupt-segment":
				path := filepath.Join(l.path, c.Segments[0].File)
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				data[len(data)/2] ^= 1
				require.NoError(t, os.WriteFile(path, data, 0o600))
			case "missing-segment":
				require.NoError(t, os.Remove(filepath.Join(l.path, c.Segments[0].File)))
			case "corrupt-catalog":
				require.NoError(t, os.WriteFile(filepath.Join(l.path, boltCatalogName), nil, 0o600))
			}
			// Inspection must also leave unselected files alone.
			require.NoError(t, os.WriteFile(filepath.Join(l.path, "orphan"), []byte("leftover"), 0o600))
			before := inspectionFiles(t, l.path)
			result, err := InspectDirectory(t.Context(), l.path)
			switch damage {
			case "none":
				require.NoError(t, err)
				require.Equal(t, uint64(6), result.Records)
			case "incomplete-tail":
				require.ErrorIs(t, err, ErrIncompleteTail)
				require.Equal(t, uint64(6), result.Records)
			case "corrupt-tail", "corrupt-catalog", "corrupt-segment":
				require.ErrorIs(t, err, ErrCorrupt)
			case "missing-segment":
				require.ErrorIs(t, err, os.ErrNotExist)
			}
			require.Equal(t, before, inspectionFiles(t, l.path))
		})
	}
}

func TestInspectDirectoryOwnershipAndMissingCatalog(t *testing.T) {
	l := newLog(t, 128)
	_, err := InspectDirectory(t.Context(), l.path)
	require.ErrorIs(t, err, ErrLocked)
	require.NoError(t, l.Close())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = InspectDirectory(ctx, l.path)
	require.ErrorIs(t, err, context.Canceled)
	_, err = InspectDirectory(nil, l.path) //nolint:staticcheck // Exercise explicit invalid-context rejection.
	require.ErrorIs(t, err, ErrInvalid)
	path := t.TempDir()
	_, err = InspectDirectory(t.Context(), path)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Empty(t, inspectionFiles(t, path), "inspection must not create an empty catalog")
}
