package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db/datadir"
	"github.com/committeddb/committed/pkg/segmentlog"
)

func TestDecompressNodeRefusesSegmentedBeforeAnyRewrite(t *testing.T) {
	for _, missingCatalog := range []bool{false, true} {
		name := "catalog-present"
		if missingCatalog {
			name = "catalog-missing"
		}
		t.Run(name, func(t *testing.T) {
			base := t.TempDir()
			events := datadir.EventsDir(base)
			require.NoError(t, os.MkdirAll(events, 0o700))
			log, err := segmentlog.CreateLog(events, 1, segmentlog.LogOptions{})
			require.NoError(t, err)
			require.NoError(t, log.Append([]segmentlog.Record{{ID: 1, Payload: []byte("record")}}))
			require.NoError(t, log.Close())
			if missingCatalog {
				require.NoError(t, os.Remove(filepath.Join(events, "metadata.db")))
			}
			// The native Raft log is visited before events. Its compressed segment
			// must not be rewritten before discovering the incompatible event log.
			raft := datadir.EntryLogDir(base)
			require.NoError(t, os.MkdirAll(raft, 0o700))
			compressed := filepath.Join(raft, "00000000000000000001.zst")
			raw := encodeZstd(encodeRecord([]byte("raft record")))
			require.NoError(t, os.WriteFile(compressed, raw, 0o600))
			before := make(map[string][]byte)
			files, err := os.ReadDir(events)
			require.NoError(t, err)
			for _, file := range files {
				data, err := os.ReadFile(filepath.Join(events, file.Name()))
				require.NoError(t, err)
				before[file.Name()] = data
			}
			counts, err := DecompressNode(base)
			require.ErrorContains(t, err, "segmented storage cannot be made compatible")
			require.Empty(t, counts)
			after, err := os.ReadFile(compressed)
			require.NoError(t, err)
			require.Equal(t, raw, after)
			require.NoFileExists(t, filepath.Join(raft, "00000000000000000001"))
			afterFiles, err := os.ReadDir(events)
			require.NoError(t, err)
			require.Len(t, afterFiles, len(before))
			for name, expected := range before {
				actual, err := os.ReadFile(filepath.Join(events, name))
				require.NoError(t, err)
				require.Equal(t, expected, actual)
			}
		})
	}
}
