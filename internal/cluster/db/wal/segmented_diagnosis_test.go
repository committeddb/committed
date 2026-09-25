package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/pkg/segmentlog"
)

func TestRepairNodeDiagnosesSegmentedEvents(t *testing.T) {
	for _, damage := range []string{"none", "incomplete", "corrupt", "missing-catalog"} {
		t.Run(damage, func(t *testing.T) {
			base := t.TempDir()
			dir := filepath.Join(base, "events")
			require.NoError(t, os.Mkdir(dir, 0o700))
			log, err := segmentlog.CreateLog(dir, 1, segmentlog.LogOptions{SegmentBytes: 128})
			require.NoError(t, err)
			require.NoError(t, log.Append([]segmentlog.Record{{ID: 1, Payload: []byte("one")}, {ID: 5, Payload: []byte("five")}}))
			catalog, err := log.InspectCatalog()
			require.NoError(t, err)
			_, err = DiagnoseLog(dir)
			require.ErrorIs(t, err, segmentlog.ErrLocked)
			require.NoError(t, log.Close())
			tail := filepath.Join(dir, catalog.Active.File)
			raw, err := os.ReadFile(tail)
			require.NoError(t, err)
			status := LogClean
			switch damage {
			case "incomplete":
				raw = append(raw, 1)
				status = LogIncompleteTail
			case "corrupt":
				raw[0] ^= 1
				status = LogCorrupt
			case "missing-catalog":
				require.NoError(t, os.Remove(filepath.Join(dir, "metadata.db")))
				status = LogCorrupt
			}
			require.NoError(t, os.WriteFile(tail, raw, 0o600))
			for _, commit := range []bool{false, true} {
				results, err := RepairNode(base, commit)
				require.NoError(t, err)
				require.Len(t, results, 3)
				require.Equal(t, status, results[2].Status)
				require.False(t, results[2].Repaired)
				if damage == "none" || damage == "incomplete" {
					require.Equal(t, 2, results[2].Records)
				}
				after, err := os.ReadFile(tail)
				require.NoError(t, err)
				require.Equal(t, raw, after)
			}
		})
	}
}
