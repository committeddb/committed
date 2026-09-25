package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/pkg/segmentlog"
)

func TestSpliceSegmentedImmutableFiles(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		for _, damage := range []string{"missing", "corrupt", "wrong-backup"} {
			name := "closed-tail/" + damage
			if indexed {
				name = "indexed/" + damage
			}
			t.Run(name, func(t *testing.T) {
				base := t.TempDir()
				dir := filepath.Join(base, "events")
				require.NoError(t, os.Mkdir(dir, 0o700))
				log, err := segmentlog.CreateLog(dir, 1, segmentlog.LogOptions{SegmentBytes: 128, Encoding: segmentlog.Options{Compression: segmentlog.ZstdDefault}})
				require.NoError(t, err)
				t.Cleanup(func() { _ = log.Close() })
				for id := uint64(1); id <= 12; id++ {
					require.NoError(t, log.Append([]segmentlog.Record{{ID: id, Payload: []byte("some payload")}}))
				}
				if indexed {
					_, err = log.Rewrite(t.Context(), 7, func(r segmentlog.Record) ([]byte, bool, error) { return r.Payload, r.ID%2 == 0, nil })
					require.NoError(t, err)
				}
				catalog, err := log.InspectCatalog()
				require.NoError(t, err)
				require.Greater(t, len(catalog.Segments), 1)
				require.NoError(t, log.Close())
				ref := catalog.Segments[0]
				target := filepath.Join(dir, ref.File)
				original, err := os.ReadFile(target)
				require.NoError(t, err)
				metadata, err := os.ReadFile(filepath.Join(dir, "metadata.db"))
				require.NoError(t, err)
				if damage == "wrong-backup" {
					wrong, err := os.ReadFile(filepath.Join(dir, catalog.Segments[1].File))
					require.NoError(t, err)
					require.NoError(t, os.WriteFile(target, wrong, 0o600))
				}
				archive := archiveOf(t, base)
				if damage == "missing" {
					require.NoError(t, os.Remove(target))
				} else {
					broken := bytes.Clone(original)
					broken[len(broken)/2] ^= 1
					require.NoError(t, os.WriteFile(target, broken, 0o600))
				}
				before, beforeErr := os.ReadFile(target)
				for _, commit := range []bool{false, true} {
					reports, err := SpliceNode(base, bytes.NewReader(archive), commit)
					require.NoError(t, err)
					report := reports[2]
					require.Equal(t, LogCorrupt, report.Before.Status)
					if damage == "wrong-backup" {
						require.NotEmpty(t, report.Refused)
						require.False(t, report.Applied)
					} else {
						require.Empty(t, report.Refused)
						require.NotEmpty(t, report.Plan)
						require.Equal(t, commit, report.Applied)
						if commit {
							require.Equal(t, LogClean, report.After.Status)
							restored, err := os.ReadFile(target)
							require.NoError(t, err)
							require.Equal(t, original, restored)
						} else if damage == "missing" {
							require.NoFileExists(t, target)
						}
					}
					if beforeErr == nil && (!commit || damage == "wrong-backup") {
						current, err := os.ReadFile(target)
						require.NoError(t, err)
						require.Equal(t, before, current)
					}
					after, err := os.ReadFile(filepath.Join(dir, "metadata.db"))
					require.NoError(t, err)
					require.Equal(t, metadata, after, "repair must not change catalog selection")
				}
			})
		}
	}
}
