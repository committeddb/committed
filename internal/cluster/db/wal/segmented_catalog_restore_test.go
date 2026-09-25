package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/backup"
	"github.com/committeddb/committed/internal/cluster/db/datadir"
)

func TestSegmentedCatalogDamageRequiresCompleteRestore(t *testing.T) {
	for _, damage := range []string{"missing", "corrupt"} {
		t.Run(damage, func(t *testing.T) {
			base := t.TempDir()
			opener := storageTestOpeners()["segmented"]
			source, err := openStorage(base, nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = source.Close() })
			apply := func(id uint64) {
				entry := &pb.Entry{Index: proto.Uint64(id), Term: proto.Uint64(3), Type: pb.EntryNormal.Enum()}
				require.NoError(t, source.Save(&pb.HardState{Term: proto.Uint64(3), Commit: proto.Uint64(id)}, []*pb.Entry{entry}, &pb.Snapshot{}))
				require.NoError(t, source.ApplyCommittedBatch([]*pb.Entry{entry}))
			}
			for id := uint64(1); id <= 12; id++ {
				apply(id)
			}
			var archive bytes.Buffer
			_, err = backup.CreateLive(&archive, source, 1, time.Now())
			require.NoError(t, err)
			// The backup describes an earlier complete state. Its catalog must not
			// be combined with the damaged directory's newer application metadata.
			apply(13)
			require.NoError(t, source.Close())
			catalog := filepath.Join(datadir.EventsDir(base), "metadata.db")
			if damage == "missing" {
				require.NoError(t, os.Remove(catalog))
			} else {
				require.NoError(t, os.WriteFile(catalog, nil, 0o600))
			}
			for _, commit := range []bool{false, true} {
				reports, err := SpliceNode(base, bytes.NewReader(archive.Bytes()), commit)
				require.NoError(t, err)
				require.Equal(t, LogCorrupt, reports[2].Before.Status)
				require.Contains(t, reports[2].Refused, "restore a complete backup")
				require.False(t, reports[2].Applied)
				if damage == "missing" {
					require.NoFileExists(t, catalog)
				} else {
					data, err := os.ReadFile(catalog)
					require.NoError(t, err)
					require.Empty(t, data)
				}
			}
			target := filepath.Join(t.TempDir(), "restored")
			_, err = backup.Restore(bytes.NewReader(archive.Bytes()), target, time.Now())
			require.NoError(t, err)
			diagnosis, err := DiagnoseLog(datadir.EventsDir(target))
			require.NoError(t, err)
			require.Equal(t, LogClean, diagnosis.Status)
			require.Equal(t, 12, diagnosis.Records)
			resumed, err := openStorage(target, nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			defer func() { _ = resumed.Close() }()
			require.Equal(t, uint64(12), resumed.EventIndex())
			require.Equal(t, uint64(12), resumed.AppliedIndex())
			hardState, _, err := resumed.InitialState()
			require.NoError(t, err)
			require.Equal(t, uint64(12), hardState.GetCommit())
		})
	}
}
