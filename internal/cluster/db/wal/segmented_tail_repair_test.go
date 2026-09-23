package wal

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/db/datadir"
	"github.com/committeddb/committed/pkg/segmentlog"
)

func TestRepairNodeSegmentedTailDurableBounds(t *testing.T) {
	for _, condition := range []string{"replayable", "applied", "snapshot"} {
		t.Run(condition, func(t *testing.T) {
			base := t.TempDir()
			opener := storageTestOpeners()["segmented"]
			s, err := openStorage(base, nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			var last *pb.Entry
			for index := uint64(1); index <= 3; index++ {
				last = &pb.Entry{Index: proto.Uint64(index), Term: proto.Uint64(3), Type: pb.EntryNormal.Enum()}
				batch := []*pb.Entry{last}
				require.NoError(t, s.Save(&pb.HardState{Term: proto.Uint64(3), Commit: proto.Uint64(index)}, batch, &pb.Snapshot{}))
				if index < 3 || condition != "replayable" {
					require.NoError(t, s.ApplyCommittedBatch(batch))
				} else {
					require.NoError(t, s.appendEvents(batch))
				}
			}
			if condition == "snapshot" {
				snapshot, err := s.CreateSnapshot(3, &pb.ConfState{})
				require.NoError(t, err)
				require.NoError(t, s.Save(&pb.HardState{Term: proto.Uint64(3), Commit: proto.Uint64(3)}, nil, snapshot))
			}
			require.NoError(t, s.Close())
			if condition == "snapshot" {
				// Snapshot persistence precedes metadata installation; model its durable
				// intent while application metadata still reports an older index.
				metadata, err := bolt.Open(datadir.BoltPath(datadir.MetadataDir(base)), 0o600, nil)
				require.NoError(t, err)
				require.NoError(t, metadata.Update(func(tx *bolt.Tx) error {
					var value [8]byte
					binary.BigEndian.PutUint64(value[:], 2)
					return tx.Bucket(appliedIndexBucket).Put(appliedIndexKey, value[:])
				}))
				require.NoError(t, metadata.Close())
			}
			log, err := segmentlog.OpenLog(datadir.EventsDir(base), segmentlog.Options{})
			require.NoError(t, err)
			catalog, err := log.InspectCatalog()
			require.NoError(t, err)
			require.NoError(t, log.Close())
			tail := filepath.Join(datadir.EventsDir(base), catalog.Active.File)
			bytes, err := os.ReadFile(tail)
			require.NoError(t, err)
			broken := bytes[:len(bytes)-1]
			require.NoError(t, os.WriteFile(tail, broken, 0o600))
			for _, commit := range []bool{false, true} {
				result, err := RepairNode(base, commit)
				require.NoError(t, err)
				if condition == "replayable" {
					require.Equal(t, LogTornTail, result[2].Status)
					require.Equal(t, commit, result[2].Repaired)
				} else {
					require.Equal(t, LogIncompleteTail, result[2].Status)
					require.False(t, result[2].Repaired)
				}
				if !commit || condition != "replayable" {
					after, err := os.ReadFile(tail)
					require.NoError(t, err)
					require.Equal(t, broken, after)
				}
			}
			if condition == "replayable" {
				resumed, err := openStorage(base, nil, nil, nil, opener, WithSafeMode())
				require.NoError(t, err)
				defer func() { _ = resumed.Close() }()
				require.Equal(t, uint64(2), resumed.EventIndex())
				require.NoError(t, resumed.ApplyCommittedBatch([]*pb.Entry{last}))
				require.Equal(t, uint64(3), resumed.EventIndex())
			}
		})
	}
}
