package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/db/datadir"
	"github.com/committeddb/committed/pkg/segmentlog"
)

func TestOpenSelectsSegmentedEventLog(t *testing.T) {
	path := t.TempDir()
	option := WithSegmentedEventLog(segmentlog.LogOptions{
		SegmentBytes: 128,
		Encoding:     segmentlog.Options{Compression: segmentlog.ZstdDefault},
		Cache:        segmentlog.CacheOptions{RecentBytes: 1024, HistoricalBytes: 1024},
	})
	for _, reopen := range []bool{false, true} {
		s, err := Open(path, nil, nil, nil, option, WithSafeMode())
		require.NoError(t, err)
		require.NotNil(t, s.eventLog.managed)
		require.Nil(t, s.eventLog.native)
		if !reopen {
			for index := uint64(1); index <= 12; index++ {
				entry := &pb.Entry{Index: proto.Uint64(index), Term: proto.Uint64(3), Type: pb.EntryNormal.Enum()}
				require.NoError(t, s.Save(&pb.HardState{Term: proto.Uint64(3), Commit: proto.Uint64(index)}, []*pb.Entry{entry}, &pb.Snapshot{}))
				require.NoError(t, s.ApplyCommittedBatch([]*pb.Entry{entry}))
			}
		}
		require.Equal(t, uint64(12), s.EventIndex())
		require.Equal(t, uint64(12), s.AppliedIndex())
		cursor := s.eventLog.records()
		record, err := cursor.Seek(12)
		require.NoError(t, err)
		require.Equal(t, uint64(12), record.ID)
		require.NoError(t, cursor.Close())
		require.NoError(t, s.Close())
	}
}

func eventDirectoryBytes(t *testing.T, base string) map[string][]byte {
	t.Helper()
	dir := datadir.EventsDir(base)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	result := make(map[string][]byte, len(entries))
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		require.NoError(t, err)
		result[entry.Name()] = data
	}
	return result
}

func TestOpenRefusesIncompatibleEventFormat(t *testing.T) {
	for _, existing := range []string{"native", "segmented", "missing-catalog"} {
		t.Run(existing, func(t *testing.T) {
			path := t.TempDir()
			segmented := WithSegmentedEventLog(segmentlog.LogOptions{})
			options := []Option{WithSafeMode()}
			if existing != "native" {
				options = append(options, segmented)
			}
			s, err := Open(path, nil, nil, nil, options...)
			require.NoError(t, err)
			require.NoError(t, s.AppendFetchedRecords(fetchedStream(frame(experimentEntry(t, 10, pb.EntryNormal)))))
			require.NoError(t, s.Close())
			if existing == "missing-catalog" {
				require.NoError(t, os.Remove(filepath.Join(datadir.EventsDir(path), "metadata.db")))
			}
			before := eventDirectoryBytes(t, path)
			wrongOptions := []Option{WithSafeMode()}
			if existing == "native" {
				wrongOptions = append(wrongOptions, segmented)
			}
			_, err = Open(path, nil, nil, nil, wrongOptions...)
			require.Error(t, err)
			require.Equal(t, before, eventDirectoryBytes(t, path), "wrong backend must not initialize or rewrite the events directory")
			if existing == "missing-catalog" {
				_, err = Open(path, nil, nil, nil, segmented, WithSafeMode())
				require.Error(t, err)
				require.Equal(t, before, eventDirectoryBytes(t, path))
			} else {
				resumed, err := Open(path, nil, nil, nil, options...)
				require.NoError(t, err, "failed open must release locks")
				require.Equal(t, uint64(10), resumed.EventIndex())
				require.NoError(t, resumed.Close())
			}
		})
	}
}
