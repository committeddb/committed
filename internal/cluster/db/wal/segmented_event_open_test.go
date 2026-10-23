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

// A tidwall event log, or a segmented one missing its catalog, is refused
// without the events directory changing, and the refusal releases the locks.
func TestOpenRefusesIncompatibleEventFormat(t *testing.T) {
	for _, existing := range []string{"tidwall", "missing-catalog"} {
		t.Run(existing, func(t *testing.T) {
			path := t.TempDir()
			s, err := Open(path, nil, nil, nil, WithSafeMode())
			require.NoError(t, err)
			require.NoError(t, s.AppendFetchedRecords(fetchedStream(frame(experimentEntry(t, 10, pb.EntryNormal)))))
			require.NoError(t, s.Close())
			if existing == "tidwall" {
				require.NoError(t, RewriteEventLogAsTidwallForTest(path, 1<<20))
			} else {
				require.NoError(t, os.Remove(filepath.Join(datadir.EventsDir(path), "metadata.db")))
			}
			before := eventDirectoryBytes(t, path)
			_, err = Open(path, nil, nil, nil, WithSafeMode())
			require.Error(t, err)
			if existing == "tidwall" {
				require.ErrorIs(t, err, ErrTidwallEventLog)
			}
			require.Equal(t, before, eventDirectoryBytes(t, path), "a refused open must not initialize or rewrite the events directory")
			_, err = Open(path, nil, nil, nil, WithSafeMode())
			require.Error(t, err, "the refusal is the same on a second open: it released its locks")
		})
	}
}
