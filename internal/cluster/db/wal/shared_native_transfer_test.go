package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

func TestSharedBackendsRejectNativeTransfer(t *testing.T) {
	for _, name := range []string{"tidwall", "segmented", "segmented-cached"} {
		t.Run(name, func(t *testing.T) {
			path := t.TempDir()
			opener := storageTestOpeners()[name]
			s, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			entry := &pb.Entry{Index: proto.Uint64(10), Term: proto.Uint64(3), Type: pb.EntryNormal.Enum()}
			require.NoError(t, s.appendEvents([]*pb.Entry{entry}))
			original, err := s.eventLog.managed.Read(10)
			require.NoError(t, err)
			staged := filepath.Join(t.TempDir(), "00000000000000000001")
			stagedBytes := []byte("uninspected native segment")
			require.NoError(t, os.WriteFile(staged, stagedBytes, 0o600))
			calls := []struct {
				name string
				call func() error
			}{
				{"reset", s.ResetEventLog},
				{"set-generation", func() error { return s.SetEventLogGeneration(99) }},
				{"adopt", func() error { return s.AdoptEventSegments([]string{staged}) }},
				{"serve", func() error { _, err := s.ServeEvents(t.Context(), 0, 10, nil); return err }},
				{"sequence", func() error { _, err := s.EventSeqForIndex(10); return err }},
				{"first-sequence", func() error { _, err := s.firstEventSeq(); return err }},
				{"last-sequence", func() error { _, err := s.LastEventSeq(); return err }},
				{"raw-read", func() error { _, err := s.ReadEventRaw(1); return err }},
				{"index-at-sequence", func() error { _, err := s.EventRaftIndexAt(1); return err }},
				{"encode-native-records", func() error { _, _, err := s.encodeRecords(1, 1, 1024); return err }},
				{"layout", func() error {
					release := s.FreezeEventLayout()
					defer release()
					_, err := s.EventLayout()
					return err
				}},
			}
			for _, call := range calls {
				t.Run(call.name, func(t *testing.T) {
					require.ErrorIs(t, call.call(), eventlog.ErrUnsupported)
					record, err := s.eventLog.managed.Read(10)
					require.NoError(t, err, "rejected operation must leave the live backend open")
					require.Equal(t, original, record)
					require.Equal(t, uint64(10), s.EventIndex())
					require.Zero(t, s.EventLogGeneration())
					completed, err := s.loadScrubCompleted()
					require.NoError(t, err)
					require.Zero(t, completed)
				})
			}
			bytes, err := os.ReadFile(staged)
			require.NoError(t, err)
			require.Equal(t, stagedBytes, bytes, "rejected adoption must not consume staged files")
			entry.Index = proto.Uint64(20)
			require.NoError(t, s.appendEvents([]*pb.Entry{entry}), "logical appends still work")
			require.NoError(t, s.Close())
			reopened, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err, "rejected reset must not replace the on-disk format")
			t.Cleanup(func() { _ = reopened.Close() })
			require.Equal(t, uint64(20), reopened.EventIndex())
			record, err := reopened.eventLog.managed.Read(10)
			require.NoError(t, err)
			require.Equal(t, original, record)
		})
	}
}
