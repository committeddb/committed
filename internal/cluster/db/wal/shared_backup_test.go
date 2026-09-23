package wal

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/backup"
)

func TestBackupRestoreBackends(t *testing.T) {
	for name := range storageTestOpeners() {
		for _, live := range []bool{true, false} {
			mode := "offline"
			if live {
				mode = "live"
			}
			t.Run(name+"/"+mode, func(t *testing.T) {
				opener := storageTestOpeners()[name]
				source := t.TempDir()
				s, err := openStorage(source, nil, nil, nil, opener, WithSafeMode())
				require.NoError(t, err)
				t.Cleanup(func() { _ = s.Close() })
				entries := make([]*pb.Entry, 0, 20)
				for id := uint64(1); id <= 20; id++ {
					entry := new(pb.Entry)
					require.NoError(t, proto.Unmarshal(experimentEntry(t, id, pb.EntryNormal, experimentRow("key", "value")), entry))
					entries = append(entries, entry)
				}
				for _, entry := range entries {
					require.NoError(t, s.Save(&pb.HardState{Term: proto.Uint64(3), Commit: proto.Uint64(entry.GetIndex())}, []*pb.Entry{entry}, nil))
				}
				require.NoError(t, s.appendEvents(entries))
				require.NoError(t, s.saveAppliedIndex(10))
				var archive bytes.Buffer
				if live {
					_, err = backup.CreateLive(&archive, s, 1, time.Now())
				} else {
					require.NoError(t, s.Close())
					_, err = backup.Create(&archive, source, 1, time.Now())
				}
				require.NoError(t, err)
				target := filepath.Join(t.TempDir(), "restored")
				_, err = backup.Restore(&archive, target, time.Now())
				require.NoError(t, err)
				restored, err := openStorage(target, nil, nil, nil, opener, WithSafeMode())
				require.NoError(t, err)
				defer func() { _ = restored.Close() }()
				require.Equal(t, uint64(20), restored.EventIndex())
				require.Equal(t, uint64(10), restored.AppliedIndex())
				last, err := restored.LastIndex()
				require.NoError(t, err)
				require.Equal(t, uint64(20), last)
				hs, _, err := restored.InitialState()
				require.NoError(t, err)
				require.Equal(t, uint64(20), hs.GetCommit())
				cursor := restored.eventLog.entries.NewEntryCursor(1)
				defer func() { _ = cursor.Close() }()
				for _, entry := range entries {
					actual, err := cursor.Current()
					require.NoError(t, err)
					require.True(t, proto.Equal(entry, actual))
					cursor.Advance()
				}

				require.NoError(t, restored.appendEvents([]*pb.Entry{compressionTestEntry(21)}))
				require.Equal(t, uint64(21), restored.EventIndex())
			})
		}
	}
}
