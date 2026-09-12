package wal

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/backup"
)

// A live capture of a running node restores to a node with the same
// events, raft entries, hard state, and applied index — through the same
// Restore the offline archive uses — while the source keeps appending.
func TestLiveBackup_CapturesARestorableNodeWhileItRuns(t *testing.T) {
	src := openFetchPeer(t, t.TempDir(), time.Hour)
	seedEventLog(t, src, 1, 120)
	compressOldestSealed(t, src)
	// The raft log covers every event and then some (as on a node: entries
	// are saved before they are applied); the applied index trails.
	for i := uint64(1); i <= 200; i++ {
		e := &pb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(i), Type: pb.EntryNormal.Enum(), Data: []byte("raft")}
		require.NoError(t, src.Save(&pb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(i)}, []*pb.Entry{e}, nil))
	}
	require.NoError(t, src.saveAppliedIndex(20))

	// Appends land while the capture streams: the archive is a consistent
	// point, not a torn one.
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 121; i <= 200; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_ = src.appendEvents([]*pb.Entry{compressionTestEntry(i)})
			time.Sleep(time.Millisecond)
		}
	}()

	var buf bytes.Buffer
	m, err := backup.CreateLive(&buf, src, 7, time.Now())
	close(stop)
	<-done
	require.NoError(t, err)
	require.True(t, m.Live)
	require.Equal(t, uint64(20), m.AppliedIndex, "the applied index of the captured metadata")
	require.Equal(t, filepath.Dir(src.eventLogDir), m.Source)
	names := make([]string, 0, len(m.Files))
	stateLogFiles := 0
	for _, f := range m.Files {
		names = append(names, f.Path)
		if filepath.Dir(f.Path) == "raft/state" {
			stateLogFiles++
		}
	}
	require.Contains(t, names, "metadata/bbolt.db")
	require.Positive(t, stateLogFiles, "the state log travels (its files are named by their first record, which re-anchoring moves)")
	require.Contains(t, names, "raft/log/00000000000000000001")
	require.Contains(t, names, "events/00000000000000000001.zst", "compressed segments travel as they are")

	target := filepath.Join(t.TempDir(), "restored")
	_, err = backup.Restore(&buf, target, time.Now())
	require.NoError(t, err)
	restored, err := Open(target, nil, nil, nil, WithoutFsync(), WithSealerIdleInterval(time.Hour))
	require.NoError(t, err)
	t.Cleanup(func() { _ = restored.Close() })

	require.GreaterOrEqual(t, restored.EventIndex(), uint64(120), "every event through the capture point")
	for i := 1; i <= 120; i++ {
		entry, err := restored.readEventAt(uint64(i))
		require.NoError(t, err, "event %d", i)
		require.Contains(t, string(entry), fmt.Sprintf(`{"entity_id":%d,`, i))
	}
	require.Equal(t, uint64(20), restored.AppliedIndex())
	last, err := restored.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(200), last, "every raft entry the commit covers")
	hs, _, err := restored.InitialState()
	require.NoError(t, err)
	require.Equal(t, uint64(200), hs.GetCommit())
	require.GreaterOrEqual(t, last, restored.EventIndex(), "the raft log covers every event the archive holds")
}

// A snapshot install between the state-log and entry-log reads would leave
// a commit index the entry log no longer starts under; the capture detects
// it through the entry log's epoch and reports the archive as raced.
func TestLiveBackup_DetectsAnInstallUnderTheRead(t *testing.T) {
	src := openFetchPeer(t, t.TempDir(), time.Hour)
	seedEventLog(t, src, 1, 10)
	e := &pb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(1), Type: pb.EntryNormal.Enum(), Data: []byte("raft")}
	require.NoError(t, src.Save(&pb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(1)}, []*pb.Entry{e}, nil))

	bumped := false
	_, err := src.CaptureBackup(func(name string, size int64, write func(io.Writer) error) error {
		if !bumped && filepath.Dir(name) == "raft/state" {
			src.entryLogEpoch.Add(1) // what resetEntryLogToSnapshot does
			bumped = true
		}
		return write(io.Discard)
	})
	require.ErrorIs(t, err, ErrLiveBackupRaced)
	require.True(t, bumped)
}

// The metadata streams from a spool, not from an open read transaction: a
// receiver that takes its time cannot block bbolt from growing, so this
// node's apply path keeps writing — here a write large enough to force a
// remap, made while the metadata entry's stream is held.
func TestLiveBackup_MetadataStreamDoesNotBlockWrites(t *testing.T) {
	src := openFetchPeer(t, t.TempDir(), time.Hour)
	seedEventLog(t, src, 1, 5)
	for i := uint64(1); i <= 5; i++ {
		e := &pb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(i), Type: pb.EntryNormal.Enum(), Data: []byte("raft")}
		require.NoError(t, src.Save(&pb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(i)}, []*pb.Entry{e}, nil))
	}

	grown := false
	_, err := src.CaptureBackup(func(name string, size int64, write func(io.Writer) error) error {
		if filepath.Base(name) == "bbolt.db" {
			done := make(chan error, 1)
			go func() {
				done <- src.update(func(tx *bolt.Tx) error {
					b, err := tx.CreateBucketIfNotExists([]byte("live-backup-test"))
					if err != nil {
						return err
					}
					return b.Put([]byte("grow"), make([]byte, 4<<20))
				})
			}()
			select {
			case err := <-done:
				require.NoError(t, err)
				grown = true
			case <-time.After(10 * time.Second):
				t.Fatal("a bbolt write that grows the file blocked while the metadata entry streamed")
			}
		}
		return write(io.Discard)
	})
	require.NoError(t, err)
	require.True(t, grown)
}

// A node catching up from a peer holds events its raft log does not cover;
// an archive of it would restore a node that skips its first new entries
// at those indexes as already written. The capture refuses that shape.
func TestLiveBackup_RefusesEventsAheadOfTheRaftLog(t *testing.T) {
	src := openFetchPeer(t, t.TempDir(), time.Hour)
	seedEventLog(t, src, 1, 50) // events through 50, as a fetch would leave them
	e := &pb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(1), Type: pb.EntryNormal.Enum(), Data: []byte("raft")}
	require.NoError(t, src.Save(&pb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(1)}, []*pb.Entry{e}, nil))

	_, err := src.CaptureBackup(func(_ string, _ int64, write func(io.Writer) error) error { return write(io.Discard) })
	require.ErrorIs(t, err, ErrLiveBackupEventsAhead)
}
