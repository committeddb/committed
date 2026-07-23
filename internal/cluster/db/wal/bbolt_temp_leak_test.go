package wal_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// plantDirAt creates an empty directory at path. os.WriteFile and bolt.Open both
// fail with EISDIR when their target is a directory — for every uid, unlike a
// read-only file whose perms root ignores — so planting a dir at the swap temp
// path is a portable stand-in for the ENOSPC "create/first-write fails with the
// artifact already present" case the fix must clean up.
func plantDirAt(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.Mkdir(path, 0o755))
}

// TestCompact_RemovesTempOnOpenFailure pins #7: compactLocked's
// bolt.Open(tmpPath) is the first write of the compaction temp and the branch
// that fails first on a full disk. bbolt does not unlink on that failure, so
// without an explicit os.Remove the stray leaks — and runOwedCompaction re-drives
// it every scrub signal under sustained ENOSPC, orphaning a fresh (unique-named)
// file each time until the next Open sweeps. Force the open to fail by planting a
// directory at the compact temp path and assert the stray is removed.
func TestCompact_RemovesTempOnOpenFailure(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	s.RegisterType(t, "user-events", 1, 1)
	key := []byte("alice")
	// Upsert @2 + delete @3 so a scrub at bound 3 prunes a tombstone and drives
	// markScrubComplete into compactLocked (same path F1 exercises).
	saveEntity(t, &cluster.Entity{Type: &cluster.Type{ID: "user-events"}, Key: key, Data: []byte(`{"a":1}`)}, s, 1, 2)
	saveEntity(t, cluster.NewDeleteEntity(&cluster.Type{ID: "user-events"}, key), s, 1, 3)

	planted := filepath.Join(t.TempDir(), "compact-planted")
	plantDirAt(t, planted)
	s.SetBoltTmpPathForTest(func(string) string { return planted })

	require.Error(t, s.RunScrubForTest(3), "the forced compaction-open failure must surface")
	require.NoDirExists(t, planted,
		"compactLocked must remove the compaction temp when bolt.Open fails, not leak it (#7)")
}

// TestRestoreSnapshot_RemovesTempOnWriteFailure pins #9: swapBboltToSnapshotData
// writes the leader's full serialized bbolt to a sibling temp before the atomic
// rename. On a mid-write failure (ENOSPC) os.WriteFile leaves a partial file, and
// without an explicit os.Remove that (potentially many-MB, RTBF-bearing) stray
// leaks — one per failed install attempt on a follower stuck under disk pressure.
// Force the write to fail by planting a directory at the restore temp path and
// assert the stray is removed.
func TestRestoreSnapshot_RemovesTempOnWriteFailure(t *testing.T) {
	p := parser.New()
	opts := []wal.Option{wal.WithoutFsync()}

	// Source: apply a type so bbolt has content, then snapshot it.
	src, err := wal.Open(t.TempDir(), p, nil, nil, opts...)
	require.NoError(t, err)
	typeEnt, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "t", Name: "t", Version: 1})
	require.NoError(t, err)
	saveEntity(t, typeEnt, src, 1, 1)
	snap, err := src.CreateSnapshot(src.AppliedIndex(), &pb.ConfState{})
	require.NoError(t, err)
	require.NoError(t, src.Close())

	// Destination: advance its event log to >= the snapshot index so
	// RestoreSnapshot's invariant (snap.Index <= EventIndex) holds and the swap
	// (not the early invariant bail) is reached.
	dst, err := wal.Open(t.TempDir(), p, nil, nil, opts...)
	require.NoError(t, err)
	defer dst.Close()
	typeEnt2, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "t2", Name: "t2", Version: 1})
	require.NoError(t, err)
	saveEntity(t, typeEnt2, dst, 1, 1)

	planted := filepath.Join(t.TempDir(), "restore-planted")
	plantDirAt(t, planted)
	dst.SetBoltTmpPathForTest(func(string) string { return planted })

	require.Error(t, dst.RestoreSnapshot(snap), "the forced temp-write failure must surface")
	require.NoDirExists(t, planted,
		"RestoreSnapshot must remove the restore temp when the temp write fails, not leak it (#9)")
}
