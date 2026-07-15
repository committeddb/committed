package wal_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// A crash between a full-DB temp write (RestoreSnapshot / compactLocked) and its
// atomic rename leaves a bbolt.db.restore.* / bbolt.db.compact.* file behind in
// metadata/. The restore temp holds a leader-supplied snapshot payload that can
// carry an RTBF-erased key, so it must not linger across a restart. Open sweeps
// them; the live bbolt.db is untouched.
func TestOpen_SweepsOrphanedBoltTempFiles(t *testing.T) {
	dir := t.TempDir()

	// First open creates metadata/bbolt.db, then close it cleanly.
	s, err := wal.Open(dir, nil, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	require.NoError(t, s.Close())

	metadataDir := filepath.Join(dir, "metadata")
	restoreTmp := filepath.Join(metadataDir, "bbolt.db.restore.1700000000000000000")
	compactTmp := filepath.Join(metadataDir, "bbolt.db.compact.1700000000000000001")
	require.NoError(t, os.WriteFile(restoreTmp, []byte("stale snapshot payload"), 0o600))
	require.NoError(t, os.WriteFile(compactTmp, []byte("stale compaction target"), 0o600))

	// Reopen: the strays must be gone, the live db preserved.
	s2, err := wal.Open(dir, nil, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	defer func() { _ = s2.Close() }()

	require.NoFileExists(t, restoreTmp, "orphaned bbolt.db.restore.* must be swept on Open")
	require.NoFileExists(t, compactTmp, "orphaned bbolt.db.compact.* must be swept on Open")
	require.FileExists(t, filepath.Join(metadataDir, "bbolt.db"), "the live bbolt.db must survive the sweep")
}

// The live bbolt.db (and any unrelated file) must never be swept — only the
// .restore./.compact. temp forms, which carry an extra path segment.
func TestOpen_SweepLeavesLiveAndUnrelatedFiles(t *testing.T) {
	dir := t.TempDir()

	s, err := wal.Open(dir, nil, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	require.NoError(t, s.Close())

	metadataDir := filepath.Join(dir, "metadata")
	// A sibling file whose name merely starts with "bbolt.db" but is not a temp.
	unrelated := filepath.Join(metadataDir, "bbolt.db.lock.notes")
	require.NoError(t, os.WriteFile(unrelated, []byte("keep me"), 0o600))

	s2, err := wal.Open(dir, nil, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	defer func() { _ = s2.Close() }()

	require.FileExists(t, filepath.Join(metadataDir, "bbolt.db"))
	require.FileExists(t, unrelated, "only bbolt.db.restore.* / bbolt.db.compact.* are swept")
}
