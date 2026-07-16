package backup_test

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/backup"
)

// writeTree lays out a mock node data directory: the same shape wal.Open
// uses (raft/log, raft/state, events, metadata) with a file in each.
func writeTree(t *testing.T) (string, map[string]string) {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"raft/log/00000000000000000001":   "entry-log-bytes",
		"raft/state/00000000000000000001": "state-log-bytes",
		"events/00000000000000000001":     "event-log-bytes",
		"metadata/bbolt.db":               "bolt-bytes",
	}
	for rel, content := range files {
		full := filepath.Join(dir, filepath.FromSlash(rel))
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o700))
		require.NoError(t, os.WriteFile(full, []byte(content), 0o600))
	}
	return dir, files
}

func TestCreateRestore_RoundTrip(t *testing.T) {
	src, files := writeTree(t)

	var buf bytes.Buffer
	now := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	m, err := backup.Create(&buf, src, 7, now)
	require.NoError(t, err)
	require.Equal(t, backup.FormatVersion, m.FormatVersion)
	require.Equal(t, uint64(7), m.NodeID)
	require.Len(t, m.Files, len(files))

	dst := filepath.Join(t.TempDir(), "restored")
	rm, err := backup.Restore(&buf, dst, now)
	require.NoError(t, err)
	require.Equal(t, m.Files, rm.Files)

	// Every file came back byte-for-byte.
	for rel, content := range files {
		got, err := os.ReadFile(filepath.Join(dst, filepath.FromSlash(rel)))
		require.NoError(t, err, rel)
		require.Equal(t, content, string(got), rel)
	}
	// And the restore marker is present.
	marker, err := os.ReadFile(filepath.Join(dst, "RESTORED.json"))
	require.NoError(t, err)
	require.Contains(t, string(marker), "restoredAt")
}

// TestCreate_FailsClosedOnSymlinkedStore: a store dir symlinked in under the
// data root must FAIL the backup, not be silently skipped. filepath.Walk does not
// descend a symlinked dir, so the real files (and the manifest entries for them)
// would otherwise vanish — a hollow backup that passes restore's completeness
// check.
func TestCreate_FailsClosedOnSymlinkedStore(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "raft", "log"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "raft", "log", "0001"), []byte("x"), 0o600))
	// A real store living elsewhere, linked in under the data dir.
	realStore := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(realStore, "0001"), []byte("events"), 0o600))
	require.NoError(t, os.Symlink(realStore, filepath.Join(dir, "events")))

	var buf bytes.Buffer
	_, err := backup.Create(&buf, dir, 0, time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a regular file")
}

// TestCreate_FailsClosedOnSymlinkedDataRoot: a symlinked data root must FAIL, not
// walk to zero files and report a hollow success.
func TestCreate_FailsClosedOnSymlinkedDataRoot(t *testing.T) {
	real, _ := writeTree(t)
	link := filepath.Join(t.TempDir(), "datalink")
	require.NoError(t, os.Symlink(real, link))

	var buf bytes.Buffer
	_, err := backup.Create(&buf, link, 0, time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "symlink")
}

// TestCreate_RejectsEmptyDataDir: a data dir with no regular files yields no
// archivable content, so Create refuses rather than writing an empty backup that
// would "restore" a hollow node.
func TestCreate_RejectsEmptyDataDir(t *testing.T) {
	var buf bytes.Buffer
	_, err := backup.Create(&buf, t.TempDir(), 0, time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "no regular files")
}

// TestRestore_MidFailureLeavesTargetCleanAndRetryable proves restore atomicity: a
// failure partway through (here a truncated archive caught by the completeness
// check, after some files are already staged) leaves NO target directory behind,
// so a retry is not blocked by a half-restored dir — and a subsequent good restore
// into the same target succeeds.
func TestRestore_MidFailureLeavesTargetCleanAndRetryable(t *testing.T) {
	// Archive whose manifest lists two files but contains only one.
	var bad bytes.Buffer
	tw := tar.NewWriter(&bad)
	m := backup.Manifest{FormatVersion: backup.FormatVersion, Files: []string{"events/0001", "raft/log/0001"}}
	mb, _ := json.MarshalIndent(m, "", "  ")
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: backup.ManifestName, Mode: 0o600, Size: int64(len(mb)), Typeflag: tar.TypeReg}))
	_, _ = tw.Write(mb)
	payload := []byte("present")
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "events/0001", Mode: 0o600, Size: int64(len(payload)), Typeflag: tar.TypeReg}))
	_, _ = tw.Write(payload)
	require.NoError(t, tw.Close())

	parent := t.TempDir()
	target := filepath.Join(parent, "restored")

	_, err := backup.Restore(&bad, target, time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing from the archive")

	// No half-restored target, and no staging leftovers in the parent.
	_, statErr := os.Stat(target)
	require.True(t, os.IsNotExist(statErr), "a mid-restore failure must leave no target dir; got %v", statErr)
	parentEntries, err := os.ReadDir(parent)
	require.NoError(t, err)
	require.Empty(t, parentEntries, "the staging dir must be cleaned up on failure")

	// Retry with a good archive into the same target: the earlier failure left
	// nothing to block it.
	src, files := writeTree(t)
	var good bytes.Buffer
	_, err = backup.Create(&good, src, 0, time.Now())
	require.NoError(t, err)
	_, err = backup.Restore(&good, target, time.Now())
	require.NoError(t, err, "a good restore must succeed after a failed one")
	for rel, content := range files {
		got, rerr := os.ReadFile(filepath.Join(target, filepath.FromSlash(rel)))
		require.NoError(t, rerr, rel)
		require.Equal(t, content, string(got), rel)
	}
}

// TestRestore_RefusesNonEmptyTarget guards against clobbering an existing
// (possibly live) node directory.
func TestRestore_RefusesNonEmptyTarget(t *testing.T) {
	src, _ := writeTree(t)
	var buf bytes.Buffer
	_, err := backup.Create(&buf, src, 0, time.Now())
	require.NoError(t, err)

	dst := t.TempDir() // exists and...
	require.NoError(t, os.WriteFile(filepath.Join(dst, "preexisting"), []byte("x"), 0o600))

	_, err = backup.Restore(&buf, dst, time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "not empty")
}

// TestRestore_RejectsPathTraversal proves the zip-slip guard: a crafted
// archive entry escaping the target is rejected.
func TestRestore_RejectsPathTraversal(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	// A valid-looking manifest...
	m := backup.Manifest{FormatVersion: backup.FormatVersion, Files: []string{"../escape"}}
	mb, _ := json.MarshalIndent(m, "", "  ")
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: backup.ManifestName, Mode: 0o600, Size: int64(len(mb)), Typeflag: tar.TypeReg}))
	_, _ = tw.Write(mb)
	// ...and a malicious entry climbing out of the target.
	payload := []byte("pwned")
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "../escape", Mode: 0o600, Size: int64(len(payload)), Typeflag: tar.TypeReg}))
	_, _ = tw.Write(payload)
	require.NoError(t, tw.Close())

	dst := filepath.Join(t.TempDir(), "restored")
	_, err := backup.Restore(&buf, dst, time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "escapes")
}

// TestRestore_RejectsWrongFormatVersion refuses an archive from an
// incompatible backup format.
func TestRestore_RejectsWrongFormatVersion(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	m := backup.Manifest{FormatVersion: backup.FormatVersion + 1}
	mb, _ := json.MarshalIndent(m, "", "  ")
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: backup.ManifestName, Mode: 0o600, Size: int64(len(mb)), Typeflag: tar.TypeReg}))
	_, _ = tw.Write(mb)
	require.NoError(t, tw.Close())

	_, err := backup.Restore(&buf, filepath.Join(t.TempDir(), "r"), time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported backup format version")
}

// TestRestore_RejectsMissingManifest refuses a tar that isn't a committed
// backup.
func TestRestore_RejectsMissingManifest(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "random.txt", Mode: 0o600, Size: 3, Typeflag: tar.TypeReg}))
	_, _ = tw.Write([]byte("abc"))
	require.NoError(t, tw.Close())

	_, err := backup.Restore(&buf, filepath.Join(t.TempDir(), "r"), time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a committed backup")
}

// TestRestore_DetectsTruncatedArchive: the manifest lists a file the archive
// doesn't actually contain.
func TestRestore_DetectsTruncatedArchive(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	m := backup.Manifest{FormatVersion: backup.FormatVersion, Files: []string{"events/0001"}}
	mb, _ := json.MarshalIndent(m, "", "  ")
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: backup.ManifestName, Mode: 0o600, Size: int64(len(mb)), Typeflag: tar.TypeReg}))
	_, _ = tw.Write(mb)
	// ...but never write events/0001.
	require.NoError(t, tw.Close())

	_, err := backup.Restore(&buf, filepath.Join(t.TempDir(), "r"), time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing from the archive")
}
