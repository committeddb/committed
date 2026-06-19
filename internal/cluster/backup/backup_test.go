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
