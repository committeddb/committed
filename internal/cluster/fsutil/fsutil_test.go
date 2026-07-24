package fsutil_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/fsutil"
)

// TestSyncFile: fsyncs an existing file, and — the property that matters for a
// crash-durability primitive — reports a failure rather than silently succeeding,
// so callers abort instead of proceeding on a non-durable write.
func TestSyncFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	require.NoError(t, os.WriteFile(p, []byte("data"), 0o600))

	require.NoError(t, fsutil.SyncFile(p), "fsync an existing file")
	require.Error(t, fsutil.SyncFile(filepath.Join(dir, "missing")),
		"a missing file must error, not be silently swallowed")
}

// TestSyncDir: fsyncs an existing directory and errors on a missing one.
func TestSyncDir(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, fsutil.SyncDir(dir), "fsync an existing directory")
	require.Error(t, fsutil.SyncDir(filepath.Join(dir, "missing")),
		"a missing directory must error")
}
