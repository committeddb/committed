package cmd

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/committeddb/committed/internal/cluster/db/datadir"
)

// makeNodeBolt creates a minimal node metadata/bbolt.db and returns the data
// dir. (The stopped-node lock's own tests live with the lock in datadir.)
func makeNodeBolt(t *testing.T) string {
	t.Helper()
	dataDir := t.TempDir()
	require.NoError(t, os.MkdirAll(datadir.MetadataDir(dataDir), 0o700))
	db, err := bolt.Open(datadir.BoltPath(datadir.MetadataDir(dataDir)), 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	require.NoError(t, db.Close())
	return dataDir
}

// makeCompleteNode extends makeNodeBolt into a full, backup-able node directory:
// the four canonical subtrees backup.Create requires (event log, raft entry log,
// raft state log, metadata/bbolt.db).
func makeCompleteNode(t *testing.T) string {
	t.Helper()
	dataDir := makeNodeBolt(t)
	for _, dir := range []string{
		datadir.EventsDir(dataDir),
		datadir.EntryLogDir(dataDir),
		datadir.StateLogDir(dataDir),
	} {
		require.NoError(t, os.MkdirAll(dir, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "00000000000000000001"), []byte("x"), 0o600))
	}
	return dataDir
}

// TestRunBackup_ArchiveIsOwnerOnly: a backup archive holds the node's entire
// state (event log + metadata, including any PII), so the written file must be
// owner-only — never world-readable via os.Create's 0666&~umask default.
func TestRunBackup_ArchiveIsOwnerOnly(t *testing.T) {
	dataDir := makeCompleteNode(t)
	out := filepath.Join(t.TempDir(), "backup.tar")

	// runBackup reads package-level flag vars; set and restore them.
	prevData, prevTo, prevID := backupDataDir, backupTo, backupNodeID
	t.Cleanup(func() { backupDataDir, backupTo, backupNodeID = prevData, prevTo, prevID })
	backupDataDir, backupTo, backupNodeID = dataDir, out, 0

	require.NoError(t, runBackup())

	info, err := os.Stat(out)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm(),
		"backup archive must be owner-only; it contains the node's full state including PII")
	require.Zero(t, info.Mode().Perm()&0o077, "no group or world permission bits")
}
