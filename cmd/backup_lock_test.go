package cmd

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"

	"github.com/committeddb/committed/internal/cluster/db/datadir"
)

// makeNodeBolt creates a minimal node metadata/bbolt.db so lockStoppedNode has a
// file to lock, and returns the data dir.
func makeNodeBolt(t *testing.T) string {
	t.Helper()
	dataDir := t.TempDir()
	require.NoError(t, os.MkdirAll(datadir.MetadataDir(dataDir), 0o700))
	db, err := bolt.Open(datadir.BoltPath(datadir.MetadataDir(dataDir)), 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	require.NoError(t, db.Close())
	return dataDir
}

// TestLockStoppedNode_HeldLockBlocksNodeStart is the #3 regression: the backup
// must HOLD BoltDB's shared lock for the whole archive, not probe-and-release, so
// a node cannot start (take the exclusive lock) and write into the data dir while
// the walk runs. While the returned handle is open, an exclusive Open — what a
// node start does — must fail; after Close it must succeed.
func TestLockStoppedNode_HeldLockBlocksNodeStart(t *testing.T) {
	dataDir := makeNodeBolt(t)
	boltPath := datadir.BoltPath(datadir.MetadataDir(dataDir))

	lockDB, err := lockStoppedNode(dataDir)
	require.NoError(t, err)
	require.NotNil(t, lockDB, "a stopped node's bbolt must lock")

	// A node starting now takes BoltDB's EXCLUSIVE lock; while the backup holds
	// the shared lock that must fail fast, so the node can't write mid-backup.
	_, err = bolt.Open(boltPath, 0o600, &bolt.Options{Timeout: 200 * time.Millisecond})
	require.ErrorIs(t, err, bolterrors.ErrTimeout,
		"while the backup holds the shared lock, a node's exclusive Open must fail — the lock is HELD across the walk, not released after a probe")

	// After the backup releases it, the node can start.
	require.NoError(t, lockDB.Close())
	db, err := bolt.Open(boltPath, 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err, "after the backup releases the lock, a node can start")
	require.NoError(t, db.Close())
}

// TestLockStoppedNode_RefusesRunningNode: a node holding the exclusive lock is
// refused with a clear, actionable error.
func TestLockStoppedNode_RefusesRunningNode(t *testing.T) {
	dataDir := makeNodeBolt(t)
	boltPath := datadir.BoltPath(datadir.MetadataDir(dataDir))

	node, err := bolt.Open(boltPath, 0o600, &bolt.Options{Timeout: time.Second}) // "running node"
	require.NoError(t, err)
	defer func() { _ = node.Close() }()

	_, err = lockStoppedNode(dataDir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "in use by a running node")
}

// TestLockStoppedNode_MissingBoltReturnsNilHandle: a fresh node with no bbolt.db
// yet has no lock to take; Create then surfaces the empty-dir error.
func TestLockStoppedNode_MissingBoltReturnsNilHandle(t *testing.T) {
	db, err := lockStoppedNode(t.TempDir())
	require.NoError(t, err)
	require.Nil(t, db)
}
