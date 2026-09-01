package datadir_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"

	"github.com/committeddb/committed/internal/cluster/db/datadir"
)

// makeNodeBolt creates a minimal node metadata/bbolt.db so the stopped-node
// lock has a file to lock, and returns the data dir.
func makeNodeBolt(t *testing.T) string {
	t.Helper()
	dataDir := t.TempDir()
	require.NoError(t, os.MkdirAll(datadir.MetadataDir(dataDir), 0o700))
	db, err := bolt.Open(datadir.BoltPath(datadir.MetadataDir(dataDir)), 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	require.NoError(t, db.Close())
	return dataDir
}

// TestLockStoppedNode_HeldLockBlocksNodeStart pins the held-not-probed
// contract: the maintenance tool must HOLD the lock for its whole run, so a
// node cannot start (take the exclusive lock) and write into the data dir
// mid-walk. While the returned handle is open, an exclusive Open — what a
// node start does — must fail; after Close it must succeed.
func TestLockStoppedNode_HeldLockBlocksNodeStart(t *testing.T) {
	dataDir := makeNodeBolt(t)
	boltPath := datadir.BoltPath(datadir.MetadataDir(dataDir))

	lockDB, err := datadir.LockStoppedNode(dataDir)
	require.NoError(t, err)
	require.NotNil(t, lockDB, "a stopped node's bbolt must lock")

	_, err = bolt.Open(boltPath, 0o600, &bolt.Options{Timeout: 200 * time.Millisecond})
	require.ErrorIs(t, err, bolterrors.ErrTimeout,
		"while maintenance holds the shared lock, a node's exclusive Open must fail — the lock is HELD, not released after a probe")

	require.NoError(t, lockDB.Close())
	db, err := bolt.Open(boltPath, 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err, "after maintenance releases the lock, a node can start")
	require.NoError(t, db.Close())
}

// TestLockStoppedNode_RefusesRunningNode: a node holding the exclusive lock
// is refused with a clear, actionable, matchable error.
func TestLockStoppedNode_RefusesRunningNode(t *testing.T) {
	dataDir := makeNodeBolt(t)
	boltPath := datadir.BoltPath(datadir.MetadataDir(dataDir))

	node, err := bolt.Open(boltPath, 0o600, &bolt.Options{Timeout: time.Second}) // "running node"
	require.NoError(t, err)
	defer func() { _ = node.Close() }()

	_, err = datadir.LockStoppedNode(dataDir)
	require.ErrorIs(t, err, datadir.ErrNodeRunning)
	require.Contains(t, err.Error(), "in use by a running node")

	_, err = datadir.LockStoppedNodeExclusive(dataDir)
	require.ErrorIs(t, err, datadir.ErrNodeRunning, "the exclusive mode refuses a running node too")
}

// TestLockStoppedNodeExclusive_ExcludesReaders: a rewriter (wal repair
// --commit, wal decompress) must not run under a concurrent reader — a
// backup archived mid-rewrite is silently inconsistent — while two readers
// coexist by design.
func TestLockStoppedNodeExclusive_ExcludesReaders(t *testing.T) {
	dataDir := makeNodeBolt(t)

	reader, err := datadir.LockStoppedNode(dataDir)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	reader2, err := datadir.LockStoppedNode(dataDir)
	require.NoError(t, err, "two readers (concurrent backups) coexist")
	require.NoError(t, reader2.Close())

	_, err = datadir.LockStoppedNodeExclusive(dataDir)
	require.ErrorIs(t, err, datadir.ErrNodeRunning,
		"a rewriter must refuse to run under a held reader lock")

	require.NoError(t, reader.Close())
	rw, err := datadir.LockStoppedNodeExclusive(dataDir)
	require.NoError(t, err, "with all readers released, the rewriter locks")
	require.NotNil(t, rw)
	require.NoError(t, rw.Close())
}

// TestLockStoppedNode_MissingBoltReturnsNilHandle: a fresh node with no
// bbolt.db yet has no lock to take; the caller's own open/validation
// surfaces whatever is actually wrong with the directory.
func TestLockStoppedNode_MissingBoltReturnsNilHandle(t *testing.T) {
	db, err := datadir.LockStoppedNode(t.TempDir())
	require.NoError(t, err)
	require.Nil(t, db)
}
