package datadir

import (
	"errors"
	"fmt"
	"os"
	"time"

	bolt "go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"
)

// The stopped-node lock, shared by every offline maintenance tool that
// touches a data directory (backup, wal repair, wal decompress). A running
// node holds BoltDB's exclusive file lock on its metadata database for its
// whole lifetime (wal.Open takes it), so locking that same database is the
// one liveness signal that cannot drift from the node's own behavior.
//
// The lock is HELD for the duration of the maintenance, not probed and
// released: a probe would leave a window where a node starts (taking the
// exclusive lock) while the tool is still walking or rewriting the very
// files the node now serves. Holding it closes both directions — a live node
// refuses the tool, and an in-progress tool makes a starting node fail its
// own bbolt open, loudly, instead of racing the maintenance.
//
// Two modes, by what the tool does to the directory:
//   - LockStoppedNode (shared): for readers like backup. Concurrent readers
//     coexist; only a running node (or an exclusive-mode tool) conflicts.
//   - LockStoppedNodeExclusive: for rewriters like `wal repair --commit` and
//     `wal decompress`. A rewrite must also exclude concurrent readers (a
//     backup archived mid-rewrite is silently inconsistent) and other
//     rewriters, not just the node.

// ErrNodeRunning wraps every lock refusal, so callers can attach their own
// operational guidance (a docs pointer) to exactly the contention case.
var ErrNodeRunning = errors.New("the data directory is in use by a running node")

// lockStoppedNode is the shared implementation. A nil, nil return means the
// directory has no metadata database to lock (a fresh node, or a partial
// copy of the log dirs alone) — there is no liveness to prove, and the
// caller's own open/validation surfaces whatever is actually wrong.
func lockStoppedNode(root string, shared bool) (*bolt.DB, error) {
	boltPath := BoltPath(MetadataDir(root))
	if _, err := os.Stat(boltPath); err != nil {
		return nil, nil //nolint:nilnil // absent bbolt: no lock to take; see doc comment
	}
	db, err := bolt.Open(boltPath, 0o600, &bolt.Options{ReadOnly: shared, Timeout: 2 * time.Second})
	if errors.Is(err, bolterrors.ErrTimeout) {
		return nil, fmt.Errorf("%w or another maintenance process: could not lock %s under %q; stop it first (for a live cluster: stop one follower — quorum holds — run the maintenance, restart it)",
			ErrNodeRunning, boltPath, root)
	}
	if err != nil {
		return nil, fmt.Errorf("open metadata db %s: %w", boltPath, err)
	}
	return db, nil
}

// LockStoppedNode takes and HOLDS a shared lock on the node's metadata
// database, refusing a running node. For maintenance that only READS the
// data directory. Close the returned handle when done; nil means there was
// no metadata database to lock.
func LockStoppedNode(root string) (*bolt.DB, error) {
	return lockStoppedNode(root, true)
}

// LockStoppedNodeExclusive is LockStoppedNode with an exclusive lock, for
// maintenance that REWRITES the data directory — it additionally excludes
// concurrent shared-mode readers and other rewriters.
func LockStoppedNodeExclusive(root string) (*bolt.DB, error) {
	return lockStoppedNode(root, false)
}
