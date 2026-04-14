package wal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/philborlin/committed/internal/cluster"
	bolt "go.etcd.io/bbolt"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

// CreateSnapshot captures the current metadata state (bbolt content) as
// a pb.Snapshot keyed at the given raft index. It is called periodically
// from the raft serve loop so raft has a metadata-only snapshot to ship
// to followers whose raft logs have been compacted past.
//
// The snapshot's Data field holds the serialized bbolt database. The
// permanent event log and time-series store are NOT in the snapshot —
// they live on every node's local disk and are too large to ship
// through raft. This is the "metadata snapshot install" shape from
// docs/event-log-architecture.md § "Severe lag": a follower that
// receives this snapshot and whose permanent event log is already
// current finishes catchup via normal raft replication; a follower too
// far behind tripls the storage invariant and fatal-exits.
func (s *Storage) CreateSnapshot(index uint64, confState *pb.ConfState) (pb.Snapshot, error) {
	if index > s.appliedIndex.Load() {
		return pb.Snapshot{}, fmt.Errorf("cannot snapshot at index %d: appliedIndex is only %d", index, s.appliedIndex.Load())
	}

	var buf bytes.Buffer
	err := s.keyValueStorage.View(func(tx *bolt.Tx) error {
		_, werr := tx.WriteTo(&buf)
		return werr
	})
	if err != nil {
		return pb.Snapshot{}, fmt.Errorf("snapshot bbolt: %w", err)
	}

	term, err := s.Term(index)
	if err != nil {
		// Term may be unavailable if the entry at `index` has already
		// been compacted out of the raft log. That's fine for a fresh
		// snapshot because the snapshot metadata's Term field will be
		// filled by the caller from raft's own state — CreateSnapshot
		// is a content-capture operation, not a raft-state-capture
		// one.
		term = 0
	}

	cs := pb.ConfState{}
	if confState != nil {
		cs = *confState
	} else {
		cs = s.snapshot.Metadata.ConfState
	}

	snap := pb.Snapshot{
		Data: buf.Bytes(),
		Metadata: pb.SnapshotMetadata{
			ConfState: cs,
			Index:     index,
			Term:      term,
		},
	}
	s.snapshot = snap
	return snap, nil
}

// RestoreSnapshot installs the metadata state carried by snap onto this
// node, replacing the current bbolt contents. Called from
// raft.processSnapshot when raft delivers a non-empty rd.Snapshot in
// the Ready loop.
//
// The operation is not atomic: we close the current bolt handle,
// overwrite bbolt.db on disk, and reopen. A crash between close and
// reopen leaves the node unable to start — an operator must rerun the
// rebuild procedure in that case. The Ready loop's invariant check
// catches the much more common failure, which is a snapshot that
// advances raft's applied index past this node's permanent event log:
// that condition is caught before any bbolt content is touched and
// fatal-exits with the rebuild message.
func (s *Storage) RestoreSnapshot(snap pb.Snapshot) error {
	if len(snap.Data) == 0 {
		return fmt.Errorf("restore snapshot: empty data")
	}

	// Before replacing any on-disk state, check the invariant: the
	// snapshot's metadata index must not leap past our permanent event
	// log. If it does, we can't safely serve reads afterwards and must
	// bail out; the Ready loop's invariant check will catch this at the
	// end of the iteration, but failing early here keeps bbolt intact
	// so the operator can rebuild from a clean starting point.
	if snap.Metadata.Index > s.eventIndex.Load() {
		return fmt.Errorf(
			"restore snapshot: snap.Metadata.Index=%d exceeds EventIndex=%d; run rebuild procedure",
			snap.Metadata.Index, s.eventIndex.Load(),
		)
	}

	// Close the current bolt handle so we can overwrite its file.
	boltPath := s.keyValueStorage.Path()
	if err := s.keyValueStorage.Close(); err != nil {
		return fmt.Errorf("close bbolt before restore: %w", err)
	}

	// Write the snapshot's bbolt content to a sibling file, then rename
	// over the live file. Rename is atomic on POSIX, so a crash between
	// the Write and Rename leaves the original file untouched and the
	// next Open sees a consistent state.
	tmpPath := filepath.Join(filepath.Dir(boltPath), fmt.Sprintf("bbolt.db.restore.%d", time.Now().UnixNano()))
	if err := os.WriteFile(tmpPath, snap.Data, 0o600); err != nil {
		return fmt.Errorf("write restored bbolt to tmp: %w", err)
	}
	if err := os.Rename(tmpPath, boltPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename restored bbolt: %w", err)
	}

	// Reopen bbolt. Match the Open-path options: 1s timeout, NoSync
	// stays off (fsync path; restored snapshots should be durable).
	boltOpts := &bolt.Options{Timeout: 1 * time.Second}
	db, err := bolt.Open(boltPath, 0o600, boltOpts)
	if err != nil {
		return fmt.Errorf("reopen bbolt after restore: %w", err)
	}
	s.keyValueStorage = db

	// Refresh cached metadata: the snapshot replaced bbolt content,
	// which includes the appliedIndex bucket. Load it so subsequent
	// ApplyCommitted calls treat already-snapshotted entries as
	// already applied, and re-load databases since their configs
	// may have changed.
	idx, err := s.loadAppliedIndex()
	if err != nil {
		return fmt.Errorf("reload appliedIndex: %w", err)
	}
	s.appliedIndex.Store(idx)

	// Drop any in-memory database handles and rebuild from the
	// restored bucket. loadDatabases populates s.databases afresh.
	s.databases = make(map[string]cluster.Database)
	if err := s.loadDatabases(); err != nil {
		return fmt.Errorf("reload databases: %w", err)
	}

	// Record the snapshot so Storage.Snapshot() returns it and so
	// InitialState reflects the restored confState.
	s.snapshot = snap

	return nil
}
