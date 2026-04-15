package wal

import (
	"bytes"
	"encoding/binary"
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
	err := s.view(func(tx *bolt.Tx) error {
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
// Concurrency: RestoreSnapshot takes kvMu for write, so it blocks any
// concurrent view / update callers until the close-swap-reopen dance
// finishes and the new bbolt handle is installed. In-flight queries
// started before RestoreSnapshot was called complete before the close
// happens (RLock → Lock serialization), so no read ever sees a closed
// handle.
//
// The file swap itself is not atomic across close / rename / reopen:
// a crash anywhere in that window leaves the node unable to start, and
// an operator must rerun the rebuild procedure. The Ready loop's
// invariant check catches the much more common failure, which is a
// snapshot that advances raft's applied index past this node's
// permanent event log: that condition is caught before any bbolt
// content is touched and fatal-exits with the rebuild message.
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

	// Serialize against every concurrent bbolt reader/writer. Held for
	// the full swap so nothing outside this function observes a closed
	// or torn bbolt handle.
	s.kvMu.Lock()
	defer s.kvMu.Unlock()

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

	// Post-swap reloads. We can't use the s.view / s.update helpers
	// here because we still hold kvMu.Lock and they take kvMu.RLock —
	// that would deadlock. Read directly against the local db handle;
	// the lock ensures nothing else is touching it.
	var idx uint64
	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(appliedIndexBucket)
		if b == nil {
			return ErrBucketMissing
		}
		v := b.Get(appliedIndexKey)
		if v == nil {
			return nil
		}
		if len(v) != 8 {
			return fmt.Errorf("appliedIndex: expected 8 bytes, got %d", len(v))
		}
		idx = binary.BigEndian.Uint64(v)
		return nil
	}); err != nil {
		return fmt.Errorf("reload appliedIndex: %w", err)
	}
	s.appliedIndex.Store(idx)

	// Drop any in-memory database handles and rebuild from the
	// restored bucket. Walks the `databases` bucket directly since the
	// loadDatabases helper routes through s.view.
	s.databases = make(map[string]cluster.Database)
	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(databaseBucket)
		if b == nil {
			return ErrBucketMissing
		}
		return forEachCurrent(b, func(id, data []byte) error {
			cfg := &cluster.Configuration{}
			if err := cfg.Unmarshal(data); err != nil {
				return err
			}
			_, parsed, err := s.parser.ParseDatabase(cfg.MimeType, cfg.Data)
			if err != nil {
				return err
			}
			s.databases[cfg.ID] = parsed
			return nil
		})
	}); err != nil {
		return fmt.Errorf("reload databases: %w", err)
	}

	// Record the snapshot so Storage.Snapshot() returns it and so
	// InitialState reflects the restored confState.
	s.snapshot = snap

	return nil
}
