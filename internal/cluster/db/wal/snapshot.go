package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
)

// boltRestoreTmpPrefix / boltCompactTmpPrefix name the full-DB temp files
// RestoreSnapshot and compactLocked write beside the live bbolt.db before the
// atomic rename over it. A crash between the write and the rename orphans one;
// sweepBoltTempFiles removes it on the next Open. The trailing '.' before the
// nanosecond suffix keeps both prefixes from ever matching the live "bbolt.db".
const (
	boltRestoreTmpPrefix = "bbolt.db.restore."
	boltCompactTmpPrefix = "bbolt.db.compact."
)

// sweepBoltTempFiles removes orphaned bbolt.db.restore.* / bbolt.db.compact.*
// temp files from the metadata dir — the residue of a crash between a full-DB
// temp write (RestoreSnapshot / compactLocked) and its atomic rename. Open calls
// it before opening bbolt, mirroring recoverScrubDirs for the events dir. The
// bbolt.db.restore.* form is RTBF-relevant: it holds a leader-supplied snapshot
// payload that can carry an erased key, so a lingering copy must not survive a
// restart (a disk leak besides). A missing metadata dir is a no-op (fresh node);
// the live "bbolt.db" never matches either prefix.
func sweepBoltTempFiles(metadataDir string) error {
	entries, err := os.ReadDir(metadataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // fresh data dir; nothing to sweep
		}
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, boltRestoreTmpPrefix) || strings.HasPrefix(name, boltCompactTmpPrefix) {
			if err := os.RemoveAll(filepath.Join(metadataDir, name)); err != nil {
				return err
			}
		}
	}
	return nil
}

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
func (s *Storage) CreateSnapshot(index uint64, confState *pb.ConfState) (*pb.Snapshot, error) {
	if index > s.appliedIndex.Load() {
		return nil, fmt.Errorf("cannot snapshot at index %d: appliedIndex is only %d", index, s.appliedIndex.Load())
	}

	var buf bytes.Buffer
	err := s.view(func(tx *bolt.Tx) error {
		_, werr := tx.WriteTo(&buf)
		return werr
	})
	if err != nil {
		return nil, fmt.Errorf("snapshot bbolt: %w", err)
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

	s.snapMu.Lock()
	defer s.snapMu.Unlock()

	cs := s.snapshot.Metadata.GetConfState()
	if confState != nil {
		cs = confState
	}

	snap := pb.Snapshot{
		Data: buf.Bytes(),
		Metadata: &pb.SnapshotMetadata{
			ConfState: cs,
			Index:     &index,
			Term:      &term,
		},
	}
	s.snapshot = &snap
	// Dirty so the next Save persists the new snapshot to the state log —
	// Save only writes the snapshot on change, never per Ready.
	s.snapDirty = true
	return &snap, nil
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
func (s *Storage) RestoreSnapshot(snap *pb.Snapshot) error {
	if len(snap.Data) == 0 {
		return fmt.Errorf("restore snapshot: empty data")
	}

	// Before replacing any on-disk state, check the invariant: the
	// snapshot's metadata index must not leap past our permanent event
	// log. If it does, we can't safely serve reads afterwards and must
	// bail out; the Ready loop's invariant check will catch this at the
	// end of the iteration, but failing early here keeps bbolt intact
	// so the operator can rebuild from a clean starting point.
	if snap.Metadata.GetIndex() > s.eventIndex.Load() {
		return fmt.Errorf(
			"restore snapshot: snap.Metadata.Index=%d exceeds EventIndex=%d; run rebuild procedure",
			snap.Metadata.GetIndex(), s.eventIndex.Load(),
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
	tmpPath := filepath.Join(filepath.Dir(boltPath), fmt.Sprintf("%s%d", boltRestoreTmpPrefix, time.Now().UnixNano()))
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
	s.snapMu.Lock()
	// Clone, don't alias: snap is raft's rd.Snapshot (it may point at raft's
	// internal unstable snapshot), and ConfState() later mutates
	// s.snapshot.Metadata in place — so we must own this copy.
	s.snapshot = proto.Clone(snap).(*pb.Snapshot)
	s.snapMu.Unlock()

	// The swapped-in bbolt may carry syncable/ingestable configs whose creating
	// raft entry was compacted out of the log — a lagging follower learns them
	// ONLY via this InstallSnapshot. They now exist on disk but have no worker:
	// RestoreSnapshot only swaps bbolt, and the apply path (the only other thing
	// that sends a config to the worker channels) is skipped for the compacted
	// entries the snapshot stands in for. Without a re-drive the node has the
	// config but does nothing — and if it is later elected leader without a
	// process restart, that syncable/ingestable silently stops projecting.
	// Re-drive them (and refresh the scrub bound + config-secret gauge, which also
	// derive from the swapped bbolt) off the Ready loop; see refreshAfterRestore.
	go s.refreshAfterRestore()

	return nil
}

// refreshAfterRestore re-derives the in-memory state that hangs off bbolt after
// RestoreSnapshot swaps in a new database file: the syncable/ingestable workers,
// the scrub bound, and the config-secret/build-error gauge. It mirrors what the
// Open path does (cmd/node calls RestoreSyncableWorkers/RestoreIngestableWorkers
// after Open) for the snapshot-install path, which previously did none of it.
//
// Launched in its own goroutine by RestoreSnapshot, mirroring the Open path's
// `go RestoreSyncableWorkers`: it keeps the raft Ready loop from blocking on the
// worker-channel sends, and lets its s.view reads wait cleanly for RestoreSnapshot's
// deferred kvMu.Unlock instead of deadlocking against the held write lock.
// Duplicate sends racing a concurrent apply of the same config are collapsed by
// db.Sync/db.Ingest (replace-by-id), so this is idempotent.
func (s *Storage) refreshAfterRestore() {
	if bound, err := s.loadScrubCompleted(); err != nil {
		s.logger.Warn("restore: reload scrub bound", zap.Error(err))
	} else {
		// Adopt the restored bbolt's completed bound, as Open does. A value below
		// the current one only re-GCs an already-clean range (idempotent); the
		// scrub skip/gauge logic stays correct either way.
		s.lastScrubbedBound.Store(bound)
	}
	if err := s.validateConfigSecrets(); err != nil {
		s.logger.Warn("restore: validate config secrets", zap.Error(err))
	}
	s.RestoreSyncableWorkers()
	s.RestoreIngestableWorkers()
}

// compactLocked rewrites the bbolt database in place, dropping free pages so the
// bytes of deleted keys — notably pruned RTBF tombstones (raw subject identifiers)
// — no longer linger in the file. bbolt's Delete only frees a page, it doesn't
// zero it, and CreateSnapshot serializes the whole file via tx.WriteTo (free pages
// included), so without this an erased key would keep riding in snapshots until its
// page happened to be reused. bolt.Compact copies only live data into a fresh file;
// we then atomically rename it over the live file and reopen, mirroring
// RestoreSnapshot's swap. The logical content is unchanged, so the in-memory caches
// (databases, appliedIndex, …) stay valid.
//
// The CALLER must hold kvMu.Lock — not just so no reader observes a closed/torn
// handle, but so it can be fused with the tombstone prune into one critical
// section: CreateSnapshot copies free pages under kvMu.RLock, so a gap between a
// committed prune and this compaction would let a snapshot serialize the
// freed-but-uncompacted key. markScrubComplete holds the lock across both.
func (s *Storage) compactLocked() error {
	boltPath := s.keyValueStorage.Path()
	boltOpts := &bolt.Options{Timeout: 1 * time.Second}
	tmpPath := filepath.Join(filepath.Dir(boltPath), fmt.Sprintf("%s%d", boltCompactTmpPrefix, time.Now().UnixNano()))

	dst, err := bolt.Open(tmpPath, 0o600, boltOpts)
	if err != nil {
		return fmt.Errorf("open compaction target: %w", err)
	}
	if err := bolt.Compact(dst, s.keyValueStorage, 0); err != nil {
		_ = dst.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("compact bbolt: %w", err)
	}
	if err := dst.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close compaction target: %w", err)
	}

	// Swap: close the live handle, rename the compacted file over it (atomic on
	// POSIX), reopen. A crash between the close and the rename leaves the original
	// intact; the tombstone was already logically pruned durably, and the next
	// scrub re-compacts.
	if err := s.keyValueStorage.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close bbolt before compaction swap: %w", err)
	}
	if err := os.Rename(tmpPath, boltPath); err != nil {
		_ = os.Remove(tmpPath)
		// The live handle is already closed and this rename failed, so boltPath is
		// still the original file. Reopen it so the node survives (the scrub retries
		// later); reopenKVAfterSwapOrFatal fatals if that also fails, so a closed
		// handle never reaches the apply path.
		s.reopenKVAfterSwapOrFatal(boltPath, boltOpts, "rename compacted bbolt failed")
		return fmt.Errorf("rename compacted bbolt: %w", err)
	}
	// Rename succeeded: boltPath is now the compacted file. Reopen it or fatal —
	// previously a failed reopen here returned into the survive-and-continue scrub
	// worker and left the closed handle for the apply path to hit later, as a
	// delayed ErrDatabaseNotOpen crash mis-attributed to apply.
	s.reopenKVAfterSwapOrFatal(boltPath, boltOpts, "reopen bbolt after compaction")
	return nil
}

// reopenKVAfterSwapOrFatal reopens bbolt at boltPath and reassigns
// s.keyValueStorage, or FATALS if the reopen fails. Every post-close branch of
// compactLocked calls it: the live handle is already closed, so returning an
// error into the scrub worker (which only logs and continues — see
// runPendingScrub) would leave the closed handle for the apply path to trip over
// later, as an ErrDatabaseNotOpen crash in saveAppliedIndex long after and
// mis-attributed to the real swap failure. Fataling here gives correct
// attribution at the swap site; the on-disk file is a valid bbolt (the rename is
// atomic), so a restart recovers cleanly. Caller holds kvMu.Lock.
func (s *Storage) reopenKVAfterSwapOrFatal(boltPath string, boltOpts *bolt.Options, what string) {
	reopened, err := bolt.Open(boltPath, 0o600, boltOpts)
	if err != nil {
		s.logger.Fatal("bbolt swap could not reopen storage; the node cannot continue (restart to recover from the on-disk file)",
			zap.String("op", what), zap.String("path", boltPath), zap.Error(err))
	}
	s.keyValueStorage = reopened
}
