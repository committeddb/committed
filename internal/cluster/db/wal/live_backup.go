package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/tidwall/wal"
	bolt "go.etcd.io/bbolt"

	"github.com/committeddb/committed/internal/cluster/backup"
	"github.com/committeddb/committed/internal/cluster/db/datadir"
)

// A live backup reads a running node's four stores one after another, and
// the ORDER is what makes the result a directory a node can boot from.
// Each store is read at a later instant than the one before, and a restart
// tolerates exactly the shapes that ordering produces:
//
//  1. metadata (bbolt) — applied index A, from one read transaction;
//  2. raft state log — HardState commit C ≥ A and the persisted snapshot;
//  3. permanent event log — events through E ≥ A, under its freeze;
//  4. raft entry log — entries through X ≥ E, under its freeze.
//
// A restored node replays (A, C] from its raft log, skipping events it
// already holds, so it needs A ≤ C ≤ X and A ≤ E ≤ X: later reads see
// larger indexes, and entries a commit covers are never truncated. The
// one order that fails is the entry log BEFORE the event log — the
// archive could then hold events past the raft log's end, and the restored
// node would skip its first new entries at those indexes as "already
// written" while keeping the old cluster's. A snapshot install between the
// state-log and entry-log reads would leave a commit index below the entry
// log's first index; the entry log's epoch detects it and the backup starts
// over. A truncation under a read makes a listed file vanish or shrink; the
// read fails and the backup starts over.
//
// Everything a raft log holds at index ≤ C is byte-identical on every node,
// and the event log is too at one generation, so a live archive restores
// the same node the offline one would have — the restore path is shared.

// ErrLiveBackupRaced is a live backup the node's own maintenance overtook —
// a snapshot install or a raft-log truncation under the read. The archive
// is incomplete; take it again.
var ErrLiveBackupRaced = errors.New("live backup: the node installed a snapshot or truncated its raft log during the backup; take it again")

// ErrLiveBackupEventsAhead is a capture whose event log runs past its raft
// log — the shape of a node catching up from a peer, whose events arrive
// without raft entries behind them. Such an archive would restore a node
// that skips its first new entries as already written; refused instead.
var ErrLiveBackupEventsAhead = errors.New("live backup: this node's event log runs past its raft log (it is catching up from a peer); take the backup once it has caught up")

// CaptureBackup implements backup.LiveSource: it hands visit each store's
// files in the consistent order above.
func (s *Storage) CaptureBackup(visit func(name string, size int64, write func(io.Writer) error) error) (backup.LiveInfo, error) {
	root := filepath.Dir(s.eventLogDir)
	info := backup.LiveInfo{DataDir: root}

	// 1. Metadata: the applied index and the file's bytes from ONE read
	// transaction, so the manifest's applied index is the archive's. The
	// bytes are spooled to a sibling file under the transaction and
	// streamed from it afterwards: a read transaction held open across the
	// stream would block bbolt from growing (its remap waits for readers),
	// stalling this node's apply path for as long as the receiver took.
	spool := s.newBoltTmpPath(s.keyValueStorage.Path(), datadir.BoltBackupPrefix)
	defer func() { _ = os.Remove(spool) }()
	var size int64
	if err := s.view(func(tx *bolt.Tx) error {
		if b := tx.Bucket(appliedIndexBucket); b != nil {
			if v := b.Get(appliedIndexKey); len(v) == 8 {
				info.AppliedIndex = binary.BigEndian.Uint64(v)
			}
		}
		f, err := os.OpenFile(spool, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // G304: a temp file beside this node's own bbolt
		if err != nil {
			return err
		}
		size, err = tx.WriteTo(f)
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		return err
	}); err != nil {
		return info, fmt.Errorf("live backup: metadata: %w", err)
	}
	name := filepath.ToSlash(filepath.Join(filepath.Base(datadir.MetadataDir(root)), filepath.Base(s.keyValueStorage.Path())))
	if err := visit(name, size, func(w io.Writer) error {
		f, err := os.Open(spool) //nolint:gosec // G304: the spool this capture just wrote
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		_, err = io.CopyN(w, f, size)
		return err
	}); err != nil {
		return info, fmt.Errorf("live backup: metadata: %w", err)
	}

	epoch := s.entryLogEpoch.Load()

	// 2. State log. Its re-anchoring truncation is not held; a file it
	// removes under the read fails the read, and the backup starts over.
	if err := s.captureLog(s.StateLog, root, visit); err != nil {
		return info, fmt.Errorf("live backup: raft state log: %w", err)
	}

	// 3. Event log, under its freeze: the sealer, the scrub swap, and a
	// catch-up's adoption or reset step aside; the tail is read to the
	// length committed at listing time and may grow behind the read.
	var eventIndex uint64
	if err := func() error {
		release := s.FreezeEventLayout()
		defer release()
		info.EventLogGeneration = s.EventLogGeneration()
		eventIndex = s.eventIndex.Load()
		s.eventMu.RLock()
		log := s.eventLog
		s.eventMu.RUnlock()
		return s.captureLog(log, root, visit)
	}(); err != nil {
		return info, fmt.Errorf("live backup: event log: %w", err)
	}

	// 4. Entry log, under its freeze: compaction defers. Last, so it covers
	// every event the archive holds — which it does on a node whose events
	// came through raft, and does not on one catching up from a peer.
	var lastIndex uint64
	if err := func() error {
		release := s.FreezeRaftLayout()
		defer release()
		s.entryMu.RLock()
		log := s.EntryLog
		s.entryMu.RUnlock()
		// Read BEFORE the listing: entries appended after it are not in the
		// archive, so a later read could pass an archive that does not
		// cover its events. Before it, the check is conservative.
		lastIndex = s.lastIndex.Load()
		return s.captureLog(log, root, visit)
	}(); err != nil {
		return info, fmt.Errorf("live backup: raft entry log: %w", err)
	}

	if s.entryLogEpoch.Load() != epoch {
		return info, ErrLiveBackupRaced
	}
	if lastIndex < eventIndex {
		return info, ErrLiveBackupEventsAhead
	}
	return info, nil
}

// captureLog hands visit a log's sealed segment files whole and its tail to
// the length committed when the layout was listed.
func (s *Storage) captureLog(log *wal.Log, root string, visit func(string, int64, func(io.Writer) error) error) error {
	lay, err := log.LayoutSnapshot()
	if err != nil {
		if errors.Is(err, wal.ErrClosed) {
			return fmt.Errorf("%w: the log was replaced under the read", ErrLiveBackupRaced)
		}
		return err
	}
	for _, sg := range lay.Sealed {
		fi, err := os.Stat(sg.Path)
		if err != nil {
			return raced(err)
		}
		if err := s.captureFile(root, sg.Path, fi.Size(), visit); err != nil {
			return err
		}
	}
	return s.captureFile(root, lay.Tail.Path, lay.TailLen, visit)
}

func (s *Storage) captureFile(root, path string, size int64, visit func(string, int64, func(io.Writer) error) error) error {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return err
	}
	return visit(filepath.ToSlash(rel), size, func(w io.Writer) error {
		f, err := os.Open(path) //nolint:gosec // G304: a segment file of this node's own logs, listed under a freeze
		if err != nil {
			return raced(err)
		}
		defer func() { _ = f.Close() }()
		n, err := io.CopyN(w, f, size)
		if errors.Is(err, io.EOF) || (err == nil && n != size) {
			return fmt.Errorf("%w: %s shrank under the read", ErrLiveBackupRaced, filepath.Base(path))
		}
		return err
	})
}

// raced turns a listed file that has vanished into the retryable outcome;
// any other failure is what it is.
func raced(err error) error {
	if os.IsNotExist(err) {
		return fmt.Errorf("%w: %v", ErrLiveBackupRaced, err)
	}
	return err
}
