package wal

import (
	"fmt"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// EventIndex returns the highest raft entry index that has been durably
// written to the permanent event log on this node. This is "P_local" in
// the storage invariant P_local == R_local, which the Ready loop checks
// after every iteration (see raft.go's checkStorageInvariant). On a fresh
// or brand-new node eventLog starts empty and EventIndex is 0.
func (s *Storage) EventIndex() uint64 {
	return s.eventIndex.Load()
}

// recoverEventIndex restores durable append progress, including indexes whose
// records have been erased. Open calls it before bbolt reconciliation so the
// snapshot guard uses storage progress rather than the last surviving record.
func (s *Storage) recoverEventIndex() error {
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	last, ok, err := s.eventLog.entries.LastAppended()
	if err != nil {
		return fmt.Errorf("event log recover append progress: %w", err)
	}
	if ok {
		s.eventIndex.Store(last)
	}
	return nil
}

// DataEventIndex returns the highest raft index of a DATA (non-metadata)
// entry applied on this node — the head a per-syncable reader converges to
// at EOF. It is the reference for per-syncable lag (lag = max(0,
// DataEventIndex − checkpoint)); unlike EventIndex it excludes the
// syncable-metadata entries the reader skips, so a caught-up syncable reads
// lag 0 instead of a phantom backlog of trailing index bumps. O(1), local,
// and replicated-deterministic (every node applied through index i computes
// the same value). 0 on a node that has applied no data entries yet.
func (s *Storage) DataEventIndex() uint64 {
	return s.dataEventIndex.Load()
}

// firstEventSeq returns the wal sequence of the first entry in the
// permanent event log, or 0 when the log is empty. Package-internal.
// Takes eventMu.RLock so it never observes the handle mid-swap.
func (s *Storage) firstEventSeq() (uint64, error) {
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	return s.firstEventSeqLocked()
}

// firstEventSeqLocked is firstEventSeq without the lock; the caller must hold
// eventMu (R or W). Used by the scrub swap, which already holds eventMu.Lock
// and would deadlock re-acquiring RLock.
func (s *Storage) firstEventSeqLocked() (uint64, error) {
	return s.nativeEventTransferLocked().FirstSequence()
}

// lastEventSeq returns the wal sequence of the last entry in the
// permanent event log, or 0 when the log is empty. Package-internal.
func (s *Storage) lastEventSeq() (uint64, error) {
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	return s.lastEventSeqLocked()
}

// lastEventSeqLocked is lastEventSeq without the lock; caller must hold eventMu.
func (s *Storage) lastEventSeqLocked() (uint64, error) {
	return s.nativeEventTransferLocked().LastSequence()
}

// readEventAt reads the pb.Entry bytes at the given wal sequence from the
// permanent event log, verifying the per-entry checksum (see checksum.go).
// Package-internal; callers outside the storage tier should go through the
// Reader abstraction, which handles raft index ↔ wal seq translation and
// filters metadata entries.
func (s *Storage) readEventAt(seq uint64) ([]byte, error) {
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	return s.readEventAtLocked(seq)
}

// readEventAtLocked is readEventAt without the lock; caller must hold eventMu.
func (s *Storage) readEventAtLocked(seq uint64) ([]byte, error) {
	return s.nativeEventTransferLocked().ReadPayload(seq)
}

// unframe verifies and strips the checksum frame from a raw log read,
// recording a corruption-counter sample (attributed to logName) before
// returning ErrCorruptEntry on invalid framing or a CRC mismatch. The metrics
// handle is nil-safe. Unframed entries are rejected.
func (s *Storage) unframe(raw []byte, logName string) ([]byte, error) {
	payload, err := unframe(raw)
	if err != nil {
		s.recordCorrupt(logName)
		return nil, err
	}
	return payload, nil
}

// recordCorrupt bumps the corruption counter for logName when metrics are
// enabled. A nil *Metrics (metrics disabled) is a no-op.
func (s *Storage) recordCorrupt(logName string) {
	if s.metrics != nil {
		s.metrics.WalCorruptEntry(logName)
	}
}

// appendEvent writes a committed raft entry's raw bytes to the
// permanent event log, stamping firstEventIndex on the very first
// write and bumping eventIndex on success. Called only from
// ApplyCommitted, which already guards against writes at or below
// eventIndex (crash-window idempotence). Kept on Storage (not inlined
// into ApplyCommitted) so there's exactly one site that advances
// P_local.
// appendEvents is appendEvent for one Ready's worth of entries: all frames go
// to the event log in ONE batched write (one sync) instead of one write+sync
// per entry — the apply loop's dominant fsync cost. Entries already in the log
// (index <= eventIndex) are skipped, mirroring ApplyCommitted's guard, so a
// restart replay never double-appends. Same eventMu.RLock scope as appendEvent
// for the same scrub-swap reason.
func (s *Storage) appendEvents(entries []*pb.Entry) error {
	s.eventAppendMu.Lock()
	defer s.eventAppendMu.Unlock()
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()

	appender := s.eventAppenderLocked()
	_, hasHistory, err := appender.LastAppended()
	if err != nil {
		return fmt.Errorf("event log last index: %w", err)
	}
	records := make([]eventlog.Record, 0, len(entries))
	first, last := uint64(0), uint64(0)
	wasEmpty := !hasHistory
	for _, entry := range entries {
		if entry.GetIndex() <= s.eventIndex.Load() {
			continue
		}
		entryBytes, err := proto.Marshal(entry)
		if err != nil {
			return fmt.Errorf("marshal entry for event log: %w", err)
		}
		records = append(records, eventlog.Record{ID: entry.GetIndex(), Payload: entryBytes})
		if first == 0 {
			first = entry.GetIndex()
		}
		last = entry.GetIndex()
	}
	if last == 0 {
		return nil
	}
	if err := appender.Append(records); err != nil {
		return fmt.Errorf("event log write batch (raft indexes %d-%d): %w", first, last, err)
	}
	s.eventLogWriteOps.Add(1)
	if wasEmpty {
		// The log was empty before this batch: record the raft index its
		// first record carries.
		s.firstEventIndex.Store(first)
	}
	s.eventIndex.Store(last)
	return nil
}

func (s *Storage) appendEvent(entry *pb.Entry) error {
	s.eventAppendMu.Lock()
	defer s.eventAppendMu.Unlock()
	// RLock keeps backend progress and the append on the same handle across
	// a scrub swap. Shared with concurrent readers; only the swap
	// (eventMu.Lock) is excluded.
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()

	entryBytes, err := proto.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal entry for event log: %w", err)
	}
	appender := s.eventAppenderLocked()
	_, hasHistory, err := appender.LastAppended()
	if err != nil {
		return fmt.Errorf("event log last index: %w", err)
	}
	if err := appender.Append([]eventlog.Record{{ID: entry.GetIndex(), Payload: entryBytes}}); err != nil {
		return fmt.Errorf("event log write raft index %d: %w", entry.GetIndex(), err)
	}
	s.eventLogWriteOps.Add(1)
	if !hasHistory {
		s.firstEventIndex.Store(entry.GetIndex())
	}
	s.eventIndex.Store(entry.GetIndex())
	return nil
}
