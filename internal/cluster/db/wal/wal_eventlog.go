package wal

import (
	"fmt"

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

// recoverEventIndex sets eventIndex from the last durable event-log entry's raft
// index. Open calls it early so reconcileBboltWithSnapshot can apply the same
// snapIdx <= eventIndex guard RestoreSnapshot uses before any bbolt swap. It is
// idempotent with the fuller event-log recovery later in Open, which re-derives
// the same value alongside firstEventIndex/dataEventIndex.
func (s *Storage) recoverEventIndex() error {
	last, err := s.eventLog.LastIndex()
	if err != nil {
		return err
	}
	if last == 0 {
		return nil
	}
	data, err := s.readEventAt(last)
	if err != nil {
		return fmt.Errorf("event log read last entry: %w", err)
	}
	e := &pb.Entry{}
	if err := proto.Unmarshal(data, e); err != nil {
		return fmt.Errorf("event log unmarshal last entry: %w", err)
	}
	s.eventIndex.Store(e.GetIndex())
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
	return s.eventLog.FirstIndex()
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
	return s.eventLog.LastIndex()
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
	raw, err := s.eventLog.Read(seq)
	if err != nil {
		return nil, err
	}
	return s.unframe(raw, "event_log")
}

// unframe verifies and strips the checksum frame from a raw log read,
// recording a corruption-counter sample (attributed to logName) before
// returning ErrCorruptEntry on a CRC mismatch. The metrics handle is
// nil-safe. Legacy un-checksummed entries pass through unchanged.
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
func (s *Storage) appendEvent(entry *pb.Entry) error {
	// RLock for the whole body so the seq it computes (LastIndex+1) and the
	// Write that consumes it can't straddle a scrub swap that would replace the
	// handle underneath them. Shared with concurrent readers; only the swap
	// (eventMu.Lock) is excluded.
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()

	entryBytes, err := proto.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal entry for event log: %w", err)
	}
	nextSeq, err := s.eventLog.LastIndex()
	if err != nil {
		return fmt.Errorf("event log last index: %w", err)
	}
	nextSeq++
	if err := s.eventLog.Write(nextSeq, frame(entryBytes)); err != nil {
		return fmt.Errorf("event log write seq %d (raft index %d): %w", nextSeq, entry.GetIndex(), err)
	}
	if nextSeq == 1 {
		s.firstEventIndex.Store(entry.GetIndex())
	}
	s.eventIndex.Store(entry.GetIndex())
	return nil
}
