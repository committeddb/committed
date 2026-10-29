package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"time"

	bolt "go.etcd.io/bbolt"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/datadir"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// The event-log fetch: how a node hands its permanent event log to a peer
// that is behind (the serving side, ServeEvents) and how a node takes one in
// (the receiving side, AppendFetchedRecords). A node whose event log is behind
// the snapshot raft wants to install cannot fill the gap from raft — the
// entries between were compacted — so it fills it from a peer instead of
// exiting (db/catchup.go). Events travel as framed records; protobuf payloads
// retain the peer's exact bytes. The receiving side is idempotent by raft
// index, so a retried or overlapping fetch is harmless.
//
// A log's GENERATION is the scrub bound its bytes reflect (EventLogGeneration).
// Every replica's rewrite is deterministic, so two logs at one generation
// hold the same records over their shared prefix; one can extend the other.
// The receiver never mixes generations — db/catchup.go pins one per fetch and
// discards its log when a peer's is newer — because a log stitched from two
// could hold an entity's raw upsert in one part and its hashed delete in the
// other, which no later scrub could pair up.

// ErrLayoutFrozen refuses a change to the set of event-log files while an
// event-log layout freeze stands (a peer fetch or live backup is reading them); the
// caller retries once it lifts.
var ErrLayoutFrozen = errors.New("event log layout is frozen for a reader; retry")

// BeginCatchUp holds this storage's pending scrub off while a catch-up fills
// the event log: a rewrite over an emptied or partial log would either
// fatal (nothing to keep) or stamp a generation whose content is not that
// generation's. The release lets the worker run again — the Ready loop calls
// it once the snapshot has installed — and pokes it, so a bound it deferred
// is not left waiting for the next signal.
func (s *Storage) BeginCatchUp() func() {
	s.catchingUp.Store(true)
	return func() {
		s.catchingUp.Store(false)
		s.signalScrub()
	}
}

// EventLogGeneration identifies the content of this node's event log: the
// scrub bound its bytes reflect — the completed bound, or the bound of a
// rewrite that has swapped in but not yet marked complete (the content is
// already the new one).
func (s *Storage) EventLogGeneration() uint64 {
	return max(s.lastScrubbedBound.Load(), s.swappedBound.Load())
}

// SetEventLogGeneration records, durably, the generation of content this
// node is about to adopt into an empty log from a peer: the completed
// scrub bound becomes the peer's, so a Scrub command at or below it is a
// no-op here (its rewrite is already in the bytes), a pending one beyond it
// re-runs, and a restart mid-fetch resumes at this generation.
func (s *Storage) SetEventLogGeneration(gen uint64) error {
	s.eventMu.RLock()
	shared := s.eventLog.managed != nil
	s.eventMu.RUnlock()
	persist := func() error { return s.putScrubCompleted(gen) }
	if shared {
		if err := s.initializeFetchedGenerationWith(context.Background(), gen, persist); err != nil {
			return err
		}
		// Also drains reset retirement left by an interruption before refetch.
		if _, err := s.reclaimSharedGeneration(context.Background(), gen); err != nil {
			return err
		}
	} else if err := persist(); err != nil {
		return err
	}

	s.lastScrubbedBound.Store(gen)
	s.swappedBound.Store(gen)
	return nil
}

// SnapshotScrubCompleted reads the completed scrub bound a snapshot's bbolt
// payload carries: the generation every tombstone through it has been pruned
// from, and so the least generation a log installing it may be at.
func (s *Storage) SnapshotScrubCompleted(snap *pb.Snapshot) (uint64, error) {
	if len(snap.Data) == 0 {
		return 0, errors.New("snapshot has no payload")
	}
	tmp := s.newBoltTmpPath(s.keyValueStorage.Path(), datadir.BoltRestorePrefix)
	if err := os.WriteFile(tmp, snap.Data, 0o600); err != nil {
		_ = os.Remove(tmp)
		return 0, err
	}
	defer func() { _ = os.Remove(tmp) }()
	kv, err := bolt.Open(tmp, 0o600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		return 0, fmt.Errorf("open snapshot payload: %w", err)
	}
	defer func() { _ = kv.Close() }()
	var bound uint64
	err = kv.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(pendingScrubBucket)
		if bkt == nil {
			return nil
		}
		if b := bkt.Get(scrubCompletedKey); len(b) == 8 {
			bound = binary.BigEndian.Uint64(b)
		}
		return nil
	})
	return bound, err
}

func (s *Storage) putScrubCompleted(bound uint64) error {
	return s.update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(pendingScrubBucket)
		if bkt == nil {
			return ErrBucketMissing
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], bound)
		return bkt.Put(scrubCompletedKey, buf[:])
	})
}

// ResetEventLog discards this node's event log — the content is at an older
// generation than every peer's and is fetched again whole. Readers re-derive
// their cursors by raft index, as across a scrub swap. Refused while a layout
// freeze stands.
func (s *Storage) ResetEventLog() error {
	s.eventRewriteMu.Lock()
	defer s.eventRewriteMu.Unlock()
	s.eventAppendMu.Lock()
	defer s.eventAppendMu.Unlock()
	release, ok := s.eventLayout.move()
	if !ok {
		return ErrLayoutFrozen
	}
	defer release()
	s.eventMu.Lock()
	defer s.eventMu.Unlock()
	if err := s.eventLog.managed.Reset(); err != nil {
		s.scrubGen.Add(1)
		return err
	}
	s.eventIndex.Store(0)
	s.firstEventIndex.Store(0)
	s.scrubGen.Add(1)
	if _, err := s.eventLog.managed.Reclaim(context.Background()); err != nil {
		return err
	}
	s.logger.Warn("event log reset: this node's content was at an older generation than its peers'; it is fetched again whole")
	return nil
}

// Serving bounds per ServeEvents call: a fetch is a sequence of bounded
// exchanges, each under its own layout freeze, so compaction and compression
// on the serving node only ever wait seconds, and an interrupted fetch
// resumes from the last part the receiver kept.
const (
	serveMaxSegments    = 8
	serveMaxRecordBytes = 8 << 20
)

// ServeEvents streams to sink every event with raft index in (after, to]
// that this node holds, as framed records, a bounded amount per call, under
// one layout freeze. Implements db.EventServer.
func (s *Storage) ServeEvents(ctx context.Context, after, to uint64, sink db.EventSink) (db.EventServeResult, error) {
	release := s.FreezeEventLayout()
	defer release()
	return s.serveRecordEvents(ctx, after, to, sink, serveMaxRecordBytes)
}

// AppendFetchedRecords appends a run of records fetched from a peer — the
// existing framed wire encoding, as ServeEvents produced it. Each frame is
// verified and its original protobuf payload kept without re-encoding. Records at
// or below this node's event index are skipped, so overlap at a fetch
// boundary is harmless.
func (s *Storage) AppendFetchedRecords(data []byte) error {
	var received []eventlog.Record
	var bad error
	_, incompleteAt := walkSegmentRecords(data, func(ordinal, off, _ int, rec []byte) bool {
		payload, err := unframe(rec)
		if err != nil {
			bad = fmt.Errorf("fetched record %d at offset %d: %w", ordinal, off, err)
			return false
		}
		ent := &pb.Entry{}
		if err := proto.Unmarshal(payload, ent); err != nil {
			bad = fmt.Errorf("fetched record %d: %w", ordinal, err)
			return false
		}
		received = append(received, eventlog.Record{ID: ent.GetIndex(), Payload: payload})
		return true
	})
	if bad != nil {
		return bad
	}
	if incompleteAt >= 0 {
		return fmt.Errorf("fetched records end inside a record at offset %d", incompleteAt)
	}
	return s.appendPeerRecords(received)
}

// appendPeerRecords appends records fetched from a peer. Overlap and ordering
// follow the existing fetch behavior; generation selection remains the
// catch-up coordinator's job.
func (s *Storage) appendPeerRecords(received []eventlog.Record) error {
	s.eventRewriteMu.Lock()
	defer s.eventRewriteMu.Unlock()
	s.eventAppendMu.Lock()
	defer s.eventAppendMu.Unlock()
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	appender := s.eventAppenderLocked()
	_, hasHistory, err := appender.LastAppended()
	if err != nil {
		return fmt.Errorf("event log last index: %w", err)
	}
	records := make([]eventlog.Record, 0, len(received))
	first, last := uint64(0), uint64(0)
	wasEmpty := !hasHistory
	for _, record := range received {
		if record.ID <= s.eventIndex.Load() || (last != 0 && record.ID <= last) {
			continue
		}
		records = append(records, record)
		if first == 0 {
			first = record.ID
		}
		last = record.ID
	}
	if last == 0 {
		return nil
	}
	if err := appender.Append(records); err != nil {
		return fmt.Errorf("event log write batch (raft indexes %d-%d): %w", first, last, err)
	}
	s.eventLogWriteOps.Add(1)
	if wasEmpty {
		s.firstEventIndex.Store(first)
	}
	s.eventIndex.Store(last)
	return nil
}

// deriveEventBoundsLocked sets firstEventIndex/eventIndex from the log as it
// is, after its files changed under the event lock (an adoption, a reset).
func (s *Storage) deriveEventBoundsLocked() error {
	first, last, err := s.eventBoundsLocked()
	if err != nil {
		return err
	}
	s.eventIndex.Store(last)
	s.firstEventIndex.Store(first)
	return nil
}
