package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/tidwall/wal"
	bolt "go.etcd.io/bbolt"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/datadir"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
)

// The event-log fetch: how a node hands its permanent event log to a peer
// that is behind (the serving side, ServeEvents) and how a node takes one in
// (the receiving side, AppendFetchedRecords and AdoptEventSegments). A node
// whose event log is behind the snapshot raft wants to install cannot fill
// the gap from raft — the entries between were compacted — so it fills it
// from a peer instead of exiting (db/catchup.go). Whole sealed segments
// travel as files for native tidwall (plain or .zst, as on disk); the edges
// and shared-backend records use the existing framed-record wire encoding.
// Protobuf payloads retain the peer's exact bytes. The receiving side is
// idempotent by raft index, so a retried or overlapping fetch is harmless.
//
// A log's GENERATION is the scrub bound its bytes reflect (EventLogGeneration).
// Every replica's rewrite is deterministic, so two logs at one generation
// are byte-identical over their shared prefix and sequence-aligned; one can
// extend the other file by file. The receiver never mixes generations —
// db/catchup.go pins one per fetch and discards its log when a peer's is
// newer — because a log stitched from two could hold an entity's raw upsert
// in one part and its hashed delete in the other, which no later scrub
// could pair up.

// EventSegment is one event-log segment file as a peer would ship it.
type EventSegment = tidwall.LegacySegment

// EventLayout is the serving node's event log as of a FreezeLayout: the
// sealed segments, and the tail's first sequence, committed length, and last
// sequence. Valid only while the freeze that produced it stands.
type EventLayout = tidwall.LegacyLayout

// ErrSegmentsMisaligned refuses an adoption whose first file does not start
// exactly where this node's event log ends: the receiver (db/catchup.go)
// discards its log and fetches it whole.
var ErrSegmentsMisaligned = db.ErrEventLogMisaligned

// ErrLayoutFrozen refuses a change to the set of event-log files while an
// event-log layout freeze stands (a peer fetch or live backup is reading them); the
// caller retries once it lifts.
var ErrLayoutFrozen = errors.New("event log layout is frozen for a reader; retry")

// ErrLayoutNotFrozen refuses EventLayout outside a FreezeEventLayout: a listed
// segment could otherwise vanish (compressed) before it is read.
var ErrLayoutNotFrozen = errors.New("event layout requires an event-log layout freeze (FreezeEventLayout) for as long as the listed files are read")

// IsCompressedName reports whether a segment file name is a compressed one.
func IsCompressedName(name string) bool { return wal.IsCompressedSegmentPath(name) }

// EventFetchDir is the staging directory a catch-up downloads a peer's
// segment files into before adopting them; Open sweeps it.
func (s *Storage) EventFetchDir() string { return datadir.FetchDir(s.eventLogDir) }

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
	s.eventAppendMu.Lock()
	defer s.eventAppendMu.Unlock()
	release, ok := s.eventLayout.move()
	if !ok {
		return ErrLayoutFrozen
	}
	defer release()
	s.eventMu.Lock()
	defer s.eventMu.Unlock()
	if s.eventLog.managed != nil {
		if err := s.eventLog.managed.Reset(); err != nil {
			s.scrubGen.Add(1)
			return err
		}
	} else {
		if err := s.eventLog.Close(); err != nil {
			return fmt.Errorf("close event log for reset: %w", err)
		}
		if err := tidwall.ResetLegacyDirectory(s.eventLogDir); err != nil {
			var resetErr *tidwall.LegacyResetError
			if errors.As(err, &resetErr) && resetErr.Removed {
				s.logger.Fatal("event log removed for reset but its directory could not be recreated", zap.Error(resetErr.Cause))
			}
			s.reopenEventLogAfterSwapOrFatal("event log reset aborted")
			return err
		}
		s.reopenEventLogAfterSwapOrFatal("reopen event log after reset")
	}
	s.eventIndex.Store(0)
	s.firstEventIndex.Store(0)
	s.scrubGen.Add(1)
	if s.eventLog.managed != nil {
		if _, err := s.eventLog.managed.Reclaim(context.Background()); err != nil {
			return err
		}
	}
	s.logger.Warn("event log reset: this node's content was at an older generation than its peers'; it is fetched again whole")
	return nil
}

// EventLayout lists this node's event-log files. Valid only under the
// FreezeEventLayout that must be held when it is called.
func (s *Storage) EventLayout() (EventLayout, error) {
	if !s.eventLayout.frozen() {
		return EventLayout{}, ErrLayoutNotFrozen
	}
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	if err := s.requireNativeEventLogLocked(); err != nil {
		return EventLayout{}, err
	}
	layout, err := s.nativeEventTransferLocked().Layout()
	if err != nil {
		return EventLayout{}, fmt.Errorf("event log layout: %w", err)
	}
	return layout, nil
}

// EventSeqForIndex returns the sequence of the first event whose raft index
// is at or past raftIndex (last+1 when none is). Raft indexes are strictly
// increasing along the sequence, so this is a binary search.
func (s *Storage) EventSeqForIndex(raftIndex uint64) (uint64, error) {
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	return s.eventSeqForIndexLocked(raftIndex)
}

func (s *Storage) eventSeqForIndexLocked(raftIndex uint64) (uint64, error) {
	if err := s.requireNativeEventLogLocked(); err != nil {
		return 0, err
	}
	positioner := newLegacyPositioner(s)
	defer func() { _ = positioner.Close() }()
	return positioner.SequenceFor(raftIndex)
}

// ReadEventRaw returns the framed record at seq exactly as stored, after
// verifying its frame — a corrupt record is never served to a peer.
func (s *Storage) ReadEventRaw(seq uint64) ([]byte, error) {
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	if err := s.requireNativeEventLogLocked(); err != nil {
		return nil, err
	}
	return s.nativeEventTransferLocked().Read(seq)
}

// EventRaftIndexAt returns the raft index of the event at seq.
func (s *Storage) EventRaftIndexAt(seq uint64) (uint64, error) {
	bs, err := s.readEventAt(seq)
	if err != nil {
		return 0, err
	}
	ent := &pb.Entry{}
	if err := proto.Unmarshal(bs, ent); err != nil {
		return 0, err
	}
	return ent.GetIndex(), nil
}

// LastEventSeq is the sequence of the last event this node holds (0 when none).
func (s *Storage) LastEventSeq() (uint64, error) { return s.lastEventSeq() }

// Serving bounds per ServeEvents call: a fetch is a sequence of bounded
// exchanges, each under its own layout freeze, so compaction and compression
// on the serving node only ever wait seconds, and an interrupted fetch
// resumes from the last part the receiver kept.
const (
	serveMaxSegments    = 8
	serveMaxRecordBytes = 8 << 20
)

// ServeEvents streams to sink every event with raft index in (after, to]
// that this node holds, a bounded amount per call, under one layout freeze.
// Native sealed segments wholly inside the range go as files; partial
// segments and the tail go as framed records. Shared backends send framed
// records throughout. Implements db.EventServer.
func (s *Storage) ServeEvents(ctx context.Context, after, to uint64, sink db.EventSink) (db.EventServeResult, error) {
	release := s.FreezeEventLayout()
	defer release()

	s.eventMu.RLock()
	shared := s.eventLog.managed != nil
	s.eventMu.RUnlock()
	if shared {
		return s.serveRecordEvents(ctx, after, to, sink, serveMaxRecordBytes)
	}

	res := db.EventServeResult{Generation: s.EventLogGeneration(), EventIndex: s.eventIndex.Load()}
	if err := sink.Begin(res.Generation, res.EventIndex); err != nil {
		return res, err
	}
	to = min(to, res.EventIndex)
	if after >= to {
		return res, sink.End(res)
	}
	lay, err := s.EventLayout()
	if err != nil {
		return res, err
	}
	startSeq, err := s.EventSeqForIndex(after + 1)
	if err != nil {
		return res, err
	}
	endSeq, err := s.EventSeqForIndex(to + 1)
	if err != nil {
		return res, err
	}
	endSeq = min(endSeq-1, lay.LastSeq)
	if startSeq > endSeq {
		return res, sink.End(res)
	}

	served := startSeq - 1
	segments, recordBytes := 0, 0
	sendRecords := func(lo, hi uint64) error {
		data, last, err := s.encodeRecords(lo, hi, serveMaxRecordBytes-recordBytes)
		if err != nil {
			return err
		}
		if err := sink.Records(data); err != nil {
			return err
		}
		served = last
		recordBytes += len(data)
		return nil
	}
	budget := func() bool { return segments < serveMaxSegments && recordBytes < serveMaxRecordBytes }

	for i, sg := range lay.Sealed {
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		segEnd := lay.TailFirstSeq - 1
		if i+1 < len(lay.Sealed) {
			segEnd = lay.Sealed[i+1].FirstSeq - 1
		}
		if segEnd < startSeq {
			continue
		}
		if sg.FirstSeq > endSeq || !budget() {
			break
		}
		if sg.FirstSeq >= startSeq && segEnd <= endSeq {
			if err := s.sendSegment(sink, sg); err != nil {
				return res, err
			}
			served = segEnd
			segments++
			continue
		}
		if err := sendRecords(max(sg.FirstSeq, startSeq), min(segEnd, endSeq)); err != nil {
			return res, err
		}
	}
	if served < endSeq && budget() && served+1 >= lay.TailFirstSeq {
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		if err := sendRecords(served+1, endSeq); err != nil {
			return res, err
		}
	}
	if served >= startSeq {
		if res.LastIndex, err = s.EventRaftIndexAt(served); err != nil {
			return res, err
		}
	}
	res.More = served < endSeq
	return res, sink.End(res)
}

func (s *Storage) sendSegment(sink db.EventSink, sg EventSegment) error {
	f, err := os.Open(sg.Path) //nolint:gosec // G304: a segment file of this node's own event log, listed under a freeze
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	return sink.Segment(filepath.Base(sg.Path), info.Size(), f)
}

// encodeRecords reads the records at lo..hi and returns them in the log's
// on-disk encoding, stopping early once maxBytes is reached; last is the
// sequence of the last record included.
func (s *Storage) encodeRecords(lo, hi uint64, maxBytes int) (data []byte, last uint64, err error) {
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	if err := s.requireNativeEventLogLocked(); err != nil {
		return nil, 0, err
	}
	return s.nativeEventTransferLocked().EncodeRecords(lo, hi, maxBytes)
}

// AppendFetchedRecords appends a run of records fetched from a peer — the
// existing framed wire encoding, as ServeEvents produced it. Each frame is
// verified; native storage retains the frame and shared storage retains its
// original protobuf payload without re-encoding. Records at
// or below this node's event index are skipped, so overlap at a fetch
// boundary is harmless.
func (s *Storage) AppendFetchedRecords(data []byte) error {
	var received []fetchedRecord
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
		received = append(received, fetchedRecord{frame: rec, Record: eventlog.Record{ID: ent.GetIndex(), Payload: payload}})
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

// fetchedRecord retains both wire framing and validated logical identity. Both
// slices refer to the incoming batch; receiving adds no payload copy or decode.
type fetchedRecord struct {
	frame []byte
	eventlog.Record
}

// appendPeerRecords preserves native frames for the legacy owner and writes
// logical records to shared backends. Overlap and ordering follow the existing
// fetch behavior; generation selection remains the catch-up coordinator's job.
func (s *Storage) appendPeerRecords(received []fetchedRecord) error {
	s.eventAppendMu.Lock()
	defer s.eventAppendMu.Unlock()
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	appender := s.eventAppenderLocked()
	native := s.eventLog.native != nil
	if native {
		appender = s.fetchedEventAppenderLocked()
	}

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
		payload := record.Payload
		if native {
			payload = record.frame
		}
		records = append(records, eventlog.Record{ID: record.ID, Payload: payload})
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

// AdoptEventSegments takes whole segment files fetched from a peer into this
// node's event log, in order, consuming them. Shared backends validate and
// import one complete file at a time as logical records; an error can leave
// an imported prefix, which the existing record overlap handling makes retryable.
// Native receivers install the files directly as described below.
// Each path is a staged copy
// whose base name is the segment's own (its first sequence, plus .zst for a
// compressed one); the name is the only thing the caller asserts, and every
// file is scanned completely — every record framed and valid, a compressed
// file decoding — before anything moves, because it just arrived over a
// network. The first file must start exactly at this node's next sequence
// (an empty log adopts from sequence 1) or ErrSegmentsMisaligned and nothing
// changes. The files are moved into place (a copy when the staging dir is on
// another filesystem) and the log reopened over them under the event lock;
// a reopen or boundary read that fails removes them and reopens the log as
// it was. A compressed last file is fine: the log starts a fresh plain tail
// past it. Refused while a layout freeze stands.
func (s *Storage) AdoptEventSegments(paths []string) error {
	s.eventMu.RLock()
	shared := s.eventLog.managed != nil
	s.eventMu.RUnlock()
	if shared {
		return s.importPeerSegments(paths)
	}
	if len(paths) == 0 {
		return nil
	}
	files := make([]EventSegment, 0, len(paths))
	for _, p := range paths {
		segment, err := tidwall.InspectLegacySegment(p, func(raw []byte) error {
			_, err := unframe(raw)
			return err
		})
		if err != nil {
			return err
		}
		files = append(files, segment)
	}
	release, ok := s.eventLayout.move()
	if !ok {
		return ErrLayoutFrozen
	}
	defer release()
	s.eventMu.Lock()
	defer s.eventMu.Unlock()

	last, err := s.lastEventSeqLocked()
	if err != nil {
		return err
	}
	if err := tidwall.CheckLegacyAdoption(last, files); err != nil {
		return fmt.Errorf("%w: %v", ErrSegmentsMisaligned, err)
	}
	tail, err := s.nativeEventTransferLocked().Layout()
	if err != nil {
		return err
	}
	if err := s.eventLog.Close(); err != nil {
		return fmt.Errorf("close event log for adoption: %w", err)
	}
	attempt, installErr := tidwall.InstallLegacySegments(s.eventLogDir, tail, files)
	rollback := func(cause error) error {
		_ = attempt.Rollback()
		s.reopenEventLogAfterSwapOrFatal("adoption rolled back")
		return cause
	}
	if installErr != nil {
		return rollback(installErr)
	}
	s.syncDirBestEffort(s.eventLogDir, "event-log adoption")
	reopened, err := tidwall.OpenLegacy(s.eventLogDir, s.eventOpenOptions)
	if err != nil {
		return rollback(fmt.Errorf("reopen event log over adopted segments: %w", err))
	}
	s.eventLog = bindLegacyEventLog(reopened, s.metrics)
	// The adopted files must be readable at their boundaries; a file the
	// log cannot read at its first sequence is not one to keep.
	for _, f := range files {
		if _, err := s.readEventAtLocked(f.FirstSeq); err != nil {
			_ = s.eventLog.Close()
			return rollback(fmt.Errorf("adopted %s unreadable at seq %d: %w", filepath.Base(f.Path), f.FirstSeq, err))
		}
	}
	if err := s.deriveEventBoundsLocked(); err != nil {
		_ = s.eventLog.Close()
		return rollback(err)
	}
	s.logger.Info("adopted event-log segments from a peer",
		zap.Int("segments", len(files)), zap.Uint64("fromSeq", files[0].FirstSeq), zap.Uint64("eventIndex", s.eventIndex.Load()))
	return nil
}

// importPeerSegments keeps native filename, compression, and record-boundary
// interpretation in tidwall. WAL owns frame/protobuf validation and logical
// append ordering, just as it does for a Records part of the same peer stream.
func (s *Storage) importPeerSegments(paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	release, ok := s.eventLayout.move()
	if !ok {
		return ErrLayoutFrozen
	}
	defer release()
	for _, path := range paths {
		var records []fetchedRecord
		_, err := tidwall.InspectLegacySegment(path, func(raw []byte) error {
			payload, err := unframe(raw)
			if err != nil {
				return err
			}
			entry := new(pb.Entry)
			if err := proto.Unmarshal(payload, entry); err != nil {
				return err
			}
			records = append(records, fetchedRecord{Record: eventlog.Record{ID: entry.GetIndex(), Payload: payload}})
			return nil
		})
		if err != nil {
			return err
		}
		if err := s.appendPeerRecords(records); err != nil {
			return err
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("remove imported peer segment: %w", err)
		}
	}
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
