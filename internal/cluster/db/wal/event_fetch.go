package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/wal"
	bolt "go.etcd.io/bbolt"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/datadir"
)

// The event-log fetch: how a node hands its permanent event log to a peer
// that is behind (the serving side, ServeEvents) and how a node takes one in
// (the receiving side, AppendFetchedRecords and AdoptEventSegments). A node
// whose event log is behind the snapshot raft wants to install cannot fill
// the gap from raft — the entries between were compacted — so it fills it
// from a peer instead of exiting (db/catchup.go). Whole sealed segments
// travel as files (plain or .zst, as on disk); the edges travel as records
// in the log's own on-disk encoding, so every byte the receiver writes is
// the byte the peer holds. The receiving side is idempotent by raft index,
// so a retried or overlapping fetch is harmless.
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
type EventSegment struct {
	Path       string // on the serving node, the file to stream; on the receiving node, the staged copy
	FirstSeq   uint64
	Compressed bool
}

// EventLayout is the serving node's event log as of a FreezeLayout: the
// sealed segments, and the tail's first sequence, committed length, and last
// sequence. Valid only while the freeze that produced it stands.
type EventLayout struct {
	Sealed       []EventSegment
	TailPath     string
	TailFirstSeq uint64
	TailLen      int64
	LastSeq      uint64
}

// ErrSegmentsMisaligned refuses an adoption whose first file does not start
// exactly where this node's event log ends: the receiver falls back to
// records for the boundary, or replaces its log wholesale.
var ErrSegmentsMisaligned = errors.New("event segments do not align with this node's event log")

// ErrLayoutFrozen refuses a change to the set of event-log files while a
// layout freeze stands (a peer fetch or live backup is reading them); the
// caller retries once it lifts.
var ErrLayoutFrozen = errors.New("event log layout is frozen for a reader; retry")

// ErrLayoutNotFrozen refuses EventLayout outside a FreezeLayout: a listed
// segment could otherwise vanish (compressed) before it is read.
var ErrLayoutNotFrozen = errors.New("event layout requires a layout freeze (FreezeLayout) for as long as the listed files are read")

// IsCompressedName reports whether a segment file name is a compressed one.
func IsCompressedName(name string) bool { return wal.IsCompressedSegmentPath(name) }

// EventFetchDir is the staging directory a catch-up downloads a peer's
// segment files into before adopting them; Open sweeps it.
func (s *Storage) EventFetchDir() string { return datadir.FetchDir(s.eventLogDir) }

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
	if err := s.putScrubCompleted(gen); err != nil {
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
	release, ok := s.moveLayout()
	if !ok {
		return ErrLayoutFrozen
	}
	defer release()
	s.eventMu.Lock()
	defer s.eventMu.Unlock()
	if err := s.eventLog.Close(); err != nil {
		return fmt.Errorf("close event log for reset: %w", err)
	}
	if err := os.RemoveAll(s.eventLogDir); err != nil {
		s.reopenEventLogAfterSwapOrFatal("event log reset aborted")
		return fmt.Errorf("remove event log for reset: %w", err)
	}
	if err := os.MkdirAll(s.eventLogDir, 0o700); err != nil {
		s.logger.Fatal("event log removed for reset but its directory could not be recreated", zap.Error(err))
	}
	s.reopenEventLogAfterSwapOrFatal("reopen event log after reset")
	s.eventIndex.Store(0)
	s.firstEventIndex.Store(0)
	s.scrubGen.Add(1)
	s.logger.Warn("event log reset: this node's content was at an older generation than its peers'; it is fetched again whole")
	return nil
}

// EventLayout lists this node's event-log files. Valid only under the
// FreezeLayout that must be held when it is called.
func (s *Storage) EventLayout() (EventLayout, error) {
	if !s.layoutFrozen() {
		return EventLayout{}, ErrLayoutNotFrozen
	}
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	lay, err := s.eventLog.LayoutSnapshot()
	if err != nil {
		return EventLayout{}, fmt.Errorf("event log layout: %w", err)
	}
	out := EventLayout{TailPath: lay.Tail.Path, TailFirstSeq: lay.Tail.Index, TailLen: lay.TailLen, LastSeq: lay.LastIndex}
	for _, sg := range lay.Sealed {
		out.Sealed = append(out.Sealed, EventSegment{Path: sg.Path, FirstSeq: sg.Index, Compressed: wal.IsCompressedSegmentPath(sg.Path)})
	}
	return out, nil
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
	first, err := s.firstEventSeqLocked()
	if err != nil {
		return 0, err
	}
	last, err := s.lastEventSeqLocked()
	if err != nil {
		return 0, err
	}
	if first == 0 || last == 0 || last < first {
		return last + 1, nil
	}
	if raftIndex == 0 {
		return first, nil
	}
	lo, hi := first, last+1
	for lo < hi {
		mid := lo + (hi-lo)/2
		bs, err := s.readEventAtLocked(mid)
		if err != nil {
			return 0, fmt.Errorf("event log read seq %d during resolve: %w", mid, err)
		}
		ent := &pb.Entry{}
		if err := proto.Unmarshal(bs, ent); err != nil {
			return 0, err
		}
		if ent.GetIndex() >= raftIndex {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo, nil
}

// ReadEventRaw returns the framed record at seq exactly as stored, after
// verifying its frame — a corrupt record is never served to a peer.
func (s *Storage) ReadEventRaw(seq uint64) ([]byte, error) {
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	raw, err := s.eventLog.Read(seq)
	if err != nil {
		return nil, err
	}
	if _, err := s.unframe(raw, "event_log"); err != nil {
		return nil, err
	}
	return raw, nil
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
// Sealed segments whose records all lie in the range go whole, as files;
// the edges — the segment holding the first wanted record, one extending
// past the last, and the tail — go as records. Implements db.EventServer.
func (s *Storage) ServeEvents(ctx context.Context, after, to uint64, sink db.EventSink) (db.EventServeResult, error) {
	release := s.FreezeLayout()
	defer release()

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
	var prefix [binary.MaxVarintLen64]byte
	for seq := lo; seq <= hi; seq++ {
		raw, err := s.ReadEventRaw(seq)
		if err != nil {
			return nil, 0, fmt.Errorf("event log read seq %d to serve: %w", seq, err)
		}
		n := binary.PutUvarint(prefix[:], uint64(len(raw)))
		data = append(data, prefix[:n]...)
		data = append(data, raw...)
		last = seq
		if len(data) >= maxBytes {
			break
		}
	}
	return data, last, nil
}

// AppendFetchedRecords appends a run of records fetched from a peer — the
// log's on-disk encoding, as ServeEvents produced it — verbatim: each
// frame is verified and the bytes written are the peer's bytes. Records at
// or below this node's event index are skipped, so overlap at a fetch
// boundary is harmless.
func (s *Storage) AppendFetchedRecords(data []byte) error {
	var raws [][]byte
	var indexes []uint64
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
		raws = append(raws, rec)
		indexes = append(indexes, ent.GetIndex())
		return true
	})
	if bad != nil {
		return bad
	}
	if incompleteAt >= 0 {
		return fmt.Errorf("fetched records end inside a record at offset %d", incompleteAt)
	}
	return s.appendRawEvents(raws, indexes)
}

// appendRawEvents is appendEvents for records already framed: the bytes go
// into the log as they are.
func (s *Storage) appendRawEvents(raws [][]byte, indexes []uint64) error {
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()

	nextSeq, err := s.eventLog.LastIndex()
	if err != nil {
		return fmt.Errorf("event log last index: %w", err)
	}
	batch := new(wal.Batch)
	first, last := uint64(0), uint64(0)
	wroteSeqOne := nextSeq == 0
	for i, raw := range raws {
		if indexes[i] <= s.eventIndex.Load() || (last != 0 && indexes[i] <= last) {
			continue
		}
		nextSeq++
		batch.Write(nextSeq, raw)
		if first == 0 {
			first = indexes[i]
		}
		last = indexes[i]
	}
	if last == 0 {
		return nil
	}
	if err := s.eventLog.WriteBatch(batch); err != nil {
		return fmt.Errorf("event log write batch (raft indexes %d-%d): %w", first, last, err)
	}
	s.eventLogWriteOps.Add(1)
	if wroteSeqOne {
		s.firstEventIndex.Store(first)
	}
	s.eventIndex.Store(last)
	return nil
}

// AdoptEventSegments takes whole segment files fetched from a peer into this
// node's event log, in order, consuming them. Each path is a staged copy
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
	if len(paths) == 0 {
		return nil
	}
	files := make([]EventSegment, 0, len(paths))
	for _, p := range paths {
		seq, compressed, err := parseSegmentName(filepath.Base(p))
		if err != nil {
			return err
		}
		if _, err := scanSealedSegment(p, compressed); err != nil {
			return fmt.Errorf("refusing to adopt %s: %w", filepath.Base(p), err)
		}
		files = append(files, EventSegment{Path: p, FirstSeq: seq, Compressed: compressed})
	}
	release, ok := s.moveLayout()
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
	if files[0].FirstSeq != last+1 {
		return fmt.Errorf("%w: this log ends at seq %d, the first file starts at %d", ErrSegmentsMisaligned, last, files[0].FirstSeq)
	}
	for i := 1; i < len(files); i++ {
		if files[i].FirstSeq <= files[i-1].FirstSeq {
			return fmt.Errorf("%w: files out of order at %d", ErrSegmentsMisaligned, i)
		}
	}
	tail, err := s.eventLog.LayoutSnapshot()
	if err != nil {
		return err
	}
	if err := s.eventLog.Close(); err != nil {
		return fmt.Errorf("close event log for adoption: %w", err)
	}
	// An empty tail file bears the name of the next sequence — the very name
	// the first adopted file carries (an empty log's tail is named 1; a log
	// that ended exactly at a segment boundary has an empty tail named
	// last+1). Remove it; a tail with committed bytes stays and becomes a
	// sealed segment, whatever its size.
	if tail.TailLen == 0 {
		if err := os.Remove(tail.Tail.Path); err != nil && !os.IsNotExist(err) {
			s.reopenEventLogAfterSwapOrFatal("adoption aborted before moving files")
			return fmt.Errorf("remove empty tail: %w", err)
		}
	}
	var moved []string
	rollback := func(cause error) error {
		for _, p := range moved {
			_ = os.Remove(p)
		}
		s.reopenEventLogAfterSwapOrFatal("adoption rolled back")
		return cause
	}
	for _, f := range files {
		dst := filepath.Join(s.eventLogDir, filepath.Base(f.Path))
		if err := moveFile(f.Path, dst); err != nil {
			return rollback(fmt.Errorf("adopt %s: %w", filepath.Base(f.Path), err))
		}
		moved = append(moved, dst)
	}
	s.syncDirBestEffort(s.eventLogDir, "event-log adoption")
	reopened, err := wal.Open(s.eventLogDir, s.eventWalOpts)
	if err != nil {
		return rollback(fmt.Errorf("reopen event log over adopted segments: %w", err))
	}
	s.eventLog = reopened
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

// deriveEventBoundsLocked sets firstEventIndex/eventIndex from the log as it
// is, after its files changed under the event lock (an adoption, a reset).
func (s *Storage) deriveEventBoundsLocked() error {
	last, err := s.lastEventSeqLocked()
	if err != nil {
		return err
	}
	if last == 0 {
		s.eventIndex.Store(0)
		s.firstEventIndex.Store(0)
		return nil
	}
	lb, err := s.readEventAtLocked(last)
	if err != nil {
		return err
	}
	le := &pb.Entry{}
	if err := proto.Unmarshal(lb, le); err != nil {
		return err
	}
	s.eventIndex.Store(le.GetIndex())
	first, err := s.firstEventSeqLocked()
	if err != nil {
		return err
	}
	bs, err := s.readEventAtLocked(first)
	if err != nil {
		return err
	}
	fe := &pb.Entry{}
	if err := proto.Unmarshal(bs, fe); err != nil {
		return err
	}
	s.firstEventIndex.Store(fe.GetIndex())
	return nil
}

// moveFile renames src to dst, or copies and removes it when the two are on
// different filesystems. dst must not exist.
func moveFile(src, dst string) error {
	if _, err := os.Lstat(dst); err == nil {
		return fmt.Errorf("%s already exists", filepath.Base(dst))
	}
	if err := os.Rename(src, dst); err == nil {
		return nil
	}
	if err := copyFile(src, dst); err != nil {
		return err
	}
	return os.Remove(src)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src) //nolint:gosec // G304: a staged fetch file this node wrote
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600) //nolint:gosec // G304: a segment name under this node's own events dir
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		_ = os.Remove(dst)
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		_ = os.Remove(dst)
		return err
	}
	return out.Close()
}

// parseSegmentName reads a segment file name: twenty digits naming its first
// sequence, optionally followed by the compressed suffix.
func parseSegmentName(name string) (seq uint64, compressed bool, err error) {
	base := name
	if wal.IsCompressedSegmentPath(base) {
		compressed = true
		base = strings.TrimSuffix(base, ".zst")
	}
	if len(base) != 20 {
		return 0, false, fmt.Errorf("%q is not a segment file name", name)
	}
	seq, err = strconv.ParseUint(base, 10, 64)
	if err != nil || seq == 0 {
		return 0, false, fmt.Errorf("%q is not a segment file name", name)
	}
	return seq, compressed, nil
}

// scanSealedSegment reads a staged segment file end to end: a compressed one
// must decode, and every record must be complete and pass its frame check.
// A sealed segment has no torn tail to forgive. Returns the record count.
func scanSealedSegment(path string, compressed bool) (int, error) {
	data, err := os.ReadFile(path) //nolint:gosec // G304: a staged fetch file this node wrote
	if err != nil {
		return 0, err
	}
	if compressed {
		if data, err = decodeZstd(data); err != nil {
			return 0, fmt.Errorf("compressed segment fails its zstd frame: %w", err)
		}
	}
	var bad error
	records, incompleteAt := walkSegmentRecords(data, func(ordinal, off, _ int, rec []byte) bool {
		if _, uerr := unframe(rec); uerr != nil {
			bad = fmt.Errorf("record %d at offset %d: %w", ordinal, off, uerr)
			return false
		}
		return true
	})
	if bad != nil {
		return records, bad
	}
	if incompleteAt >= 0 {
		return records, fmt.Errorf("incomplete record at offset %d", incompleteAt)
	}
	if records == 0 {
		return 0, errors.New("empty segment")
	}
	return records, nil
}
