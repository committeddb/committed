package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/tidwall/wal"
)

// The event-log fetch: the primitives a node uses to hand its permanent
// event log to a peer that is behind (the serving side), and to take one in
// (the receiving side). A node whose event log is behind the snapshot raft
// wants to install cannot fill the gap from raft — the entries between were
// compacted — so it fills it from a peer instead of exiting. Whole sealed
// segments travel as files (plain or .zst, as on disk); the edges travel as
// framed records by sequence. Everything here is idempotent by raft index on
// the receiving side, so a retried or overlapping fetch is harmless.
//
// Serving side: hold FreezeLayout for the whole exchange, then EventLayout,
// EventSeqForIndex, ReadEventRaw. Receiving side: AdoptEventSegments for
// whole files, AppendFetchedEvents for records.

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

// IsCompressedName reports whether a segment file name is a compressed one.
func IsCompressedName(name string) bool { return wal.IsCompressedSegmentPath(name) }

// ErrLayoutNotFrozen refuses EventLayout outside a FreezeLayout: a listed
// segment could otherwise vanish (compressed) before it is read.
var ErrLayoutNotFrozen = errors.New("event layout requires a layout freeze (FreezeLayout) for as long as the listed files are read")

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

// AppendFetchedEvents appends framed records fetched from a peer, in order.
// Each frame is verified; records at or below this node's event index are
// skipped, so overlap at a fetch boundary is harmless.
func (s *Storage) AppendFetchedEvents(raws [][]byte) error {
	entries := make([]*pb.Entry, 0, len(raws))
	for i, raw := range raws {
		payload, err := unframe(raw)
		if err != nil {
			return fmt.Errorf("fetched event %d: %w", i, err)
		}
		ent := &pb.Entry{}
		if err := proto.Unmarshal(payload, ent); err != nil {
			return fmt.Errorf("fetched event %d: %w", i, err)
		}
		entries = append(entries, ent)
	}
	return s.appendEvents(entries)
}

// AdoptEventSegments takes whole segment files fetched from a peer into this
// node's event log, in order. Each path is a staged copy whose base name is
// the segment's own (its first sequence, plus .zst for a compressed one);
// the name is the only thing the caller asserts, and every file is scanned
// completely — every record framed and valid, a compressed file decoding —
// before anything is copied, because it just arrived over a network. The
// first file must start exactly at this node's next sequence (an empty log
// adopts from sequence 1) or ErrSegmentsMisaligned and nothing changes. The
// log is reopened over the adopted files under the event lock; a reopen or
// boundary read that fails removes them and reopens the log as it was. A
// compressed last file is fine: the log starts a fresh plain tail past it.
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
			s.reopenEventLogAfterSwapOrFatal("adoption aborted before copying")
			return fmt.Errorf("remove empty tail: %w", err)
		}
	}
	var copied []string
	rollback := func(cause error) error {
		for _, p := range copied {
			_ = os.Remove(p)
		}
		s.reopenEventLogAfterSwapOrFatal("adoption rolled back")
		return cause
	}
	for _, f := range files {
		dst := filepath.Join(s.eventLogDir, filepath.Base(f.Path))
		if err := copyFile(f.Path, dst); err != nil {
			return rollback(fmt.Errorf("adopt %s: %w", filepath.Base(f.Path), err))
		}
		copied = append(copied, dst)
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
	if err := s.refreshEventBoundsLocked(); err != nil {
		_ = s.eventLog.Close()
		return rollback(err)
	}
	s.logger.Info("adopted event-log segments from a peer",
		zap.Int("segments", len(files)), zap.Uint64("fromSeq", files[0].FirstSeq), zap.Uint64("eventIndex", s.eventIndex.Load()))
	return nil
}

// refreshEventBoundsLocked re-reads firstEventIndex/eventIndex from the log
// after its files changed under the event lock.
func (s *Storage) refreshEventBoundsLocked() error {
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
