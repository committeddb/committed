package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/klauspost/compress/zstd"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/backup"
	"github.com/committeddb/committed/internal/cluster/db/datadir"
	"github.com/committeddb/committed/internal/cluster/fsutil"
)

// Backup splice: repair mid-log corruption from a backup of the same node.
//
// A bit-flip in a committed record is not a torn tail — the bytes were
// acknowledged and every replica holds them — so the offline scan refuses to
// truncate it. On a cluster the node rebuilds from a peer; on a single node
// the only other source of the correct bytes is a backup. The record at a
// given log sequence is byte-identical everywhere, so a backup that covers it
// can supply it, provided the two logs are provably the same log at the same
// alignment. Two corruption shapes, two repairs:
//
//   - a PLAIN segment holding a complete record whose frame fails: splice that
//     one record's bytes from the backup's covering segment (record splice);
//   - a COMPRESSED segment whose zstd frame fails (one flipped byte poisons the
//     whole frame, so the record cannot be located): replace the whole segment
//     with the backup's copy, always written back as .zst so the log never
//     holds a plain+zst pair for one index — the fork's crash healing treats
//     the .zst as the truth and would discard a plain replacement.
//
// Nothing is written until every verification passes (see verify* below), and
// each write is atomic: assemble in memory, re-scan, write a temp file, fsync,
// rename over the corrupt file, fsync the directory. A crash before the rename
// leaves the log exactly as it was.

// SpliceReport is the outcome for one log directory.
type SpliceReport struct {
	Dir     string
	Before  *Diagnosis // the scan that classified the log
	Plan    string     // what the splice would do (dry run) or did (commit); "" when nothing applies
	Refused string     // why the splice will not touch this log; "" when planned or applied
	Applied bool
	After   *Diagnosis // the re-scan after applying; nil unless Applied
}

// recordSpan locates one record inside a plain segment's bytes.
type recordSpan struct {
	off    int // offset of the uvarint size prefix
	prefix int // prefix length
	size   int // framed record length after the prefix
}

func (r recordSpan) end() int { return r.off + r.prefix + r.size }

// parseSpans walks a plain segment's records structurally (size prefixes
// only). It returns the spans up to the first structurally incomplete record,
// and whether the whole buffer was consumed cleanly.
func parseSpans(data []byte) (spans []recordSpan, clean bool) {
	off := 0
	for off < len(data) {
		size, n := binary.Uvarint(data[off:])
		remaining := len(data) - off - n
		if n <= 0 || uint64(remaining) < size { //nolint:gosec // G115: remaining >= 0 when n > 0
			return spans, false
		}
		s := recordSpan{off: off, prefix: n, size: int(size)} //nolint:gosec // G115: bounded by the remaining buffer
		spans = append(spans, s)
		off = s.end()
	}
	return spans, true
}

// framedValid reports whether the record's frame (magic, version, CRC) holds.
func framedValid(data []byte, s recordSpan) bool {
	_, err := unframe(data[s.off+s.prefix : s.end()])
	return err == nil
}

// parseClean parses a segment and requires every record complete and framed.
func parseClean(data []byte) ([]recordSpan, error) {
	spans, clean := parseSpans(data)
	if !clean {
		return nil, errors.New("a structurally incomplete record")
	}
	for i, s := range spans {
		if !framedValid(data, s) {
			return nil, fmt.Errorf("record %d fails its frame checksum", i)
		}
	}
	return spans, nil
}

// segmentIndexOf parses a segment file name (plain or .zst) to its start index.
func segmentIndexOf(name string) (uint64, bool) {
	if len(name) != 20 && (len(name) != 24 || !strings.HasSuffix(name, ".zst")) {
		return 0, false
	}
	idx, err := strconv.ParseUint(name[:20], 10, 64)
	if err != nil || idx == 0 {
		return 0, false
	}
	return idx, true
}

// entryLogKind says how a log's record payloads relate to raft indices:
// raft/log holds every entry (contiguous), events holds committed entries in
// order (strictly increasing), raft/state holds no entries (no check).
type entryLogKind int

const (
	entriesNone entryLogKind = iota
	entriesContiguous
	entriesIncreasing
)

func entryKindOf(rel string) entryLogKind {
	switch rel {
	case "raft/log":
		return entriesContiguous
	case "events":
		return entriesIncreasing
	}
	return entriesNone
}

// entryIndex decodes a valid record's payload as a raft entry and returns its
// index. ok is false when the frame or the entry does not decode.
func entryIndex(data []byte, s recordSpan) (uint64, bool) {
	payload, err := unframe(data[s.off+s.prefix : s.end()])
	if err != nil {
		return 0, false
	}
	e := &pb.Entry{}
	if err := proto.Unmarshal(payload, e); err != nil {
		return 0, false
	}
	return e.GetIndex(), true
}

// continues reports whether next follows prev under the log's rule.
func continues(kind entryLogKind, prev, next uint64) bool {
	switch kind {
	case entriesContiguous:
		return next == prev+1
	case entriesIncreasing:
		return next > prev
	}
	return true
}

var zstdSpliceEnc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))

func encodeZstd(raw []byte) []byte {
	return zstdSpliceEnc.EncodeAll(raw, make([]byte, 0, len(raw)/4))
}

// spliceTarget is one corrupt log the splice will try to repair.
type spliceTarget struct {
	rel string // data-dir-relative log dir, forward-slash ("events", "raft/log")
	dir string
	d   *Diagnosis
	rep *SpliceReport
	seq uint64 // record splice: the corrupt record's sequence; segment: its start
}

const repairTmpSuffix = ".repair.tmp"

// SpliceNode diagnoses each of a STOPPED node's logs and, for mid-log
// corruption a backup of the same node covers, plans (and with commit,
// applies) the repair described above. Every log gets a report; a refusal
// names its reason and leaves the log untouched. Torn tails are not handled
// here — a report's Before/After status shows one, and the plain
// `wal repair --commit` truncates it.
func SpliceNode(baseDir string, archive io.Reader, commit bool) ([]*SpliceReport, error) {
	lock, err := datadir.LockStoppedNodeExclusive(baseDir)
	if err != nil {
		return nil, err
	}
	if lock != nil {
		defer func() { _ = lock.Close() }()
	}
	var reports []*SpliceReport
	var targets []*spliceTarget
	for _, parts := range walLogSubdirs {
		dir := filepath.Join(append([]string{baseDir}, parts...)...)
		removeStaleRepairTemps(dir)
		d, err := DiagnoseLog(dir)
		if err != nil {
			return reports, fmt.Errorf("%s: %w", dir, err)
		}
		rep := &SpliceReport{Dir: dir, Before: d}
		reports = append(reports, rep)
		if d.Status != LogCorrupt {
			continue
		}
		if d.corruptShape == corruptNone {
			rep.Refused = "not a shape a backup can repair (ambiguous framing or a mid-compaction directory): rebuild"
			continue
		}
		seg := d.segs[d.corruptSeg]
		t := &spliceTarget{rel: strings.Join(parts, "/"), dir: dir, d: d, rep: rep, seq: seg.index}
		if d.corruptShape == corruptRecord {
			t.seq = seg.index + uint64(d.corruptOrdinal) //nolint:gosec // G115: an ordinal within one segment
		}
		targets = append(targets, t)
	}
	if len(targets) == 0 {
		return reports, nil
	}

	// One pass over the archive: per corrupt log, keep the greatest backup
	// segment at or below the target sequence (archive order is ascending).
	byRel := make(map[string]*spliceTarget, len(targets))
	for _, t := range targets {
		byRel[t.rel] = t
	}
	keep := func(name string) (string, bool) {
		dir, file := path.Split(name)
		dir = strings.TrimSuffix(dir, "/")
		t, ok := byRel[dir]
		if !ok {
			return "", false
		}
		idx, ok := segmentIndexOf(file)
		if !ok || idx > t.seq {
			return "", false
		}
		return dir, true
	}
	kept, _, err := backup.Extract(archive, keep)
	if err != nil {
		return reports, fmt.Errorf("wal repair --from: %w", err)
	}

	for _, t := range targets {
		x, ok := kept[t.rel]
		if !ok {
			t.rep.Refused = fmt.Sprintf("the backup holds no %s segment at or below sequence %d — it predates the corrupt record; rebuild", t.rel, t.seq)
			continue
		}
		bdata := x.Data
		if strings.HasSuffix(x.Name, ".zst") {
			if bdata, err = decodeZstd(bdata); err != nil {
				t.rep.Refused = fmt.Sprintf("backup segment %s fails its zstd frame checksum — the backup itself is damaged", x.Name)
				continue
			}
		}
		bstart, _ := segmentIndexOf(path.Base(x.Name))
		brecs, err := parseClean(bdata)
		if err != nil {
			t.rep.Refused = fmt.Sprintf("backup segment %s is not clean (%v) — the backup itself is damaged", x.Name, err)
			continue
		}
		var repaired []byte
		var target string
		switch t.d.corruptShape {
		case corruptRecord:
			repaired, target, err = planRecordSplice(t, x.Name, bdata, bstart, brecs)
		case corruptSegment:
			repaired, target, err = planSegmentReplace(t, x.Name, x.Data, bdata, bstart, brecs)
		}
		if err != nil {
			t.rep.Refused = err.Error()
			continue
		}
		if !commit {
			continue
		}
		if err := writeAtomic(target, repaired); err != nil {
			return reports, fmt.Errorf("%s: %w", t.dir, err)
		}
		t.rep.Applied = true
		after, err := DiagnoseLog(t.dir)
		if err != nil {
			return reports, fmt.Errorf("%s: re-scan after splice: %w", t.dir, err)
		}
		t.rep.After = after
	}
	return reports, nil
}

// planRecordSplice verifies and assembles a record splice into a plain
// segment. It returns the repaired segment bytes and the file to replace.
func planRecordSplice(t *spliceTarget, bname string, bdata []byte, bstart uint64, brecs []recordSpan) ([]byte, string, error) {
	seg := t.d.segs[t.d.corruptSeg]
	local, err := os.ReadFile(seg.path)
	if err != nil {
		return nil, "", err
	}
	lrecs, _ := parseSpans(local) // the corrupt record is structurally complete; a torn tail past it is not this repair's concern
	if t.d.corruptOrdinal >= len(lrecs) {
		return nil, "", fmt.Errorf("segment %s changed since the scan", seg.name)
	}
	lrec := lrecs[t.d.corruptOrdinal]
	bend := bstart + uint64(len(brecs))
	if t.seq < bstart || t.seq >= bend {
		return nil, "", fmt.Errorf("backup segment %s covers sequences %d–%d, not the corrupt record at %d — the backup predates it; rebuild", bname, bstart, bend-1, t.seq)
	}
	brec := brecs[t.seq-bstart]
	if brec.size != lrec.size {
		return nil, "", fmt.Errorf("backup record at sequence %d is %d bytes, the corrupt record is %d — not the same log (or a corrupt length prefix); refusing", t.seq, brec.size, lrec.size)
	}
	// Alignment: every other record the two segments both hold must be
	// byte-identical. This is what proves "the same log at the same
	// alignment" — and what keeps a backup taken BEFORE a scrub or a
	// truncation from re-introducing bytes the log has since rewritten (an
	// RTBF erasure, above all): any difference refuses the splice.
	lstart := seg.index
	lo, hi := max(lstart, bstart), min(lstart+uint64(len(lrecs)), bend)
	compared := 0
	for s := lo; s < hi; s++ {
		if s == t.seq {
			continue
		}
		l, b := lrecs[s-lstart], brecs[s-bstart]
		if !bytes.Equal(local[l.off:l.end()], bdata[b.off:b.end()]) {
			return nil, "", fmt.Errorf("record at sequence %d differs between the log and the backup — the log was rewritten (a scrub or a truncation) after the backup, so its bytes are not the log's current truth; refusing", s)
		}
		compared++
	}
	if compared == 0 && len(lrecs) > 1 {
		return nil, "", fmt.Errorf("the backup and the log share no other record to align on; refusing")
	}
	if kind := entryKindOf(t.rel); kind != entriesNone {
		bidx, ok := entryIndex(bdata, brec)
		if !ok {
			return nil, "", fmt.Errorf("backup record at sequence %d does not decode as a raft entry; refusing", t.seq)
		}
		if o := t.d.corruptOrdinal; o > 0 {
			if pidx, ok := entryIndex(local, lrecs[o-1]); ok && !continues(kind, pidx, bidx) {
				return nil, "", fmt.Errorf("backup record carries raft index %d, which does not continue the preceding record's %d; refusing", bidx, pidx)
			}
		}
		if o := t.d.corruptOrdinal; o+1 < len(lrecs) {
			if nidx, ok := entryIndex(local, lrecs[o+1]); ok && !continues(kind, bidx, nidx) {
				return nil, "", fmt.Errorf("backup record carries raft index %d, which the following record's %d does not continue; refusing", bidx, nidx)
			}
		}
	}
	repaired := make([]byte, 0, len(local))
	repaired = append(repaired, local[:lrec.off]...)
	repaired = append(repaired, bdata[brec.off:brec.end()]...)
	repaired = append(repaired, local[lrec.end():]...)
	spans, clean := parseSpans(repaired)
	if len(spans) != len(lrecs) || (clean != isCleanPrefix(local, lrecs)) {
		return nil, "", errors.New("assembled segment does not re-scan to the same record set; refusing")
	}
	for i := range spans {
		if i <= t.d.corruptOrdinal && !framedValid(repaired, spans[i]) {
			return nil, "", errors.New("assembled segment still fails its frame at the spliced record; refusing")
		}
	}
	t.rep.Plan = fmt.Sprintf("splice record at sequence %d (segment %s, offset %d, %d bytes) from backup segment %s", t.seq, seg.name, lrec.off, lrec.size, bname)
	return repaired, seg.path, nil
}

// isCleanPrefix reports whether local's spans consumed the whole buffer.
func isCleanPrefix(local []byte, spans []recordSpan) bool {
	return len(spans) > 0 && spans[len(spans)-1].end() == len(local)
}

// planSegmentReplace verifies a whole-segment replacement for a corrupt
// compressed segment and returns the .zst bytes to write.
func planSegmentReplace(t *spliceTarget, bname string, braw, bdata []byte, bstart uint64, brecs []recordSpan) ([]byte, string, error) {
	seg := t.d.segs[t.d.corruptSeg]
	if bstart != seg.index {
		return nil, "", fmt.Errorf("the backup has no copy of segment %s (its nearest is %s); rebuild", seg.name, bname)
	}
	si := t.d.corruptSeg
	if si+1 >= len(t.d.segs) {
		return nil, "", fmt.Errorf("segment %s is the log's last segment, so its completeness cannot be verified against a successor; refusing", seg.name)
	}
	next := t.d.segs[si+1]
	if want := next.index - seg.index; uint64(len(brecs)) != want {
		return nil, "", fmt.Errorf("backup segment %s holds %d records but the log expects %d (up to the next segment %s) — the backup predates the segment's completion; rebuild", bname, len(brecs), want, next.name)
	}
	if kind := entryKindOf(t.rel); kind != entriesNone {
		first, ok := entryIndex(bdata, brecs[0])
		if !ok {
			return nil, "", errors.New("backup segment's first record does not decode as a raft entry; refusing")
		}
		last, _ := entryIndex(bdata, brecs[len(brecs)-1])
		if si > 0 {
			if pidx, ok := lastEntryIndex(t.d.segs[si-1]); ok && !continues(kind, pidx, first) {
				return nil, "", fmt.Errorf("backup segment starts at raft index %d, which does not continue the preceding segment's last %d; refusing", first, pidx)
			}
		}
		if nidx, ok := firstEntryIndex(next); ok && !continues(kind, last, nidx) {
			return nil, "", fmt.Errorf("backup segment ends at raft index %d, which the next segment's first %d does not continue; refusing", last, nidx)
		}
	}
	out := braw
	if !strings.HasSuffix(bname, ".zst") {
		out = encodeZstd(bdata)
	}
	check, err := decodeZstd(out)
	if err != nil || !bytes.Equal(check, bdata) {
		return nil, "", errors.New("assembled compressed segment does not decode back to the verified bytes; refusing")
	}
	t.rep.Plan = fmt.Sprintf("replace compressed segment %s (%d records) from backup segment %s", seg.name, len(brecs), bname)
	return out, seg.path, nil
}

// lastEntryIndex / firstEntryIndex read a neighbouring local segment's edge
// record index (plain or compressed); ok is false when it cannot be read.
func lastEntryIndex(seg segFile) (uint64, bool) {
	data, spans, ok := loadSegmentSpans(seg)
	if !ok || len(spans) == 0 {
		return 0, false
	}
	return entryIndex(data, spans[len(spans)-1])
}

func firstEntryIndex(seg segFile) (uint64, bool) {
	data, spans, ok := loadSegmentSpans(seg)
	if !ok || len(spans) == 0 {
		return 0, false
	}
	return entryIndex(data, spans[0])
}

func loadSegmentSpans(seg segFile) ([]byte, []recordSpan, bool) {
	data, err := os.ReadFile(seg.path)
	if err != nil {
		return nil, nil, false
	}
	if strings.HasSuffix(seg.name, ".zst") {
		if data, err = decodeZstd(data); err != nil {
			return nil, nil, false
		}
	}
	spans, _ := parseSpans(data)
	return data, spans, true
}

// writeAtomic replaces path with data: temp file in the same directory,
// fsync, rename over, fsync the directory. A crash before the rename leaves
// path untouched (the temp is swept on the next run).
func writeAtomic(path string, data []byte) error {
	tmp := path + repairTmpSuffix
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := fsutil.SyncFile(tmp); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("fsync %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename %s over %s: %w", tmp, path, err)
	}
	if err := fsutil.SyncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("fsync dir after replacing %s: %w", path, err)
	}
	return nil
}

// removeStaleRepairTemps sweeps a temp left by a crash before its rename.
func removeStaleRepairTemps(dir string) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range ents {
		if !e.IsDir() && strings.HasSuffix(e.Name(), repairTmpSuffix) {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}
