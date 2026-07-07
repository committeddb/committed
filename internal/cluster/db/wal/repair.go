package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Offline WAL repair. A node's tidwall-backed logs store records as
// uvarint(size)+data, where data is a committed checksum frame (see
// checksum.go). Two failure modes can leave a log that will not Open:
//
//   - a TORN TAIL: the final record is a partial write (power loss mid-append).
//     tidwall's loadNextBinaryEntry sees size overrun the file. The record was
//     never fsync-acknowledged, so raft never treated it as committed —
//     truncating it is safe and lets the log open again.
//   - real CORRUPTION: a structurally-complete record whose checksum fails (a
//     bit-flip in committed data), or a directory caught mid-compaction. This is
//     NOT auto-repairable; the node rebuilds from a healthy replica.
//
// This runs offline (node stopped), like committed backup/restore, so the
// truncation never sits on the hot Open path and an operator confirms it.

// LogStatus classifies an offline scan of one WAL log directory.
type LogStatus string

const (
	// LogClean: every record is structurally complete and passes its checksum.
	LogClean LogStatus = "clean"
	// LogTornTail: the final record is an unacknowledged partial write, safe to
	// truncate.
	LogTornTail LogStatus = "torn-tail"
	// LogCorrupt: a complete record fails its checksum, or the log is
	// mid-compaction — not auto-repairable, rebuild from a healthy replica.
	LogCorrupt LogStatus = "corrupt"
)

// Diagnosis is the result of scanning one WAL log directory.
type Diagnosis struct {
	Dir      string
	Status   LogStatus
	Records  int    // complete, checksum-valid records scanned
	Detail   string // human-readable explanation
	Repaired bool   // RepairLog truncated a torn tail

	truncateSeg string // torn-tail: segment file to truncate or remove
	truncateOff int64  // torn-tail: byte offset to truncate to (0 => remove file)
}

type segFile struct {
	index uint64
	path  string
	name  string
}

// walLogSubdirs are the three tidwall-backed logs under a node's data dir,
// matching Open() in wal_storage.go. (metadata/ is bbolt, which page-checksums
// itself, and is not scanned here.)
var walLogSubdirs = [][]string{
	{"raft", "log"},   // entry log
	{"raft", "state"}, // state log
	{"events"},        // permanent event log
}

// listSegments returns one log dir's tidwall segment files sorted by start
// index, plus whether any .START/.END compaction marker is present.
func listSegments(dir string) ([]segFile, bool, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, false, err
	}
	var segs []segFile
	marker := false
	for _, e := range ents {
		name := e.Name()
		if e.IsDir() || len(name) < 20 {
			continue
		}
		idx, err := strconv.ParseUint(name[:20], 10, 64)
		if err != nil || idx == 0 {
			continue
		}
		isStart := len(name) == 26 && strings.HasSuffix(name, ".START")
		isEnd := len(name) == 24 && strings.HasSuffix(name, ".END")
		if len(name) != 20 && !isStart && !isEnd {
			continue
		}
		if isStart || isEnd {
			marker = true
		}
		segs = append(segs, segFile{index: idx, path: filepath.Join(dir, name), name: name})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].index < segs[j].index })
	return segs, marker, nil
}

// DiagnoseLog scans one WAL log directory (read-only) and classifies it,
// distinguishing a torn trailing record (safe to truncate) from mid-log
// corruption (must rebuild). It never modifies the log.
func DiagnoseLog(dir string) (*Diagnosis, error) {
	d := &Diagnosis{Dir: dir}
	segs, marker, err := listSegments(dir)
	if err != nil {
		if os.IsNotExist(err) {
			d.Status = LogClean
			d.Detail = "no log directory"
			return d, nil
		}
		return nil, err
	}
	if len(segs) == 0 {
		d.Status = LogClean
		d.Detail = "empty log"
		return d, nil
	}
	if marker {
		d.Status = LogCorrupt
		d.Detail = "log has .START/.END compaction markers (crash mid-compaction); not a simple torn tail — rebuild from a healthy replica"
		return d, nil
	}

	total := 0
	for si, seg := range segs {
		data, err := os.ReadFile(seg.path)
		if err != nil {
			return nil, err
		}
		off := 0
		for off < len(data) {
			size, n := binary.Uvarint(data[off:])
			// When n > 0, binary.Uvarint consumed n <= len(data)-off bytes, so
			// remaining is non-negative; the n <= 0 branch short-circuits first.
			remaining := len(data) - off - n
			if n <= 0 || uint64(remaining) < size { //nolint:gosec // G115: remaining >= 0 when n > 0
				// A structurally incomplete record — the write did not finish.
				if si != len(segs)-1 {
					// Incomplete record before the final segment is real
					// corruption, not a tail. Refuse.
					d.Status = LogCorrupt
					d.Detail = fmt.Sprintf("incomplete record at offset %d of non-final segment %s", off, seg.name)
					return d, nil
				}
				d.Status = LogTornTail
				d.Records = total
				d.truncateSeg = seg.path
				d.truncateOff = int64(off)
				d.Detail = fmt.Sprintf("incomplete trailing record at offset %d of last segment %s (%d valid records precede it)", off, seg.name, total)
				return d, nil
			}
			// size <= len(data)-off-n (checked just above), so it fits in int.
			recLen := int(size) //nolint:gosec // G115: bounded by the remaining buffer length
			rec := data[off+n : off+n+recLen]
			if _, uerr := unframe(rec); errors.Is(uerr, ErrCorruptEntry) {
				// A structurally complete record that fails its checksum: a
				// bit-flip in committed data, not an unacknowledged torn write.
				// Never truncate it — the operator rebuilds from a healthy replica.
				d.Status = LogCorrupt
				d.Records = total
				d.Detail = fmt.Sprintf("checksum mismatch on complete record %d (segment %s, offset %d) — data corruption, not a torn tail", total, seg.name, off)
				return d, nil
			}
			off += n + recLen
			total++
		}
	}
	d.Status = LogClean
	d.Records = total
	d.Detail = fmt.Sprintf("%d records, all valid", total)
	return d, nil
}

// RepairLog diagnoses one log dir and, when commit is true and the diagnosis is
// a torn tail, truncates the incomplete trailing record so the log opens again.
// A clean log is a no-op; a corrupt (mid-log / marker) log is never modified.
func RepairLog(dir string, commit bool) (*Diagnosis, error) {
	d, err := DiagnoseLog(dir)
	if err != nil {
		return nil, err
	}
	if d.Status != LogTornTail || !commit {
		return d, nil
	}
	if d.truncateOff == 0 {
		// The torn record is the entire final segment: remove the file. A prior
		// segment (if any) holds the valid tail; if it was the only segment the
		// log reopens empty, which is correct — nothing was ever durable.
		if err := os.Remove(d.truncateSeg); err != nil {
			return d, fmt.Errorf("remove torn segment %s: %w", d.truncateSeg, err)
		}
	} else if err := os.Truncate(d.truncateSeg, d.truncateOff); err != nil {
		return d, fmt.Errorf("truncate torn tail in %s: %w", d.truncateSeg, err)
	}
	d.Repaired = true
	return d, nil
}

// RepairNode scans (and, when commit, repairs a torn tail in) each of a node's
// three WAL logs under baseDir, returning one Diagnosis per log in fixed order:
// entry log, state log, event log.
func RepairNode(baseDir string, commit bool) ([]*Diagnosis, error) {
	out := make([]*Diagnosis, 0, len(walLogSubdirs))
	for _, parts := range walLogSubdirs {
		dir := filepath.Join(append([]string{baseDir}, parts...)...)
		d, err := RepairLog(dir, commit)
		if err != nil {
			return out, fmt.Errorf("%s: %w", dir, err)
		}
		out = append(out, d)
	}
	return out, nil
}
