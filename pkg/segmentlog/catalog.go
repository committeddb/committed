package segmentlog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

var (
	ErrCatalogConflict = errors.New("segmentlog: catalog revision conflict")
	ErrCatalogPoisoned = errors.New("segmentlog: catalog requires recovery after publication failure")
)

// SegmentRef describes an immutable revision of a fixed logical range. An empty
// range has Count zero, no File, and a zero SHA256; it still retains Coverage.
type SegmentRef struct {
	Coverage Coverage
	File     string
	SHA256   [32]byte
	Count    uint64
	// TailBytes freezes an append-format file at this exact size. Zero denotes indexed encoding.
	TailBytes int64 `json:",omitempty"`
}

// TailRef names the single active tail, whose end is recovered from its groups.
type TailRef struct {
	File       string
	Start      uint64
	Checkpoint *TailCheckpoint `json:",omitempty"`
}

// TailCheckpoint restores append progress at the end of a rewritten prefix.
// End is a complete-group byte boundary (or the header for an erased prefix).
// Count and Framed describe original appended input, not surviving payloads.
// Appends beyond End contribute their own original accounting during recovery.
type TailCheckpoint struct {
	End                 int64
	Last, Count, Framed uint64
}

func (c TailCheckpoint) valid(start uint64) bool {
	return c.End >= tailHeaderSize && c.Last >= start && c.Last != ^uint64(0) && c.Count > 0 && c.Count <= c.Last-start+1 && c.Count <= ^uint64(0)/format.FrameOverhead && c.Framed >= c.Count*format.FrameOverhead
}

// Catalog is a diagnostic layout snapshot. Revision tracks physical
// layout, Generation tracks logical rewriting, and History identifies the stream.
// Segments cover a contiguous interval beginning at Start. Active, if present,
// begins immediately after them. Immutable ranges may retain closed append-format files.
type Catalog struct {
	History    [16]byte
	Revision   uint64
	Generation uint64
	// SegmentBytes persists the managed log rotation target.
	SegmentBytes uint64 `json:",omitempty"`
	Start        uint64
	Segments     []SegmentRef
	Active       *TailRef
}

func validDataName(name, suffix string) bool {
	return len(name) > len(suffix) && len(name) <= 255 && filepath.Base(name) == name && !strings.ContainsAny(name, "/\\\x00") && !strings.HasPrefix(name, ".") && strings.HasSuffix(name, suffix)
}

func validateCatalog(c Catalog) error {
	if c.History == ([16]byte{}) || c.Revision == 0 || c.Start == ^uint64(0) || c.SegmentBytes > maxGroupBytes || (c.SegmentBytes > 0 && c.SegmentBytes < format.FrameOverhead) {
		return ErrInvalid
	}
	next := c.Start
	seen := map[string]bool{}
	for _, s := range c.Segments {
		if s.Coverage.Start != next || s.Coverage.End <= next {
			return ErrInvalid
		}
		next = s.Coverage.End
		if s.Count == 0 {
			if s.File != "" || s.SHA256 != ([32]byte{}) || s.TailBytes != 0 {
				return ErrInvalid
			}
			continue
		}
		suffix := ".seg"
		if s.TailBytes != 0 {
			if s.TailBytes < tailHeaderSize+groupHeaderSize+groupTrailerSize+format.FrameOverhead {
				return ErrInvalid
			}
			suffix = ".active"
		}
		if s.Count > s.Coverage.End-s.Coverage.Start || !validDataName(s.File, suffix) || s.SHA256 == ([32]byte{}) || seen[s.File] {
			return ErrInvalid
		}
		seen[s.File] = true
	}
	if c.Active != nil && (c.Active.Start != next || next == ^uint64(0) || !validDataName(c.Active.File, ".active") || seen[c.Active.File]) {
		return ErrInvalid
	}
	if c.Active != nil && c.Active.Checkpoint != nil && !c.Active.Checkpoint.valid(c.Active.Start) {
		return ErrInvalid
	}
	return nil
}

func verifyCatalogFiles(dir string, c Catalog) (retErr error) {
	return checkCatalogFiles(dir, c, true)
}

// Publication verifies and syncs candidates before selecting them. Offline
// inspection uses the same checks without flushing any files.
func checkCatalogFiles(dir string, c Catalog, syncFiles bool) (retErr error) {
	for _, ref := range c.Segments {
		if ref.Count == 0 {
			continue
		}
		err := func() (err error) {
			path := filepath.Join(dir, ref.File)
			info, err := os.Lstat(path) // #nosec G703 -- Reference basenames are validated by catalog decoding/publication; dir is the exclusively owned log directory.
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return ErrCorrupt
			}
			f, err := os.Open(path) // #nosec G304 G703 -- Fixed or validated catalog filename in the caller-selected, exclusively managed storage directory.
			if err != nil {
				return err
			}
			defer func() { err = errors.Join(err, f.Close()) }()
			if err := verifySegmentDigest(f, info.Size(), ref); err != nil {
				return err
			}
			if syncFiles {
				return f.Sync()
			}
			return nil
		}()
		if err != nil {
			return fmt.Errorf("segmentlog: segment %s: %w", ref.File, err)
		}
	}
	if c.Active != nil {
		path := filepath.Join(dir, c.Active.File)
		info, err := os.Lstat(path) // #nosec G703 -- Active basename is validated by catalog decoding/publication; dir is the exclusively owned log directory.
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return ErrCorrupt
		}
		f, err := os.Open(path) // #nosec G304 G703 -- Fixed or validated catalog filename in the caller-selected, exclusively managed storage directory.
		if err != nil {
			return err
		}
		defer func() { retErr = errors.Join(retErr, f.Close()) }()
		_, err = scanManagedTail(f, info.Size(), *c.Active, nil)
		if err != nil {
			return err
		}
		if syncFiles {
			return f.Sync()
		}
	}
	return nil
}
