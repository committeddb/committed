package segmentlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/committeddb/committed/internal/durablefs"
	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

const maxCatalogBytes = 16 << 20

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

// Catalog is an experimental complete local layout. Revision tracks physical
// layout, Generation tracks logical rewriting, and History identifies the stream.
// Segments cover a contiguous interval beginning at Start. Active, if present,
// begins immediately after them. Immutable ranges may retain closed append-format files.
type Catalog struct {
	History    [16]byte
	Revision   uint64
	Generation uint64
	// SegmentBytes persists the managed log rotation target; zero denotes a standalone catalog.
	SegmentBytes uint64 `json:",omitempty"`
	Start        uint64
	Segments     []SegmentRef
	Active       *TailRef
}

func validDataName(name, suffix string) bool {
	return len(name) > len(suffix) && len(name) <= 255 && filepath.Base(name) == name && !strings.ContainsAny(name, "/\\\x00") && !strings.HasPrefix(name, ".") && strings.HasSuffix(name, suffix)
}

func validateCatalog(c Catalog) error {
	if c.History == ([16]byte{}) || c.Revision == 0 || c.Start == ^uint64(0) || len(c.Segments) > format.MaxBlocks || c.SegmentBytes > maxGroupBytes || (c.SegmentBytes > 0 && c.SegmentBytes < format.FrameOverhead) {
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

func encodeCatalog(c Catalog) ([]byte, error) {
	if err := validateCatalog(c); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	if len(payload) > maxCatalogBytes {
		return nil, ErrInvalid
	}
	b := make([]byte, 16, 16+len(payload)+4)
	copy(b, "SLCAT000")
	format.LE.PutUint32(b[12:], uint32(len(payload))) // #nosec G115 -- Payload length is bounded by maxCatalogBytes above.
	b = append(b, payload...)
	b = format.LE.AppendUint32(b, format.CRC(b))
	return b, nil
}

func decodeCatalog(b []byte) (c Catalog, err error) {
	if len(b) < 20 || len(b) > maxCatalogBytes+20 || int64(format.LE.Uint32(b[12:])) != int64(len(b))-20 || format.CRC(b[:len(b)-4]) != format.LE.Uint32(b[len(b)-4:]) {
		return c, ErrCorrupt
	}
	if string(b[:8]) != "SLCAT000" || format.LE.Uint32(b[8:]) != 0 {
		return c, ErrUnsupported
	}
	d := json.NewDecoder(bytes.NewReader(b[16 : len(b)-4]))
	d.DisallowUnknownFields()
	if err = d.Decode(&c); err != nil {
		return c, errors.Join(ErrCorrupt, err)
	}
	if err = d.Decode(new(any)); err != io.EOF {
		return c, ErrCorrupt
	}
	if err = validateCatalog(c); err != nil {
		return c, errors.Join(ErrCorrupt, err)
	}
	// Require the canonical encoder representation, rejecting duplicate JSON
	// fields and alternate encodings rather than silently choosing one value.
	canonical, err := encodeCatalog(c)
	if err != nil || !bytes.Equal(canonical, b) {
		return c, ErrCorrupt
	}
	return c, nil
}

func catalogName(revision uint64, hash [32]byte) string {
	return fmt.Sprintf("catalog-%020d-%x.manifest", revision, hash)
}

func pointer(c Catalog, b []byte) []byte {
	p := make([]byte, 48, 52)
	copy(p, "SLCUR000")
	format.LE.PutUint64(p[8:], c.Revision)
	hash := sha256.Sum256(b)
	copy(p[16:], hash[:])
	return format.LE.AppendUint32(p, format.CRC(p))
}

func readBounded(path string, limit int64) (data []byte, err error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > limit {
		return nil, ErrCorrupt
	}
	f, err := os.Open(path) // #nosec G304 -- Fixed or validated catalog filename in the caller-selected, exclusively managed storage directory.
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, ErrCorrupt
	}
	return b, nil
}

func loadCatalog(dir string) (Catalog, string, error) {
	var c Catalog
	p, err := readBounded(filepath.Join(dir, "CURRENT"), 52)
	if err != nil {
		return c, "", err
	}
	if len(p) != 52 || format.CRC(p[:48]) != format.LE.Uint32(p[48:]) {
		return c, "", ErrCorrupt
	}
	if string(p[:8]) != "SLCUR000" {
		return c, "", ErrUnsupported
	}
	revision := format.LE.Uint64(p[8:])
	var hash [32]byte
	copy(hash[:], p[16:48])
	name := catalogName(revision, hash)
	b, err := readBounded(filepath.Join(dir, name), maxCatalogBytes+20)
	if err != nil {
		return c, "", err
	}
	if sha256.Sum256(b) != hash {
		return c, "", ErrCorrupt
	}
	c, err = decodeCatalog(b)
	if err != nil {
		return c, "", err
	}
	if c.Revision != revision {
		return c, "", ErrCorrupt
	}
	return c, name, nil
}

// CatalogStore coordinates one directory's catalog publications. The caller must
// exclusively own the directory across processes and instances. Methods serialize
// within this instance only. Publication retains old catalogs/files; the managed
// Log can reclaim obsolete artifacts separately. Publication is not erasure completion.
type CatalogStore struct {
	mu      sync.Mutex
	path    string
	pub     catalogPublisher
	current Catalog
	poison  error
}

type catalogPublisher interface {
	Install(string, func(io.Writer) error) (durablefs.Result, error)
	Replace(string, func(io.Writer) error) (durablefs.Result, error)
	Sync() error
}

// CreateCatalogStore explicitly initializes a directory containing prepared data
// files. It refuses an existing CURRENT or any catalog artifact, including an
// orphaned manifest. Initialization never guesses which old history to use.
// The initial revision must be 1. Referenced files must not change during the call.
func CreateCatalogStore(path string, initial Catalog) (*CatalogStore, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	d, err := durablefs.Open(path)
	if err != nil {
		return nil, err
	}
	names, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	for _, n := range names {
		if n.Name() == "CURRENT" || strings.HasPrefix(n.Name(), "catalog-") || strings.HasPrefix(n.Name(), ".segmentlog-") {
			return nil, ErrCatalogConflict
		}
	}
	if initial.Revision != 1 {
		return nil, ErrInvalid
	}
	s := &CatalogStore{path: path, pub: d}
	if err = s.publish(initial, true); err != nil {
		return nil, err
	}
	return s, nil
}

// OpenCatalogStore selects only CURRENT, validates all references, and confirms
// their file/directory durability before permitting further publication. Missing
// or corrupt CURRENT never falls back to another manifest. This initial verifier
// scans full sealed payloads and active groups; startup optimization is pending.
func OpenCatalogStore(path string) (*CatalogStore, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	d, err := durablefs.Open(path)
	if err != nil {
		return nil, err
	}
	c, name, err := loadCatalog(path)
	if err != nil {
		return nil, err
	}
	if err = verifyCatalogFiles(path, c); err != nil {
		return nil, err
	}
	for _, n := range []string{name, "CURRENT"} {
		if err = syncRegular(filepath.Join(path, n)); err != nil {
			return nil, err
		}
	}
	if err = d.Sync(); err != nil {
		return nil, err
	}
	return &CatalogStore{path: path, pub: d, current: c}, nil
}

func catalogEnd(c Catalog) uint64 {
	if len(c.Segments) == 0 {
		return c.Start
	}
	return c.Segments[len(c.Segments)-1].Coverage.End
}

func cloneCatalog(c Catalog) Catalog {
	if c.Segments != nil {
		c.Segments = append([]SegmentRef{}, c.Segments...)
	}
	if c.Active != nil {
		tail := *c.Active
		if tail.Checkpoint != nil {
			checkpoint := *tail.Checkpoint
			tail.Checkpoint = &checkpoint
		}
		c.Active = &tail
	}
	return c
}

func (s *CatalogStore) Current() (Catalog, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneCatalog(s.current), s.poison
}

// Publish requires the expected current revision and exactly its successor.
// History and Start cannot change; Generation cannot decrease. The caller is
// responsible for semantic equivalence of rewritten ranges and for fencing tail
// appends/rotation during preparation. Any publication I/O error poisons the
// instance; reopen to recover. Input/revision errors do not poison it.
// Exact unchanged sealed references retain their prior verification and durability;
// callers must never mutate or replace those files. Active tails are reverified.
func (s *CatalogStore) Publish(expected uint64, next Catalog) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poison != nil {
		return s.poison
	}
	if expected != s.current.Revision {
		return ErrCatalogConflict
	}
	if expected == ^uint64(0) || next.Revision != expected+1 || next.History != s.current.History || next.Start != s.current.Start || next.SegmentBytes != s.current.SegmentBytes || next.Generation < s.current.Generation || catalogEnd(next) < catalogEnd(s.current) {
		return ErrInvalid
	}
	return s.publish(next, false)
}

func (s *CatalogStore) publish(next Catalog, initial bool) error {
	b, err := encodeCatalog(next)
	if err != nil {
		return err
	}
	fail := func(err error) error { s.poison = errors.Join(ErrCatalogPoisoned, err); return s.poison }
	if err = verifyCatalogFiles(s.path, publicationFiles(s.current, next)); err != nil {
		return fail(err)
	}
	// Sync directory entries for caller-prepared reference files before the catalog.
	if err = s.pub.Sync(); err != nil {
		return fail(err)
	}
	return s.publishMetadata(next, b, initial)
}

// publishMetadata requires durable referenced files and validated catalog bytes.
// Its caller holds s.mu (or exclusively owns an unpublished new store).
// Only catalog installation and authoritative pointer replacement happen here.
func (s *CatalogStore) publishMetadata(next Catalog, b []byte, initial bool) error {
	var err error
	fail := func(err error) error { s.poison = errors.Join(ErrCatalogPoisoned, err); return s.poison }
	name := catalogName(next.Revision, sha256.Sum256(b))
	path := filepath.Join(s.path, name)
	if _, err = os.Lstat(path); err == nil {
		// A prior failed attempt can leave the same content-addressed catalog.
		// Exclusive directory ownership makes this check/install sequence safe.
		existing, e := readBounded(path, maxCatalogBytes+20)
		if e != nil || !bytes.Equal(existing, b) {
			return fail(errors.Join(ErrCorrupt, e))
		}
		if e = syncRegular(path); e != nil {
			return fail(e)
		}
		if e = s.pub.Sync(); e != nil {
			return fail(e)
		}
	} else if errors.Is(err, os.ErrNotExist) {
		if _, err = s.pub.Install(name, func(w io.Writer) error { return writeFull(w, b) }); err != nil {
			return fail(err)
		}
	} else {
		return fail(err)
	}
	p := pointer(next, b)
	if initial {
		_, err = s.pub.Install("CURRENT", func(w io.Writer) error { return writeFull(w, p) })
	} else {
		_, err = s.pub.Replace("CURRENT", func(w io.Writer) error { return writeFull(w, p) })
	}
	if err != nil {
		return fail(err)
	}
	s.current = cloneCatalog(next)
	return nil
}

// publicationFiles reuses verification only for exact immutable references in
// the last confirmed catalog. Creation/recovery verify all references first;
// failed publications never advance current. Active tails are always checked.
func publicationFiles(current, next Catalog) Catalog {
	pending := next
	pending.Segments = nil
	previous := 0
	for _, ref := range next.Segments {
		// Both catalogs have validated, ordered, nonoverlapping coverage.
		for previous < len(current.Segments) && current.Segments[previous].Coverage.Start < ref.Coverage.Start {
			previous++
		}
		if previous == len(current.Segments) || current.Segments[previous] != ref {
			pending.Segments = append(pending.Segments, ref)
		}
	}
	return pending
}

func syncRegular(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return ErrCorrupt
	}
	f, err := os.Open(path) // #nosec G304 -- Fixed or validated catalog filename in the caller-selected, exclusively managed storage directory.
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

func verifyCatalogFiles(dir string, c Catalog) (retErr error) {
	for _, ref := range c.Segments {
		if ref.Count == 0 {
			continue
		}
		err := func() (err error) {
			path := filepath.Join(dir, ref.File)
			info, err := os.Lstat(path)
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return ErrCorrupt
			}
			f, err := os.Open(path) // #nosec G304 -- Fixed or validated catalog filename in the caller-selected, exclusively managed storage directory.
			if err != nil {
				return err
			}
			defer func() { err = errors.Join(err, f.Close()) }()
			if err := verifySegmentDigest(f, info.Size(), ref); err != nil {
				return err
			}
			return f.Sync()
		}()
		if err != nil {
			return fmt.Errorf("segmentlog: segment %s: %w", ref.File, err)
		}
	}
	if c.Active != nil {
		path := filepath.Join(dir, c.Active.File)
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return ErrCorrupt
		}
		f, err := os.Open(path) // #nosec G304 -- Fixed or validated catalog filename in the caller-selected, exclusively managed storage directory.
		if err != nil {
			return err
		}
		defer func() { retErr = errors.Join(retErr, f.Close()) }()
		_, err = scanManagedTail(f, info.Size(), *c.Active, nil)
		if err != nil {
			return err
		}
		return f.Sync()
	}
	return nil
}
