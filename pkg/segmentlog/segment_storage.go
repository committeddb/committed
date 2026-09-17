package segmentlog

import (
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
)

// segmentStorage prepares physical files without selecting the authoritative
// layout. The managed Log owns exclusive mutation throughout preparation and
// publication. This initial boundary covers rollover; rewrite uses its existing
// independently verified publication path.
type segmentStorage struct {
	path      string
	installer fileInstaller
}

// preparedRollover is private, process-local preparation state, not a disk
// receipt. Only prepareRollover constructs it. The caller must neither append to
// the old tail nor write the new tail before publication, and owns closing file
// on failure. No referenced file may be removed on an uncertain publication.
type preparedRollover struct {
	path     string
	history  [16]byte
	revision uint64
	source   string
	closed   SegmentRef
	active   TailRef
	file     *os.File
	tail     *Tail
	consumed bool
}

// prepareRollover accepts the exclusively owned appender's last synchronized
// state. Its old name is already durable. The installer establishes both data
// and directory durability for the header and first group together. records and
// framed come from Append's validated batch and rotation partitioning.
func (s segmentStorage) prepareRollover(c Catalog, file *os.File, state TailState, records []Record, framed uint64) (*preparedRollover, error) {
	if c.Active == nil || !state.HasRecords || state.Start != c.Active.Start || len(records) == 0 || framed == 0 || framed > maxGroupBytes {
		return nil, ErrInvalid
	}
	ref := SegmentRef{Coverage: Coverage{Start: state.Start, End: state.Last + 1}}
	if state.Count > 0 {
		hash := sha256.New()
		n, err := io.Copy(hash, io.NewSectionReader(file, 0, state.End))
		if err != nil {
			return nil, err
		}
		if n != state.End {
			return nil, ErrCorrupt
		}
		ref.File, ref.Count, ref.TailBytes = c.Active.File, state.Count, state.End
		copy(ref.SHA256[:], hash.Sum(nil))
	}
	name, err := uniqueName("tail", ref.Coverage.End, ".active")
	if err != nil {
		return nil, err
	}
	group := encodeTailGroup(records, int(framed)) // #nosec G115 -- framed is bounded by maxGroupBytes above.
	if _, err = s.installer.Install(name, func(w io.Writer) error {
		if e := WriteTailHeader(w, ref.Coverage.End); e != nil {
			return e
		}
		return writeFull(w, group)
	}); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(s.path, name), os.O_RDWR, 0) // #nosec G304 -- Internally generated basename in the exclusively managed log directory.
	if err != nil {
		return nil, err
	}
	// These validated bytes were durably installed by us. Construct the known
	// state directly; recovery scanning and another sync are unnecessary here.
	tail := &Tail{file: f, state: TailState{
		Start: ref.Coverage.End, End: tailHeaderSize + int64(len(group)),
		Last: records[len(records)-1].ID, HasRecords: true,
		Count: uint64(len(records)), OriginalCount: uint64(len(records)), Framed: framed,
	}}
	return &preparedRollover{
		path: s.path, history: c.History, revision: c.Revision, source: c.Active.File,
		closed: ref, active: TailRef{File: name, Start: ref.Coverage.End}, file: f, tail: tail,
	}, nil
}
