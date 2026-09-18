package segmentlog

import (
	"errors"
	"hash"
	"io"
	"iter"
	"os"
	"path/filepath"
)

// rangeSource keeps managed range operations independent of physical encoding.
// Indexed segments and frozen append files preserve the same sparse record IDs.
type rangeSource interface {
	Coverage() Coverage
	Count() uint64
	Seek(uint64) (Record, error)
	Records() iter.Seq2[Record, error]
	recordsIn(Coverage) iter.Seq2[Record, error]
}

func openRangeSource(r io.ReaderAt, size int64, ref SegmentRef) (rangeSource, error) {
	if ref.TailBytes == 0 {
		return OpenSegment(r, size)
	}
	if err := checkClosedTail(r, size, ref, nil); err != nil {
		return nil, err
	}
	return &closedTail{r: r, size: size, coverage: ref.Coverage, count: ref.Count}, nil
}

// checkClosedTail validates the complete frozen append file and its catalog
// reference. An optional digest hashes physical bytes in the same scan.
func checkClosedTail(r io.ReaderAt, size int64, ref SegmentRef, digest hash.Hash) error {
	if size != ref.TailBytes {
		return ErrCorrupt
	}
	state, err := scanTailHashed(r, size, nil, nil, nil, digest)
	if err != nil {
		return err
	}
	if state.Start != ref.Coverage.Start || state.Count != ref.Count || !state.HasRecords || state.Last >= ref.Coverage.End {
		return ErrCorrupt
	}
	return nil
}

type closedTail struct {
	r        io.ReaderAt
	size     int64
	coverage Coverage
	count    uint64
}

func (s *closedTail) Coverage() Coverage                { return s.coverage }
func (s *closedTail) Count() uint64                     { return s.count }
func (s *closedTail) Records() iter.Seq2[Record, error] { return tailRecords(s.r, s.size) }
func (s *closedTail) Seek(id uint64) (Record, error) {
	var result Record
	_, err := ScanTail(s.r, s.size, func(r Record) error {
		if r.ID >= id {
			result = r
			return errStopScan
		}
		return nil
	})
	if errors.Is(err, errStopScan) {
		return result, nil
	}
	if err != nil {
		return Record{}, err
	}
	return Record{}, ErrNotFound
}

func rangeRecords(records iter.Seq2[Record, error], bounds Coverage) iter.Seq2[Record, error] {
	return func(yield func(Record, error) bool) {
		for r, err := range records {
			if err != nil {
				yield(Record{}, err)
				return
			}
			if r.ID >= bounds.End {
				return
			}
			if r.ID >= bounds.Start && !yield(r, nil) {
				return
			}
		}
	}
}

func (s *closedTail) recordsIn(bounds Coverage) iter.Seq2[Record, error] {
	return rangeRecords(s.Records(), bounds)
}

// openRangeFile checks the selected on-demand file before reading it. Exclusive
// directory ownership prevents replacement between the check and open.
func openRangeFile(dir string, ref SegmentRef) (*os.File, error) {
	path := filepath.Join(dir, ref.File)
	info, err := os.Lstat(path) // #nosec G703 -- ref is selected from the validated catalog; File is a validated data basename within the owned log directory.
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, ErrCorrupt
	}
	return os.Open(path) // #nosec G304 G703 -- Validated catalog range basename in the exclusively owned log directory.
}
