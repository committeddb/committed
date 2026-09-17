package segmentlog

import (
	"errors"
	"io"
	"iter"
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
	if size != ref.TailBytes {
		return nil, ErrCorrupt
	}
	state, err := scanTail(r, size, nil, nil)
	if err != nil {
		return nil, err
	}
	if state.Start != ref.Coverage.Start || state.Count != ref.Count || !state.HasRecords || state.Last >= ref.Coverage.End {
		return nil, ErrCorrupt
	}
	return &closedTail{r: r, size: size, coverage: ref.Coverage, count: ref.Count}, nil
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
