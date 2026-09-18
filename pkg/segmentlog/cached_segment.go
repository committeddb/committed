package segmentlog

import (
	"bytes"
	"iter"
	"reflect"
	"sort"
)

// cachedSegment owns decoded payloads and a sparse ID index. After construction
// both are immutable; all records exposed to callers have private payloads.
// Holding this object keeps its storage alive independently of cache eviction.
type cachedSegment struct {
	ref     SegmentRef
	data    []byte
	records []cachedRecord
}

type cachedRecord struct {
	id         uint64
	start, end int
}

// materializeSegment consumes a validating source completely before returning.
// It copies each payload immediately, including sources that reuse their buffers.
func materializeSegment(ref SegmentRef, source rangeSource) (*cachedSegment, error) {
	if source.Coverage() != ref.Coverage || source.Count() != ref.Count || ref.Coverage.Start >= ref.Coverage.End {
		return nil, ErrCorrupt
	}
	s := &cachedSegment{ref: ref}
	for r, err := range source.Records() {
		if err != nil {
			return nil, err
		}
		if r.ID < ref.Coverage.Start || r.ID >= ref.Coverage.End || (len(s.records) > 0 && r.ID <= s.records[len(s.records)-1].id) || uint64(len(s.records)) >= ref.Count {
			return nil, ErrCorrupt
		}
		start := len(s.data)
		s.data = append(s.data, r.Payload...)
		s.records = append(s.records, cachedRecord{r.ID, start, len(s.data)})
	}
	if uint64(len(s.records)) != ref.Count {
		return nil, ErrCorrupt
	}
	return s, nil
}

// charge includes allocated payload/index capacity, not just populated length.
// Cache bookkeeping and outstanding caller copies are outside this accounting.
func (s *cachedSegment) charge() uint64 {
	return uint64(reflect.TypeFor[cachedSegment]().Size()) + uint64(cap(s.data)) + uint64(cap(s.records))*uint64(reflect.TypeFor[cachedRecord]().Size())
}

func (s *cachedSegment) Coverage() Coverage { return s.ref.Coverage }
func (s *cachedSegment) Count() uint64      { return s.ref.Count }
func (s *cachedSegment) record(i int) Record {
	r := s.records[i]
	return Record{r.id, bytes.Clone(s.data[r.start:r.end])}
}

func (s *cachedSegment) Seek(id uint64) (Record, error) {
	i := sort.Search(len(s.records), func(i int) bool { return s.records[i].id >= id })
	if i == len(s.records) {
		return Record{}, ErrNotFound
	}
	return s.record(i), nil
}
func (s *cachedSegment) Records() iter.Seq2[Record, error] { return s.recordsIn(s.ref.Coverage) }
func (s *cachedSegment) recordsIn(bounds Coverage) iter.Seq2[Record, error] {
	return func(yield func(Record, error) bool) {
		first := sort.Search(len(s.records), func(i int) bool { return s.records[i].id >= bounds.Start })
		for i := first; i < len(s.records) && s.records[i].id < bounds.End; i++ {
			if !yield(s.record(i), nil) {
				return
			}
		}
	}
}
