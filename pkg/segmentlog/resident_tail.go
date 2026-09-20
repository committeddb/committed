package segmentlog

// CacheOptions supplies runtime memory budgets; it is never stored on disk.
// Zero budgets disable caching. When either budget is nonzero, the active tail
// is resident in addition to these sealed-segment budgets. Reader-owned payload
// copies and evicted entries still in use are also outside the budgets.
type CacheOptions struct {
	RecentBytes     uint64
	HistoricalBytes uint64
}

// segmentBuilder is mutable only under Log.mu (or before a Log is published).
// freeze transfers its arrays into an immutable entry; the builder is then
// discarded, never appended to again.
type segmentBuilder struct {
	data    []byte
	records []cachedRecord
}

func (b *segmentBuilder) append(r Record) {
	start := len(b.data)
	b.data = append(b.data, r.Payload...)
	b.records = append(b.records, cachedRecord{r.ID, start, len(b.data)})
}

// view borrows the arrays only while Log.mu is held; no entry escapes that lock.
func (b *segmentBuilder) view(start uint64) *cachedSegment {
	return b.freeze(SegmentRef{Coverage: Coverage{start, ^uint64(0)}, Count: uint64(len(b.records))})
}

func (b *segmentBuilder) freeze(ref SegmentRef) *cachedSegment {
	return &cachedSegment{ref: ref, data: b.data, records: b.records}
}

// recoverResidentTail collects private payloads during the existing recovery
// scan, not through an additional file read. No partial builder escapes failure.
func recoverResidentTail(file TailFile, checkpoint *TailCheckpoint, resident bool) (*Tail, *segmentBuilder, error) {
	var b *segmentBuilder
	var visit func(Record) error
	if resident {
		b = &segmentBuilder{}
		visit = func(r Record) error { b.append(r); return nil }
	}
	tail, err := openTailWithVisitor(file, checkpoint, visit)
	if err != nil {
		return nil, nil, err
	}
	return tail, b, nil
}
