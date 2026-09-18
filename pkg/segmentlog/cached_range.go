package segmentlog

import "errors"

// acquireRange runs under Log.mu. A cached source owns its contents; an
// uncached source borrows a file until release. Verify deliberately bypasses
// this path, so a cache hit cannot conceal damage from explicit disk checking.
// Only historical retention is connected here; normal opens leave cache nil.
// Log.mu serializes misses, so concurrent managed reads do not duplicate loads.
func (l *Log) acquireRange(ref SegmentRef) (rangeSource, func() error, error) {
	if l.cache != nil {
		if s, ok := l.cache.acquire(ref); ok {
			return s, releaseCachedRange, nil
		}
	}
	f, err := openRangeFile(l.path, ref)
	if err != nil {
		return nil, nil, err
	}
	fail := func(err error) (rangeSource, func() error, error) {
		return nil, nil, errors.Join(err, f.Close())
	}
	info, err := f.Stat()
	if err != nil {
		return fail(err)
	}
	source, err := openRangeSource(f, info.Size(), ref)
	if err != nil {
		return fail(err)
	}
	if source.Coverage() != ref.Coverage || source.Count() != ref.Count {
		return fail(ErrCorrupt)
	}
	if l.cache == nil || l.cache.historicalLimit == 0 {
		return source, f.Close, nil
	}
	cached, err := materializeSegment(ref, source)
	if err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		return nil, nil, err
	}
	return l.cache.retain(cached, false), releaseCachedRange, nil
}

func releaseCachedRange() error { return nil }
