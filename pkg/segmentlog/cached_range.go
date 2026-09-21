package segmentlog

import "errors"

// acquireRange runs under Log.mu. A cached source owns its contents; an
// uncached source borrows a file until release. Verify deliberately bypasses
// this path, so a cache hit cannot conceal damage from explicit disk checking.
// Misses enter historical retention; rollover supplies recent entries.
// Managed reads serialize misses; rewrite preparation can load concurrently.
func (l *Log) acquireRange(ref SegmentRef) (rangeSource, func() error, error) {
	return acquireCachedRange(l.path, l.cache, ref)
}

// The caller keeps the directory and reference alive for acquisition and release.
// Cache entries and budgets are safe to share; concurrent misses may load twice,
// but retain selects one immutable entry for the cache.
func acquireCachedRange(path string, cache *segmentCache, ref SegmentRef) (rangeSource, func() error, error) {
	if cache != nil {
		if s, ok := cache.acquire(ref); ok {
			return s, releaseCachedRange, nil
		}
	}
	f, err := openRangeFile(path, ref)
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
	if cache == nil || cache.historicalLimit == 0 {
		return source, f.Close, nil
	}
	cached, err := materializeSegment(ref, source)
	if err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		return nil, nil, err
	}
	return cache.retain(cached, false), releaseCachedRange, nil
}

func releaseCachedRange() error { return nil }
