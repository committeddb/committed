package segmentlog

import (
	"container/list"
	"sync"
)

// segmentCache is scoped to one log directory. It owns references, never reader
// lifetimes. A segment belongs to at most one retention list; eviction drops
// only the cache's reference. No disk I/O or callbacks run under its mutex.
// This cache is not yet connected to managed log operations.
type segmentCache struct {
	mu                           sync.Mutex
	entries                      map[SegmentRef]*list.Element
	recent, historical           list.List
	recentLimit, historicalLimit uint64
	recentBytes, historicalBytes uint64
	hits, misses, evictions      uint64
}

type cacheItem struct {
	segment *cachedSegment
	recent  bool
}

type segmentCacheStats struct {
	RecentBytes, HistoricalBytes     uint64
	RecentEntries, HistoricalEntries int
	Hits, Misses, Evictions          uint64
}

func newSegmentCache(recentBytes, historicalBytes uint64) *segmentCache {
	return &segmentCache{entries: make(map[SegmentRef]*list.Element), recentLimit: recentBytes, historicalLimit: historicalBytes}
}

// acquire refreshes historical recency exactly once per segment acquisition.
// Reading records through the returned object never touches policy state.
func (c *segmentCache) acquire(ref SegmentRef) (*cachedSegment, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[ref]
	if e == nil {
		c.misses++
		return nil, false
	}
	c.hits++
	item := e.Value.(cacheItem)
	if !item.recent {
		c.historical.MoveToFront(e)
	}
	return item.segment, true
}

// retain returns the existing immutable object on duplicate insertion. A recent
// insertion can promote a historical entry without copying its contents.
// Recent order follows logical range start, not access or insertion order.
// Oversized entries are usable by the caller but not retained; zero disables a
// tier. Aging out of recent does not automatically insert into historical.
func (c *segmentCache) retain(s *cachedSegment, recent bool) *cachedSegment {
	c.mu.Lock()
	defer c.mu.Unlock()
	var promotion *list.Element
	if e := c.entries[s.ref]; e != nil {
		item := e.Value.(cacheItem)
		s = item.segment
		if item.recent {
			return s
		}
		if !recent {
			c.historical.MoveToFront(e)
			return s
		}
		if c.recentLimit == 0 || s.charge() > c.recentLimit {
			return s
		}
		promotion = e
	}
	limit, used := c.historicalLimit, &c.historicalBytes
	order := &c.historical
	if recent {
		limit, used, order = c.recentLimit, &c.recentBytes, &c.recent
	}
	charge := s.charge()
	if limit == 0 || charge > limit {
		return s
	}
	if recent {
		// An older arrival must not displace newer ranges. Determine whether
		// it fits beside them before evicting anything.
		available := limit
		for at := order.Front(); at != nil; at = at.Next() {
			newer := at.Value.(cacheItem).segment
			if newer.ref.Coverage.Start <= s.ref.Coverage.Start {
				break
			}
			available -= newer.charge()
		}
		if charge > available {
			return s
		}
	}
	if promotion != nil {
		c.remove(promotion)
	}
	// Make room before adding so byte accounting cannot overflow.
	for *used > limit-charge {
		c.remove(order.Back())
		c.evictions++
	}
	item := cacheItem{s, recent}
	var e *list.Element
	if recent {
		for at := order.Front(); at != nil; at = at.Next() {
			if at.Value.(cacheItem).segment.ref.Coverage.Start <= s.ref.Coverage.Start {
				e = order.InsertBefore(item, at)
				break
			}
		}
	}
	if e == nil {
		if recent {
			e = order.PushBack(item)
		} else {
			e = order.PushFront(item)
		}
	}
	*used += charge
	c.entries[s.ref] = e
	return s
}

func (c *segmentCache) remove(e *list.Element) {
	item := e.Value.(cacheItem)
	delete(c.entries, item.segment.ref)
	if item.recent {
		c.recentBytes -= item.segment.charge()
		c.recent.Remove(e)
	} else {
		c.historicalBytes -= item.segment.charge()
		c.historical.Remove(e)
	}
}

func (c *segmentCache) discard(ref SegmentRef) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.entries[ref]; e != nil {
		c.remove(e)
	}
}

func (c *segmentCache) stats() segmentCacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return segmentCacheStats{c.recentBytes, c.historicalBytes, c.recent.Len(), c.historical.Len(), c.hits, c.misses, c.evictions}
}
