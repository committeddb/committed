package segmentlog

import "sort"

// Cursor is a per-reader seek hint, not a pinned view. Each call observes the
// current rewrite generation under Log.mu. Close releases its retained arrays.
// Repeated and backwards requests are supported; logical progress belongs to
// the caller. Eviction cannot invalidate an entry already held by a cursor.
type Cursor struct {
	log    *Log
	epoch  *byte
	entry  *cachedSegment
	tail   *segmentBuilder
	start  uint64
	closed bool
	index  int
}

func (l *Log) NewCursor() *Cursor { return &Cursor{log: l} }

func (c *Cursor) Seek(id uint64) (Record, error) {
	l := c.log
	l.mu.Lock()
	defer l.mu.Unlock()
	if c.closed {
		return Record{}, ErrClosed
	}
	if err := l.usable(); err != nil {
		c.entry = nil
		c.tail = nil
		return Record{}, err
	}
	if c.epoch != l.cursorEpoch {
		c.entry = nil
		c.tail = nil
		c.epoch = l.cursorEpoch
	}
	if c.tail != nil && c.tail == l.resident && id >= c.start {
		return c.read(c.tail.view(c.start), id)
	}
	if c.entry != nil && id >= c.entry.ref.Coverage.Start && id < c.entry.ref.Coverage.End {
		if r, err := c.read(c.entry, id); err == nil {
			return r, nil
		}
	}
	c.entry = nil
	c.tail = nil
	return l.seek(id, c)
}

func (c *Cursor) Close() error {
	c.log.mu.Lock()
	defer c.log.mu.Unlock()
	c.closed = true
	c.entry = nil
	c.tail = nil
	return nil
}

// read reuses a record offset for sequential calls, falling back to a binary
// search for arbitrary requests. Checks also make the hint safe on a new source.
func (c *Cursor) read(s *cachedSegment, id uint64) (Record, error) {
	i := c.index
	if i < len(s.records) && s.records[i].id < id {
		i++
	}
	if i >= len(s.records) || s.records[i].id < id || (i > 0 && s.records[i-1].id >= id) {
		i = sort.Search(len(s.records), func(i int) bool { return s.records[i].id >= id })
	}
	c.index = i
	if i == len(s.records) {
		return Record{}, ErrNotFound
	}
	return s.record(i), nil
}
