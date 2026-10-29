package wal

import (
	"sync"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// entryCacheBytes bounds the raft entries held in memory by entryCache. The
// newest entry is always kept, so one entry at the proposal cap fits whatever
// the bound.
const entryCacheBytes = 64 << 20

// entryCache holds the raft log's most recently appended entries, so raft's
// reads of its own tail are served from memory. Raft reads the tail all the
// time: with AsyncStorageWrites it takes committed entries to apply from
// Storage rather than its in-memory log, and it reads terms at the tail on
// every append it handles. Read from disk, each read decodes the whole entry,
// and raft does it on its own goroutine, so a large entry would hold up
// ticks and heartbeats for as long as the read takes.
//
// The entries are the same values Save was given; raft never changes an
// entry once created. The cache holds consecutive indexes only. Its bounds
// are checked against the log's: Entries and Term answer ErrCompacted and
// ErrUnavailable before they consult it.
type entryCache struct {
	mu    sync.Mutex
	ents  []*pb.Entry
	sizes []uint64
	bytes uint64
}

// append records entries just written to the log. An append starting at or
// below the cached tail replaces it, as it replaced the log's; one that does
// not follow the cached tail starts the cache over.
func (c *entryCache) append(ents []*pb.Entry) {
	if len(ents) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if n := len(c.ents); n > 0 {
		first, next := c.ents[0].GetIndex(), ents[0].GetIndex()
		switch {
		case next <= first || next > c.ents[n-1].GetIndex()+1:
			c.clearLocked()
		case next <= c.ents[n-1].GetIndex():
			keep := int(next - first) //nolint:gosec // G115: next is within the cached run, so this is below len(c.ents)
			for _, s := range c.sizes[keep:] {
				c.bytes -= s
			}
			clear(c.ents[keep:])
			c.ents, c.sizes = c.ents[:keep], c.sizes[:keep]
		}
	}
	for _, e := range ents {
		s := uint64(proto.Size(e)) //nolint:gosec // G115: a message size is never negative
		c.ents = append(c.ents, e)
		c.sizes = append(c.sizes, s)
		c.bytes += s
	}
	drop := 0
	for c.bytes > entryCacheBytes && drop < len(c.ents)-1 {
		c.bytes -= c.sizes[drop]
		drop++
	}
	c.dropLocked(drop)
}

// slice returns the entries [lo, hi), stopping once their sizes pass
// maxSize (always at least one), when every one of them is cached.
func (c *entryCache) slice(lo, hi, maxSize uint64) ([]*pb.Entry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.ents) == 0 || lo >= hi {
		return nil, false
	}
	first := c.ents[0].GetIndex()
	if lo < first || hi-1 > c.ents[len(c.ents)-1].GetIndex() {
		return nil, false
	}
	var out []*pb.Entry
	var total uint64
	for i := lo - first; i < hi-first; i++ {
		total += c.sizes[i]
		if len(out) > 0 && total > maxSize {
			break
		}
		out = append(out, c.ents[i])
	}
	return out, true
}

// term returns the term of entry i when it is cached.
func (c *entryCache) term(i uint64) (uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.ents) == 0 {
		return 0, false
	}
	first := c.ents[0].GetIndex()
	if i < first || i > c.ents[len(c.ents)-1].GetIndex() {
		return 0, false
	}
	return c.ents[i-first].GetTerm(), true
}

// compactThrough drops entries the log no longer holds.
func (c *entryCache) compactThrough(i uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	drop := 0
	for drop < len(c.ents) && c.ents[drop].GetIndex() <= i {
		c.bytes -= c.sizes[drop]
		drop++
	}
	c.dropLocked(drop)
}

func (c *entryCache) clear() {
	c.mu.Lock()
	c.clearLocked()
	c.mu.Unlock()
}

func (c *entryCache) clearLocked() {
	c.ents, c.sizes, c.bytes = nil, nil, 0
}

// dropLocked removes the first n entries; their sizes are already off bytes.
func (c *entryCache) dropLocked(n int) {
	if n == 0 {
		return
	}
	clear(c.ents[:n])
	c.ents = c.ents[n:]
	c.sizes = c.sizes[n:]
}
