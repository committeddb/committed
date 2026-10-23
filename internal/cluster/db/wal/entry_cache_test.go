package wal

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func cacheEntries(term, from, to uint64, size int) []*pb.Entry {
	var out []*pb.Entry
	for i := from; i <= to; i++ {
		out = append(out, &pb.Entry{Term: proto.Uint64(term), Index: proto.Uint64(i), Data: make([]byte, size)})
	}
	return out
}

func cachedIndexes(es []*pb.Entry) []uint64 {
	out := make([]uint64, len(es))
	for i, e := range es {
		out[i] = e.GetIndex()
	}
	return out
}

// TestEntryCache_ServesOnlyWhollyCachedRanges: a range is served only when
// every entry in it is cached, stopping at maxSize but never returning none.
func TestEntryCache_ServesOnlyWhollyCachedRanges(t *testing.T) {
	var c entryCache
	c.append(cacheEntries(1, 5, 9, 10))

	es, ok := c.slice(5, 10, 1<<20)
	require.True(t, ok)
	require.Equal(t, []uint64{5, 6, 7, 8, 9}, cachedIndexes(es))

	_, ok = c.slice(4, 6, 1<<20)
	require.False(t, ok, "entry 4 is not cached")
	_, ok = c.slice(8, 11, 1<<20)
	require.False(t, ok, "entry 10 is not cached")

	size := uint64(proto.Size(es[0]))
	es, ok = c.slice(5, 10, 2*size)
	require.True(t, ok)
	require.Equal(t, []uint64{5, 6}, cachedIndexes(es))
	es, ok = c.slice(5, 10, 0)
	require.True(t, ok)
	require.Equal(t, []uint64{5}, cachedIndexes(es))

	term, ok := c.term(7)
	require.True(t, ok)
	require.Equal(t, uint64(1), term)
	_, ok = c.term(10)
	require.False(t, ok)
}

// TestEntryCache_FollowsTheLog: an append at or below the cached tail
// replaces it, a gap starts the cache over, and compaction drops entries.
func TestEntryCache_FollowsTheLog(t *testing.T) {
	var c entryCache
	c.append(cacheEntries(1, 1, 5, 10))
	c.append(cacheEntries(2, 4, 6, 10))
	es, ok := c.slice(1, 7, 1<<20)
	require.True(t, ok)
	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6}, cachedIndexes(es))
	term, _ := c.term(3)
	require.Equal(t, uint64(1), term)
	term, _ = c.term(4)
	require.Equal(t, uint64(2), term)

	c.compactThrough(2)
	_, ok = c.slice(2, 4, 1<<20)
	require.False(t, ok)
	_, ok = c.slice(3, 7, 1<<20)
	require.True(t, ok)

	c.append(cacheEntries(3, 9, 9, 10))
	_, ok = c.slice(3, 4, 1<<20)
	require.False(t, ok, "a gap starts the cache over")
	_, ok = c.slice(9, 10, 1<<20)
	require.True(t, ok)

	c.clear()
	_, ok = c.term(9)
	require.False(t, ok)
}

// TestEntryCache_KeepsTheNewestEntryWithinItsBound: past its bound the cache
// drops its oldest entries, but always keeps the newest, so one entry at the
// proposal cap is served from memory.
func TestEntryCache_KeepsTheNewestEntryWithinItsBound(t *testing.T) {
	var c entryCache
	c.append(cacheEntries(1, 1, 3, entryCacheBytes/4))
	c.append(cacheEntries(1, 4, 4, entryCacheBytes))
	_, ok := c.slice(3, 4, 1<<30)
	require.False(t, ok)
	es, ok := c.slice(4, 5, 0)
	require.True(t, ok)
	require.Equal(t, []uint64{4}, cachedIndexes(es))
	require.LessOrEqual(t, c.bytes, uint64(entryCacheBytes+100))
}
