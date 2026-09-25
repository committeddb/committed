package segmentlog

import (
	"bytes"
	"errors"
	"fmt"
	"iter"
	"sync"
	"testing"
)

type cacheTestSource struct {
	rangeSource
	stream iter.Seq2[Record, error]
}

func (s cacheTestSource) Records() iter.Seq2[Record, error] { return s.stream }

func TestMaterializeSegmentOwnershipAndErrors(t *testing.T) {
	base := cacheFixture(t, 0)
	reused := []byte("one")
	source := cacheTestSource{rangeSource: base, stream: func(yield func(Record, error) bool) {
		if !yield(Record{0, reused}, nil) {
			return
		}
		copy(reused, "two")
		yield(Record{7, reused}, nil)
	}}
	s, err := materializeSegment(base.ref, source)
	if err != nil {
		t.Fatal(err)
	}
	copy(reused, "bad")
	for _, tc := range []struct {
		id   uint64
		want string
	}{{0, "one"}, {1, "two"}, {7, "two"}} {
		r, err := s.Seek(tc.id)
		if err != nil || string(r.Payload) != tc.want {
			t.Fatal(tc, r, err)
		}
	}
	if _, err := s.Seek(8); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	injected := errors.New("source failed")
	for _, tc := range []struct {
		stream iter.Seq2[Record, error]
		want   error
	}{
		{sequence(Record{0, nil}), ErrCorrupt},
		{sequence(Record{7, nil}, Record{0, nil}), ErrCorrupt},
		{sequence(Record{0, nil}, Record{10, nil}), ErrCorrupt},
		{sequence(Record{0, nil}, Record{7, nil}, Record{8, nil}), ErrCorrupt},
		{func(yield func(Record, error) bool) { yield(Record{}, injected) }, injected},
	} {
		source.stream = tc.stream
		if got, err := materializeSegment(base.ref, source); got != nil || !errors.Is(err, tc.want) {
			t.Fatal(got, err)
		}
	}
	wrong := base.ref
	wrong.Count++
	if got, err := materializeSegment(wrong, base); got != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatal(got, err)
	}
}

func cacheFixture(t testing.TB, start uint64) *cachedSegment {
	t.Helper()
	var buf bytes.Buffer
	coverage := Coverage{start, start + 10}
	records := []Record{{start, []byte("original")}, {start + 7, []byte("second")}}
	if err := WriteSegment(&buf, coverage, sequence(records...), Options{}); err != nil {
		t.Fatal(err)
	}
	ref := SegmentRef{Coverage: coverage, File: fmt.Sprintf("%d.seg", start), Count: 2}
	s, err := materializeSegment(ref, open(t, buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSegmentCachePolicies(t *testing.T) {
	a, b, d := cacheFixture(t, 0), cacheFixture(t, 10), cacheFixture(t, 20)
	budget := 2 * a.charge()
	c := newSegmentCache(budget, budget)
	c.retain(a, true)
	c.retain(b, true)
	c.acquire(a.ref) // Does not protect the oldest recent entry.
	c.retain(d, true)
	if _, ok := c.acquire(a.ref); ok {
		t.Fatal("recent hit changed age order")
	}
	c.retain(a, true) // Old cold arrival cannot evict either newer range.
	if _, ok := c.acquire(b.ref); !ok {
		t.Fatal("old insertion evicted recent range")
	}
	if _, ok := c.acquire(a.ref); ok {
		t.Fatal("retained old arrival over newer ranges")
	}
	c.retain(a, false)
	e := cacheFixture(t, 30)
	c.retain(e, false)
	c.acquire(a.ref) // Refresh on segment acquisition.
	f := cacheFixture(t, 40)
	c.retain(f, false)
	if _, ok := c.acquire(e.ref); ok {
		t.Fatal("historical LRU did not evict least recently acquired")
	}
	// Record reads through an already acquired object must not refresh recency.
	if _, err := a.Seek(0); err != nil {
		t.Fatal(err)
	}
	c.retain(e, false)
	if _, ok := c.acquire(a.ref); ok {
		t.Fatal("record read refreshed segment LRU")
	}
	stats := c.stats()
	if stats.RecentBytes > budget || stats.HistoricalBytes > budget || stats.RecentEntries != 2 || stats.HistoricalEntries != 2 {
		t.Fatal(stats)
	}
}

func TestSegmentCacheOwnershipAndIdentity(t *testing.T) {
	a := cacheFixture(t, 0)
	c := newSegmentCache(a.charge(), a.charge())
	c.retain(a, false)
	duplicate := cacheFixture(t, 0)
	if got := c.retain(duplicate, true); got != a {
		t.Fatal("promotion duplicated contents")
	}
	if stats := c.stats(); stats.RecentEntries != 1 || stats.HistoricalEntries != 0 {
		t.Fatal(stats)
	}
	held, ok := c.acquire(a.ref)
	if !ok {
		t.Fatal("missing promoted entry")
	}
	// A replacement at the same coverage must not hit the old revision.
	replacement := a.ref
	replacement.SHA256[0] = 1
	if _, ok := c.acquire(replacement); ok {
		t.Fatal("stale identity hit")
	}
	c.discard(a.ref)
	c.retain(cacheFixture(t, 10), true)
	first, err := held.Seek(0)
	if err != nil {
		t.Fatal(err)
	}
	first.Payload[0] = 'X'
	for r, err := range held.Records() {
		if err != nil {
			t.Fatal(err)
		}
		r.Payload[0] = 'Y'
	}
	again, err := held.Seek(0)
	if err != nil || string(again.Payload) != "original" {
		t.Fatal("reader invalidated or cached bytes mutated", again, err)
	}
	got := 0
	for r, err := range held.recordsIn(Coverage{1, 8}) {
		if err != nil || r.ID != 7 {
			t.Fatal(r, err)
		}
		got++
	}
	if got != 1 {
		t.Fatal(got)
	}
}

func TestSegmentCacheAdmission(t *testing.T) {
	a := cacheFixture(t, 0)
	for _, limit := range []uint64{0, a.charge() - 1, a.charge()} {
		for _, recent := range []bool{false, true} {
			c := newSegmentCache(limit, limit)
			if c.retain(a, recent) != a {
				t.Fatal("lost uncached reader")
			}
			_, ok := c.acquire(a.ref)
			if ok != (limit == a.charge()) {
				t.Fatal(limit, recent, ok)
			}
		}
	}
	// A failed promotion leaves the historical copy resident.
	c := newSegmentCache(a.charge(), a.charge())
	c.retain(cacheFixture(t, 10), true)
	c.retain(a, false)
	c.retain(a, true)
	if stats := c.stats(); stats.HistoricalEntries != 1 || stats.RecentEntries != 1 {
		t.Fatal(stats)
	}
}

func TestMaterializeSegmentRejectsCorruption(t *testing.T) {
	raw := encode(t, Record{0, []byte("first")}, Record{7, []byte("last")})
	s := open(t, raw)
	ref := SegmentRef{Coverage: s.Coverage(), Count: s.Count()}
	raw[s.blocks[len(s.blocks)-1].offset+12] ^= 1
	if got, err := materializeSegment(ref, s); got != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatal(got, err)
	}
}

func TestSegmentCacheConcurrentEviction(t *testing.T) {
	entries := make([]*cachedSegment, 8)
	for i := range entries {
		entries[i] = cacheFixture(t, uint64(i*10))
	}
	c := newSegmentCache(2*entries[0].charge(), 2*entries[0].charge())
	var workers sync.WaitGroup
	for worker := range 8 {
		workers.Go(func() {
			for i := range 100 {
				entry := entries[(worker+i)%len(entries)]
				held := c.retain(entry, i%2 == 0)
				if hit, ok := c.acquire(entry.ref); ok {
					held = hit
				}
				c.discard(entry.ref)
				r, err := held.Seek(entry.ref.Coverage.Start)
				if err != nil || string(r.Payload) != "original" {
					t.Error("eviction damaged reader", err)
					return
				}
				r.Payload[0] = 'X'
			}
		})
	}
	workers.Wait()
	stats := c.stats()
	if stats.RecentBytes > c.recentLimit || stats.HistoricalBytes > c.historicalLimit {
		t.Fatal(stats)
	}
}
