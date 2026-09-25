package segmentlog

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

func TestCursorRetainsEvictedRange(t *testing.T) {
	l := cachedLog(t, 42)
	if err := l.Append([]Record{{1, []byte("value")}, {10, []byte("value")}, {20, []byte("value")}}); err != nil {
		t.Fatal(err)
	}
	c := l.NewCursor()
	defer func() { _ = c.Close() }()
	if _, err := c.Seek(1); err != nil {
		t.Fatal(err)
	}
	held := c.entry
	if held == nil {
		t.Fatal("cursor did not retain a range")
	}
	l.cache.discard(held.ref)
	before := l.cache.stats()
	r, err := c.Seek(2)
	if err != nil || r.ID != 10 || c.entry != held {
		t.Fatal(r, err)
	}
	if l.cache.stats() != before {
		t.Fatal("record read reacquired segment")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if c.entry != nil || c.tail != nil {
		t.Fatal("closed cursor retained bytes")
	}
}

func TestCursorRejectsFailedPublication(t *testing.T) {
	for _, after := range []bool{false, true} {
		l := cachedLog(t, 42)
		if err := l.Append([]Record{{1, []byte("value")}, {10, []byte("value")}, {20, []byte("value")}}); err != nil {
			t.Fatal(err)
		}
		c := l.NewCursor()
		if _, err := c.Seek(1); err != nil {
			t.Fatal(err)
		}
		boom := errors.New("publication failure")
		failMetadataCommit(l, 1, after, boom)
		_, err := l.Rewrite(t.Context(), 1, func(r Record) ([]byte, bool, error) { return r.Payload, r.ID != 1, nil })
		if !errors.Is(err, boom) {
			t.Fatal(err)
		}
		if _, err := c.Seek(1); !errors.Is(err, ErrLogPoisoned) {
			t.Fatal("cursor bypassed publication failure", err)
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// Exercise the interval between selecting a payload and returning its private
// copy. Maintenance can retire its source but must never alter selected bytes.
func TestCursorSelectedPayloadSurvivesMaintenance(t *testing.T) {
	for _, id := range []uint64{1, 10} {
		t.Run(fmt.Sprint(id), func(t *testing.T) {
			l := cachedLog(t, 4096)
			want := bytes.Repeat([]byte("original"), 512)
			if err := l.Append([]Record{{1, want}, {10, want}}); err != nil {
				t.Fatal(err)
			}
			c := l.NewCursor()
			defer func() { _ = c.Close() }()
			for range 2 {
				r, err := c.Seek(id)
				if err != nil || !bytes.Equal(r.Payload, want) {
					t.Fatal("unexpected cursor record", err)
				}
				// Returned payloads remain private on both acquisition and hits.
				r.Payload[0] = 'X'
			}
			l.mu.Lock()
			selected, borrowed, err := c.seekLocked(id)
			l.mu.Unlock()
			if err != nil || !borrowed {
				t.Fatal("expected a retained payload", borrowed, err)
			}
			check := func() {
				t.Helper()
				if !bytes.Equal(bytes.Clone(selected.Payload), want) {
					t.Fatal("maintenance modified selected bytes")
				}
			}
			// Appending rolls the resident source into the sealed cache.
			if err := l.Append([]Record{{20, want}}); err != nil {
				t.Fatal(err)
			}
			check()
			if _, err := l.Rewrite(t.Context(), 1, func(r Record) ([]byte, bool, error) {
				for i := range r.Payload {
					r.Payload[i] = 'Y'
				}
				return r.Payload, true, nil
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := l.Reclaim(t.Context()); err != nil {
				t.Fatal(err)
			}
			check()
			if err := l.Reset(); err != nil {
				t.Fatal(err)
			}
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			check()
		})
	}
}
