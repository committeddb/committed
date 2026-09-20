package segmentlog

import (
	"errors"
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
