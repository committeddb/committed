package segmentlog

import (
	"errors"
	"os"
	"testing"
)

func cachedLog(t *testing.T, target int) *Log {
	t.Helper()
	l, err := CreateLog(t.TempDir(), 0, LogOptions{SegmentBytes: target, Cache: CacheOptions{RecentBytes: 1 << 20, HistoricalBytes: 1 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func TestResidentTailRolloverOwnership(t *testing.T) {
	l := cachedLog(t, 42)
	payload := []byte("value")
	if err := l.Append([]Record{{1, payload}, {10, payload}}); err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	old := l.resident
	if err := l.Scan(t.Context(), Coverage{0, 11}, func(r Record) error {
		if string(r.Payload) != "value" {
			t.Fatal(r)
		}
		r.Payload[0] = 'Y'
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := l.Append([]Record{{20, []byte("other")}}); err != nil {
		t.Fatal(err)
	}
	c, err := l.InspectCatalog()
	if err != nil {
		t.Fatal(err)
	}
	entry, ok := l.cache.acquire(c.Segments[0])
	if !ok {
		t.Fatal("rollover did not retain recent segment")
	}
	if &entry.data[0] != &old.data[0] || &entry.records[0] != &old.records[0] {
		t.Fatal("rollover copied arrays")
	}
	if old == l.resident {
		t.Fatal("reused frozen builder")
	}
	for _, id := range []uint64{1, 10} {
		r, err := l.Read(id)
		if err != nil || string(r.Payload) != "value" {
			t.Fatal(r, err)
		}
	}
	if stats := l.cache.stats(); stats.Misses != 0 || stats.RecentEntries != 1 {
		t.Fatal(stats)
	}
	if err := l.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestResidentTailRewriteRecovery(t *testing.T) {
	l := cachedLog(t, 63)
	if err := l.Append([]Record{{1, []byte("value")}, {10, []byte("value")}}); err != nil {
		t.Fatal(err)
	}
	_, err := l.Rewrite(t.Context(), 1, func(r Record) ([]byte, bool, error) { r.Payload[0] = 'X'; return r.Payload, r.ID != 10, nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err = OpenLog(l.path, Options{}, CacheOptions{RecentBytes: 1 << 20, HistoricalBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	if r, err := l.Read(1); err != nil || string(r.Payload) != "Xalue" {
		t.Fatal(r, err)
	}
	if _, err := l.Read(10); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err := l.Append([]Record{{10, []byte("again")}}); !errors.Is(err, ErrInvalid) {
		t.Fatal("reused erased ID", err)
	}
	if err := l.Append([]Record{{20, []byte("value")}, {30, []byte("value")}}); err != nil {
		t.Fatal(err)
	}
	c, err := l.InspectCatalog()
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Segments) != 1 || c.Active.Start != 21 {
		t.Fatal("lost original tail fullness", c)
	}
	if r, err := l.Seek(2); err != nil || r.ID != 20 {
		t.Fatal(r, err)
	}
	if err := l.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	path := l.path
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	plain, err := OpenLog(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = plain.Close() }()
	if plain.cache != nil || plain.resident != nil {
		t.Fatal("persisted runtime cache options")
	}
}

func TestResidentTailFailedAppend(t *testing.T) {
	l := cachedLog(t, 1024)
	if err := l.Append([]Record{{1, []byte("value")}}); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("sync failed")
	l.tail.file = &faultyTail{File: l.file, syncErr: boom}
	if err := l.Append([]Record{{2, []byte("other")}}); !errors.Is(err, boom) {
		t.Fatal(err)
	}
	if len(l.resident.records) != 1 {
		t.Fatal("failed append published resident data")
	}
	if _, err := l.Read(1); !errors.Is(err, ErrLogPoisoned) {
		t.Fatal("cache bypassed poison", err)
	}
}

func TestResidentTailFailedRewritePublication(t *testing.T) {
	for _, after := range []bool{false, true} {
		l := cachedLog(t, 1024)
		if err := l.Append([]Record{{1, []byte("value")}}); err != nil {
			t.Fatal(err)
		}
		old := l.resident
		boom := errors.New("publication failed")
		failMetadataCommit(l, 1, after, boom)
		_, err := l.Rewrite(t.Context(), 1, func(r Record) ([]byte, bool, error) { r.Payload[0] = 'X'; return r.Payload, true, nil })
		if !errors.Is(err, boom) {
			t.Fatal(err)
		}
		if l.resident != old || string(old.data) != "value" {
			t.Fatal("failed publication changed resident data")
		}
		if _, err := l.Read(1); !errors.Is(err, ErrLogPoisoned) {
			t.Fatal(err)
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := OpenLog(l.path, Options{}, CacheOptions{RecentBytes: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		want := "value"
		if after {
			want = "Xalue"
		}
		r, err := reopened.Read(1)
		if err != nil || string(r.Payload) != want {
			t.Fatal(r, err)
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestResidentTailReadsAndDiskVerification(t *testing.T) {
	l := cachedLog(t, 1024)
	if err := l.Append([]Record{{1, []byte("value")}}); err != nil {
		t.Fatal(err)
	}
	path := l.file.Name()
	data := readBytes(t, path)
	data[len(data)-1] ^= 1
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if r, err := l.Read(1); err != nil || string(r.Payload) != "value" {
		t.Fatal(r, err)
	}
	calls := 0
	if err := l.Scan(t.Context(), Coverage{0, 2}, func(r Record) error {
		calls++
		if string(r.Payload) != "value" {
			t.Fatal(r)
		}
		return nil
	}); err != nil || calls != 1 {
		t.Fatal(calls, err)
	}
	if err := l.Verify(t.Context()); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if _, builder, err := recoverResidentTail(l.file, nil, true); !errors.Is(err, ErrCorrupt) || builder != nil {
		t.Fatal("partial recovery escaped", builder, err)
	}
}
