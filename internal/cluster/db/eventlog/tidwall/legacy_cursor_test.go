package tidwall

import (
	"encoding/binary"
	"errors"
	"testing"

	native "github.com/tidwall/wal"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

func TestLegacyCursorSeekRetryAndAppend(t *testing.T) {
	log, err := native.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	var calls int
	fail := false
	errDecode := errors.New("decode failed")
	cursor := NewLegacyCursor(log, func(raw []byte) (uint64, eventlog.Record, error) {
		calls++
		id := binary.BigEndian.Uint64(raw)
		if fail && id == 30 {
			return 0, eventlog.Record{}, errDecode
		}
		return id, eventlog.Record{ID: id, Payload: raw[8:]}, nil
	})
	t.Cleanup(func() { _ = cursor.Close() })
	appendID := func(seq, id uint64) {
		t.Helper()
		if err := log.Write(seq, append(binary.BigEndian.AppendUint64(nil, id), "data"...)); err != nil {
			t.Fatal(err)
		}
	}
	check := func(id, want uint64) {
		t.Helper()
		r, err := cursor.Seek(id)
		if err != nil || r.ID != want || string(r.Payload) != "data" {
			t.Fatal(id, r, err)
		}
		r.Payload[0] = 'X'
	}
	if _, err := cursor.Seek(1); !errors.Is(err, eventlog.ErrNotFound) {
		t.Fatal(err)
	}
	for i, id := range []uint64{10, 30, 90} {
		appendID(uint64(i+1), id)
	}
	check(1, 10)
	calls = 0
	check(1, 10)  // Retry reuses the same physical sequence.
	check(11, 30) // Sequential progress uses the next physical sequence.
	if calls != 2 {
		t.Fatal("streaming performed a binary search", calls)
	}
	check(31, 90)
	if _, err := cursor.Seek(91); !errors.Is(err, eventlog.ErrNotFound) {
		t.Fatal(err)
	}
	appendID(4, 150)
	check(91, 150)
	check(0, 10) // Backward seeks resolve again.
	fail = true
	for range 2 {
		if _, err := cursor.Seek(11); !errors.Is(err, errDecode) {
			t.Fatal("failed read advanced cursor", err)
		}
	}
	fail = false
	check(11, 30)
	check(20, 30)
	check(150, 150)
	if _, err := cursor.Seek(^uint64(0)); !errors.Is(err, eventlog.ErrNotFound) {
		t.Fatal(err)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := cursor.Seek(0); !errors.Is(err, eventlog.ErrClosed) {
		t.Fatal(err)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
}
