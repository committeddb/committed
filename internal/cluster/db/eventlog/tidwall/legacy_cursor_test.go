package tidwall

import (
	"encoding/binary"
	"errors"
	"slices"
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

func TestLegacySequenceForNativeTransfer(t *testing.T) {
	log, err := native.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	calls := 0
	errDecode := errors.New("decode failed")
	fail := false
	positioner := NewLegacyCursor(log, func(raw []byte) (uint64, uint64, error) {
		calls++
		if fail {
			return 0, 0, errDecode
		}
		id := binary.BigEndian.Uint64(raw)
		return id, id, nil
	})
	defer func() { _ = positioner.Close() }()
	for _, id := range []uint64{0, 10, ^uint64(0)} {
		if seq, err := positioner.SequenceFor(id); err != nil || seq != 1 {
			t.Fatal(id, seq, err)
		}
	}
	for i, id := range []uint64{10, 30, 90} {
		if err := log.Write(uint64(i+1), binary.BigEndian.AppendUint64(nil, id)); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct{ id, seq uint64 }{
		{0, 1}, {1, 1}, {10, 1}, {11, 2}, {30, 2}, {31, 3}, {90, 3}, {91, 4}, {^uint64(0), 4},
	} {
		if seq, err := positioner.SequenceFor(tc.id); err != nil || seq != tc.seq {
			t.Fatal(tc, seq, err)
		}
	}
	if err := log.TruncateFront(2); err != nil {
		t.Fatal(err)
	}
	calls = 0
	fail = true
	if seq, err := positioner.SequenceFor(0); err != nil || seq != 2 || calls != 0 {
		t.Fatal("head resolution decoded payload", seq, calls, err)
	}
	if _, err := positioner.SequenceFor(30); !errors.Is(err, errDecode) {
		t.Fatal("hidden decode failure", err)
	}
	fail = false
	if seq, err := positioner.SequenceFor(10); err != nil || seq != 2 {
		t.Fatal(seq, err)
	}
	if err := log.Write(4, binary.BigEndian.AppendUint64(nil, 100)); err != nil {
		t.Fatal(err)
	}
	if seq, err := positioner.SequenceFor(91); err != nil || seq != 4 {
		t.Fatal(seq, err)
	}
	if err := positioner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := positioner.SequenceFor(0); !errors.Is(err, eventlog.ErrClosed) {
		t.Fatal(err)
	}
}

func TestLegacyTailReadPreservesPosition(t *testing.T) {
	log, err := native.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	calls := 0
	c := NewLegacyCursor(log, func(raw []byte) (uint64, uint64, error) {
		calls++
		id := binary.BigEndian.Uint64(raw)
		return id, id, nil
	})
	defer func() { _ = c.Close() }()
	if _, err := c.Last(); !errors.Is(err, eventlog.ErrNotFound) {
		t.Fatal(err)
	}
	for i, id := range []uint64{10, 30, 90} {
		if err := log.Write(uint64(i+1), binary.BigEndian.AppendUint64(nil, id)); err != nil {
			t.Fatal(err)
		}
	}
	if id, err := c.Seek(10); err != nil || id != 10 {
		t.Fatal(id, err)
	}
	before := calls
	if id, err := c.Last(); err != nil || id != 90 || calls != before+1 {
		t.Fatal(id, calls, err)
	}
	if id, err := c.Seek(11); err != nil || id != 30 || calls != before+2 {
		t.Fatal("tail read changed sequential hints", id, calls, err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Last(); !errors.Is(err, eventlog.ErrClosed) {
		t.Fatal(err)
	}
}

func TestLegacyReverseScanBoundsAndFailures(t *testing.T) {
	log, err := native.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	fail := false
	errDecode := errors.New("decode failed")
	c := NewLegacyCursor(log, func(raw []byte) (uint64, uint64, error) {
		id := binary.BigEndian.Uint64(raw)
		if fail && id == 30 {
			return 0, 0, errDecode
		}
		return id, id, nil
	})
	defer func() { _ = c.Close() }()
	scan := func(limit int, want []uint64) {
		t.Helper()
		var got []uint64
		count, err := c.ScanReverse(limit, func(id uint64) (bool, error) { got = append(got, id); return true, nil })
		if err != nil || count != len(want) || !slices.Equal(got, want) {
			t.Fatal(count, got, err)
		}
	}
	scan(10, nil)
	for i, id := range []uint64{10, 30, 90} {
		if err := log.Write(uint64(i+1), binary.BigEndian.AppendUint64(nil, id)); err != nil {
			t.Fatal(err)
		}
	}
	scan(0, nil)
	scan(2, []uint64{90, 30})
	scan(10, []uint64{90, 30, 10})
	if err := log.TruncateFront(2); err != nil {
		t.Fatal(err)
	}
	scan(10, []uint64{90, 30})
	var got []uint64
	count, err := c.ScanReverse(10, func(id uint64) (bool, error) {
		got = append(got, id)
		if id == 90 {
			if err := log.Write(4, binary.BigEndian.AppendUint64(nil, 100)); err != nil {
				return false, err
			}
		}
		return true, nil
	})
	if err != nil || count != 2 || !slices.Equal(got, []uint64{90, 30}) {
		t.Fatal(count, got, err)
	}
	count, err = c.ScanReverse(10, func(uint64) (bool, error) { return false, nil })
	if err != nil || count != 1 {
		t.Fatal(count, err)
	}
	errVisit := errors.New("callback failed")
	count, err = c.ScanReverse(10, func(uint64) (bool, error) { return false, errVisit })
	if !errors.Is(err, errVisit) || count != 1 {
		t.Fatal(count, err)
	}
	fail = true
	count, err = c.ScanReverse(10, func(uint64) (bool, error) { return true, nil })
	if !errors.Is(err, errDecode) || count != 3 {
		t.Fatal(count, err)
	}
}
