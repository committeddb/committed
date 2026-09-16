package segmentlog

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestLogScanRangesAndErasure(t *testing.T) {
	log := newLog(t, 40)
	records := []Record{{0, []byte("zero")}, {10, []byte("ten")}, {20, []byte("twenty")}, {100, []byte("hundred")}}
	if err := log.Append(records); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Rewrite(t.Context(), 1, func(r Record) ([]byte, bool, error) { return r.Payload, r.ID != 10, nil }); err != nil {
		t.Fatal(err)
	}
	log = reopenLog(t, log)
	for _, tt := range []struct {
		bounds Coverage
		want   []uint64
	}{
		{Coverage{0, 101}, []uint64{0, 20, 100}},
		{Coverage{1, 100}, []uint64{20}},
		{Coverage{10, 20}, nil},
		{Coverage{100, 101}, []uint64{100}},
		{Coverage{20, 20}, nil},
		{Coverage{101, ^uint64(0)}, nil},
	} {
		var got []uint64
		err := log.Scan(t.Context(), tt.bounds, func(r Record) error { got = append(got, r.ID); return nil })
		if err != nil || !reflect.DeepEqual(got, tt.want) {
			t.Fatal(tt.bounds, got, err)
		}
	}
}

func TestLogScanStopsAndRemainsUsable(t *testing.T) {
	log := rotatedLog(t)
	boom := errors.New("visitor failed")
	calls := 0
	if err := log.Scan(t.Context(), Coverage{0, 100}, func(Record) error { calls++; return boom }); !errors.Is(err, boom) || calls != 1 {
		t.Fatal(calls, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls = 0
	if err := log.Scan(ctx, Coverage{0, 100}, func(Record) error { calls++; cancel(); return nil }); !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatal(calls, err)
	}
	if err := log.Scan(t.Context(), Coverage{6, 7}, func(Record) error { return errStopScan }); !errors.Is(err, errStopScan) {
		t.Fatal("swallowed callback error", err)
	}
	if err := log.Scan(t.Context(), Coverage{9, 1}, func(Record) error { return nil }); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := log.Scan(t.Context(), Coverage{0, 1}, nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := log.Append([]Record{{7, nil}}); err != nil {
		t.Fatal("scan failure poisoned log", err)
	}
}

func TestSegmentRangeScanSkipsUnselectedBlocks(t *testing.T) {
	data := encode(t, Record{0, []byte("zero")}, Record{100, bytes.Repeat([]byte("a"), 40)}, Record{200, []byte("last")})
	segment := open(t, data)
	// Corrupt an earlier block, retaining valid metadata. A later range should
	// use the index without decoding that block; a full scan must report corruption.
	data[segment.blocks[0].offset] ^= 1
	var got []uint64
	for r, err := range segment.recordsIn(Coverage{100, 201}) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, r.ID)
	}
	if !reflect.DeepEqual(got, []uint64{100, 200}) {
		t.Fatal(got)
	}
	if err := segment.Verify(); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}

// A requested prefix must not expose records before the rest of its block has
// been checked, including corrupt frames outside the requested ID interval.
func TestSegmentScanValidatesBeforeDelivery(t *testing.T) {
	for _, codec := range []Compression{NoCompression, ZstdDefault} {
		segment := seekValidationBlock(t, codec, true)
		delivered, failures := 0, 0
		for _, err := range segment.recordsIn(Coverage{0, 1}) {
			if err == nil {
				delivered++
			} else {
				if !errors.Is(err, ErrCorrupt) {
					t.Fatal(err)
				}
				failures++
			}
		}
		if delivered != 0 || failures != 1 {
			t.Fatal("scan exposed an unverified prefix", delivered, failures)
		}
		segment = seekValidationBlock(t, codec, false)
		for record, err := range segment.recordsIn(Coverage{0, 3}) {
			if err != nil || record.ID != 0 {
				t.Fatal(record, err)
			}
			delivered++
			break
		}
		if delivered != 1 {
			t.Fatal("scan did not stop after consumer exit")
		}
	}
}
