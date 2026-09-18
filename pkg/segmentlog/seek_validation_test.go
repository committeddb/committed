package segmentlog

import (
	"bytes"
	"errors"
	"testing"

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

func seekValidationBlock(t *testing.T, compression Compression, corruptLast bool) *Segment {
	t.Helper()
	var frames []byte
	for _, r := range []Record{{ID: 0}, {ID: 2, Payload: []byte("middle")}, {ID: 4, Payload: bytes.Repeat([]byte("last"), 128)}} {
		frames = format.AppendFrame(frames, r.ID, r.Payload)
	}
	if corruptLast {
		frames[len(frames)-1] ^= 1
	}
	encoder, err := format.NewEncoder(int(compression))
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	codec, stored := encoder.Encode(frames)
	stored = bytes.Clone(stored)
	if compression != NoCompression && codec != format.Zstd {
		t.Fatal("fixture did not compress")
	}
	// The outer checksum is valid; only the final record's CRC is corrupt.
	b := block{first: 0, last: 4, size: uint32(len(stored)), count: 3, crc: format.CRC(stored), decoded: uint32(len(frames)), codec: codec}
	return &Segment{r: bytes.NewReader(stored), coverage: Coverage{0, 6}, blocks: []block{b}, count: 3}
}

func TestSeekValidatesBeyondMatch(t *testing.T) {
	for _, codec := range []Compression{NoCompression, ZstdDefault} {
		segment := seekValidationBlock(t, codec, true)
		for _, id := range []uint64{0, 1, 2, 3, 4} {
			record, err := segment.Seek(id)
			if !errors.Is(err, ErrCorrupt) || record.Payload != nil {
				t.Fatalf("codec %d, seek %d exposed corrupt block: %+v %v", codec, id, record, err)
			}
		}
		if _, err := segment.Read(0); !errors.Is(err, ErrCorrupt) {
			t.Fatal("exact read hid trailing corruption", err)
		}
	}
}

func TestSeekSparsePayloadOwnership(t *testing.T) {
	for _, codec := range []Compression{NoCompression, ZstdDefault} {
		segment := seekValidationBlock(t, codec, false)
		for _, tc := range []struct{ query, want uint64 }{{0, 0}, {1, 2}, {2, 2}, {3, 4}, {4, 4}} {
			got, err := segment.Seek(tc.query)
			if err != nil || got.ID != tc.want {
				t.Fatal(tc, got, err)
			}
			if cap(got.Payload) != len(got.Payload) {
				t.Fatal("payload capacity escapes frame")
			}
		}
		if _, err := segment.Read(1); !errors.Is(err, ErrNotFound) {
			t.Fatal("exact lookup accepted a gap", err)
		}
		if _, err := segment.Seek(5); !errors.Is(err, ErrNotFound) {
			t.Fatal("seek past last record", err)
		}
		first, err := segment.Read(2)
		if err != nil {
			t.Fatal(err)
		}
		next, err := segment.Seek(1)
		if err != nil {
			t.Fatal(err)
		}
		next.Payload[0] = 'X'
		if string(first.Payload) != "middle" {
			t.Fatal("later read aliased earlier payload")
		}
		again, err := segment.Read(2)
		if err != nil || string(again.Payload) != "middle" {
			t.Fatal("caller mutation affected stored data", err)
		}
	}
}
