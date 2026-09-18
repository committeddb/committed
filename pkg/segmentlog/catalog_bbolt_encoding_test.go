package segmentlog

import (
	"bytes"
	"errors"
	"testing"

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

func encodingHeader() boltHeader {
	return boltHeader{Version: 1, Catalog: Catalog{
		History: [16]byte{1}, Revision: 1, SegmentBytes: 16,
		Active: &TailRef{File: "tail.active"},
	}}
}

func TestBoltMetadataEncoding(t *testing.T) {
	h := encodingHeader()
	b, err := boltEncode(h)
	if err != nil {
		t.Fatal(err)
	}
	for i := range b {
		bad := bytes.Clone(b)
		bad[i] ^= 1
		if err := boltDecode(bad, new(boltHeader)); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("accepted corrupt byte %d: %v", i, err)
		}
		if err := boltDecode(b[:i], new(boltHeader)); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("accepted truncation %d: %v", i, err)
		}
	}
	// Correct checksums must not make ambiguous or noncanonical JSON valid.
	for _, prefix := range []string{`{"Version":99,`, `{"Unknown":true,`, "{ "} {
		payload := append([]byte(prefix), b[1:len(b)-4]...)
		bad := format.LE.AppendUint32(payload, format.CRC(payload))
		if err := boltDecode(bad, new(boltHeader)); !errors.Is(err, ErrCorrupt) {
			t.Fatal("accepted ambiguous or noncanonical metadata", err)
		}
	}
	h.Version++
	if err := validateBoltHeader(h); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
}

func FuzzBoltHeader(f *testing.F) {
	b, err := boltEncode(encodingHeader())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(b)
	f.Fuzz(func(t *testing.T, b []byte) {
		var h boltHeader
		if err := boltDecode(b, &h); err != nil {
			return
		}
		if err := validateBoltHeader(h); err != nil {
			return
		}
		encoded, err := boltEncode(h)
		if err != nil || !bytes.Equal(encoded, b) {
			t.Fatal("header roundtrip mismatch", err)
		}
	})
}
