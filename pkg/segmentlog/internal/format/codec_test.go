package format

import (
	"bytes"
	"errors"
	"testing"
)

func TestDecodeBounds(t *testing.T) {
	e, err := NewEncoder(2)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	input := bytes.Repeat([]byte("a"), 4096)
	codec, stored := e.Encode(input)
	if codec != Zstd {
		t.Fatal("expected compression")
	}
	for _, n := range []uint32{0, 15, 16, 4095, 4097, MaxBlock + 1, ^uint32(0)} {
		if _, err := Decode(codec, stored, n); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("size %d: %v", n, err)
		}
	}
	if _, err := Decode(99, stored, 4096); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
	// Bound total output across concatenated frames as well as single frames.
	joined := append(bytes.Clone(stored), stored...)
	if _, err := Decode(codec, joined, 4096); !errors.Is(err, ErrCorrupt) {
		t.Fatal("concatenated output escaped bound", err)
	}
	if _, err := Decode(Plain, input, 4095); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}

func FuzzDecode(f *testing.F) {
	e, err := NewEncoder(2)
	if err != nil {
		f.Fatal(err)
	}
	_, stored := e.Encode(bytes.Repeat([]byte("payload"), 100))
	e.Close()
	f.Add(stored, uint32(700))
	f.Add([]byte("invalid zstd"), uint32(16))
	f.Fuzz(func(t *testing.T, stored []byte, n uint32) {
		// Keep fuzz iterations small; unit tests exercise the full format limits.
		if n > 64<<10 {
			n %= 64 << 10
		}
		if n < 16 {
			n = 16
		}
		data, err := Decode(Zstd, stored, n)
		if err == nil && len(data) != int(n) {
			t.Fatal("decoded length mismatch")
		}
	})
}
