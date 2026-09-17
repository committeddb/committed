package format

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
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

func TestDecoderReusePreservesBounds(t *testing.T) {
	e, err := NewEncoder(2)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	var d Decoder
	defer d.Close()
	for _, size := range []int{4096, 16384, 32, 8192, 16} {
		input := bytes.Repeat([]byte("a"), size)
		codec, stored := e.Encode(input)
		data, err := d.Decode(codec, stored, uint32(size))
		if err != nil || !bytes.Equal(data, input) {
			t.Fatal(size, err)
		}
		// A large retained output allocation must not admit oversized output
		// for a later block, including concatenated compressed frames.
		if size > FrameOverhead {
			if _, err := d.Decode(codec, stored, uint32(size-1)); !errors.Is(err, ErrCorrupt) {
				t.Fatal("accepted undersized declaration", size, err)
			}
		}
		if codec == Zstd {
			joined := append(bytes.Clone(stored), stored...)
			if _, err := d.Decode(codec, joined, uint32(size)); !errors.Is(err, ErrCorrupt) {
				t.Fatal("accepted concatenated output", size, err)
			}
			corrupt := bytes.Clone(stored)
			corrupt[len(corrupt)-1] ^= 1
			if _, err := d.Decode(codec, corrupt, uint32(size)); !errors.Is(err, ErrCorrupt) {
				t.Fatal("accepted corrupt frame", err)
			}
		}
		// Errors and intervening plain blocks must not contaminate later output.
		plain := bytes.Repeat([]byte("b"), FrameOverhead)
		if data, err := d.Decode(Plain, plain, FrameOverhead); err != nil || !bytes.Equal(data, plain) {
			t.Fatal(err)
		}
		data, err = d.Decode(codec, stored, uint32(size))
		if err != nil || !bytes.Equal(data, input) {
			t.Fatal("failed after reuse/error", size, err)
		}
	}
}

func TestStandaloneDecodeOwnsOutput(t *testing.T) {
	e, err := NewEncoder(2)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	first := bytes.Repeat([]byte("a"), 4096)
	codec, stored := e.Encode(first)
	data, err := Decode(codec, stored, uint32(len(first)))
	if err != nil {
		t.Fatal(err)
	}
	second := bytes.Repeat([]byte("b"), 4096)
	codec, stored = e.Encode(second)
	if _, err := Decode(codec, stored, uint32(len(second))); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, first) {
		t.Fatal("later decode changed retained output")
	}
}

func TestEncoderReusePreservesBytes(t *testing.T) {
	random := make([]byte, 256<<10)
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range random {
		random[i] = byte(rng.Uint32())
	}
	inputs := [][]byte{
		bytes.Repeat([]byte("a"), 4096), random,
		bytes.Repeat([]byte("large"), 200000), bytes.Repeat([]byte("b"), 32), random[:64],
	}
	for level := 1; level <= 4; level++ {
		t.Run(fmt.Sprint(level), func(t *testing.T) {
			e, err := NewEncoder(level)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			for _, input := range inputs {
				// EncodeAll with a nil destination is the previous allocation policy.
				expected := e.zstd.EncodeAll(input, nil)
				wantCodec := Zstd
				if len(expected) >= len(input) {
					expected, wantCodec = input, Plain
				}
				codec, stored := e.Encode(input)
				if codec != wantCodec || !bytes.Equal(stored, expected) {
					t.Fatal("changed encoding", level, len(input), codec, wantCodec)
				}
				decoded, err := Decode(codec, stored, uint32(len(input)))
				if err != nil || !bytes.Equal(decoded, input) {
					t.Fatal("roundtrip", level, len(input), err)
				}
			}
		})
	}
}
