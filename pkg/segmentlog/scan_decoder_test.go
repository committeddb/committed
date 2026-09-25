package segmentlog

import (
	"bytes"
	"errors"
	"fmt"
	"iter"
	"math/rand/v2"
	"testing"

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

func TestIndexedScanMixedBlockOwnership(t *testing.T) {
	random := rand.New(rand.NewPCG(1, 2))
	noise := make([]byte, 8192)
	for i := range noise {
		noise[i] = byte(random.Uint32())
	}
	records := []Record{
		{0, bytes.Repeat([]byte("a"), 4096)},
		{10, noise},
		{20, bytes.Repeat(noise, 4)},
		{30, bytes.Repeat([]byte("b"), 4096)},
	}
	data := compressed(t, ZstdDefault, records...)
	s := open(t, data)
	if len(s.blocks) != 4 || s.blocks[0].codec != format.Zstd || s.blocks[1].codec != format.Plain ||
		s.blocks[2].codec != format.Zstd || s.blocks[3].codec != format.Zstd ||
		s.blocks[2].size <= s.blocks[0].size || s.blocks[3].size >= s.blocks[2].size {
		t.Fatal("fixture must mix plain storage with growing and shrinking compressed blocks")
	}
	retained := collect(t, s)
	for i, r := range retained {
		if r.ID != records[i].ID || !bytes.Equal(r.Payload, records[i].Payload) {
			t.Fatal("retained payload changed", i)
		}
	}
	// A later failed read must expose none of that block, even after input reuse.
	last := s.blocks[3]
	data[int(last.offset)] ^= 0xff
	seen := 0
	var scanErr error
	for r, err := range s.Records() {
		if err != nil {
			scanErr = err
			break
		}
		if seen >= 3 || r.ID != records[seen].ID || !bytes.Equal(r.Payload, records[seen].Payload) {
			t.Fatal("unexpected record before corrupt block", seen)
		}
		seen++
	}
	if seen != 3 || !errors.Is(scanErr, ErrCorrupt) {
		t.Fatal(seen, scanErr)
	}
	for i, r := range retained {
		if !bytes.Equal(r.Payload, records[i].Payload) {
			t.Fatal("failed scan changed retained payload", i)
		}
	}
}

func TestIndexedScanDecoderOwnership(t *testing.T) {
	records := []Record{{0, bytes.Repeat([]byte("a"), 4096)}, {10, bytes.Repeat([]byte("b"), 32)}, {20, bytes.Repeat([]byte("c"), 9000)}, {30, bytes.Repeat([]byte("d"), 16)}}
	s := open(t, compressed(t, ZstdDefault, records...))
	// Two suspended scans must own independent decoder state and outputs.
	nextA, stopA := iter.Pull2(s.Records())
	defer stopA()
	nextB, stopB := iter.Pull2(s.Records())
	defer stopB()
	retained := make([]Record, 0, len(records))
	for i := range records {
		a, err, ok := nextA()
		if err != nil || !ok {
			t.Fatal(i, err, ok)
		}
		b, err, ok := nextB()
		if err != nil || !ok || !bytes.Equal(b.Payload, records[i].Payload) {
			t.Fatal(i, err, ok)
		}
		b.Payload[0] ^= 0xff
		retained = append(retained, a)
	}
	stopA()
	stopB()
	for i, r := range retained {
		if r.ID != records[i].ID || !bytes.Equal(r.Payload, records[i].Payload) {
			t.Fatal("retained payload changed", i)
		}
	}
	// Early termination also closes the decoder without invalidating output.
	var first Record
	for r, err := range s.Records() {
		if err != nil {
			t.Fatal(err)
		}
		first = r
		break
	}
	for r, err := range s.Records() {
		if err != nil {
			t.Fatal(err)
		}
		r.Payload[0] ^= 0xff
	}
	if !bytes.Equal(first.Payload, records[0].Payload) {
		t.Fatal("early-stop payload changed")
	}
}

func BenchmarkIndexedScan(b *testing.B) {
	for _, codec := range []Compression{NoCompression, ZstdDefault} {
		for _, size := range []int{32, 4080} {
			b.Run(fmt.Sprintf("codec=%d/payload=%d", codec, size), func(b *testing.B) {
				count := (20 << 20) / (size + 16)
				payload := bytes.Repeat([]byte("x"), size)
				input := func(yield func(Record, error) bool) {
					for i := range count {
						if !yield(Record{uint64(i * 3), payload}, nil) {
							return
						}
					}
				}
				var buf bytes.Buffer
				if err := WriteSegment(&buf, Coverage{0, uint64(count * 3)}, input, Options{Compression: codec}); err != nil {
					b.Fatal(err)
				}
				s := open(b, buf.Bytes())
				b.ReportAllocs()
				b.SetBytes(int64(count * (size + 16)))
				for b.Loop() {
					seen := 0
					for r, err := range s.Records() {
						if err != nil || r.ID != uint64(seen*3) || !bytes.Equal(r.Payload, payload) {
							b.Fatal(seen, err)
						}
						seen++
					}
					if seen != count {
						b.Fatal(seen)
					}
				}
			})
		}
	}
}
