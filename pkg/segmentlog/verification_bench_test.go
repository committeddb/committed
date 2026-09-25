package segmentlog

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"testing"
)

// BenchmarkSegmentVerification isolates verification from filesystem sync and
// catalog publication. Every case is one block with many small records.
func BenchmarkSegmentVerification(b *testing.B) {
	for _, codec := range []Compression{NoCompression, ZstdDefault} {
		for _, count := range []int{16, 4096} {
			b.Run(fmt.Sprintf("codec=%d/records=%d", codec, count), func(b *testing.B) {
				records := make([]Record, count)
				for i := range records {
					records[i] = Record{ID: uint64(i * 2), Payload: bytes.Repeat([]byte("x"), 32)}
				}
				var buf bytes.Buffer
				coverage := Coverage{0, uint64(count * 2)}
				if err := WriteSegment(&buf, coverage, sequence(records...), Options{Compression: codec}); err != nil {
					b.Fatal(err)
				}
				raw := buf.Bytes()
				ref := SegmentRef{Coverage: coverage, Count: uint64(count), SHA256: sha256.Sum256(raw)}
				reader := bytes.NewReader(raw)
				b.SetBytes(int64(count * 32))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if err := verifySegmentDigest(reader, int64(len(raw)), ref); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkMultiBlockVerification measures a 20 MiB segment rather than a
// single block. Repeated payloads deliberately exercise effective compression.
func BenchmarkMultiBlockVerification(b *testing.B) {
	for _, codec := range []Compression{NoCompression, ZstdDefault} {
		b.Run(fmt.Sprintf("codec=%d", codec), func(b *testing.B) {
			const count = 5120
			payload := bytes.Repeat([]byte("x"), 4080)
			records := func(yield func(Record, error) bool) {
				for i := range count {
					if !yield(Record{uint64(i), payload}, nil) {
						return
					}
				}
			}
			var buf bytes.Buffer
			coverage := Coverage{0, count}
			if err := WriteSegment(&buf, coverage, records, Options{Compression: codec}); err != nil {
				b.Fatal(err)
			}
			raw := buf.Bytes()
			ref := SegmentRef{Coverage: coverage, Count: count, SHA256: sha256.Sum256(raw)}
			reader := bytes.NewReader(raw)
			segment, err := OpenSegment(reader, int64(len(raw)))
			if err != nil {
				b.Fatal(err)
			}
			for _, mode := range []string{"frames", "digest"} {
				b.Run(mode, func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(count * 4096)
					for b.Loop() {
						var err error
						if mode == "frames" {
							err = segment.Verify()
						} else {
							err = verifySegmentDigest(reader, int64(len(raw)), ref)
						}
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}
