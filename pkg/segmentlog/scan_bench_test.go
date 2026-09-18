package segmentlog

import (
	"bytes"
	"fmt"
	"testing"
)

func BenchmarkSegmentScan(b *testing.B) {
	for _, codec := range []Compression{NoCompression, ZstdDefault} {
		for _, count := range []int{16, 4096} {
			b.Run(fmt.Sprintf("codec=%d/records=%d", codec, count), func(b *testing.B) {
				records := make([]Record, count)
				for i := range records {
					records[i] = Record{ID: uint64(i * 2), Payload: bytes.Repeat([]byte("x"), 32)}
				}
				var buf bytes.Buffer
				if err := WriteSegment(&buf, Coverage{0, uint64(count * 2)}, sequence(records...), Options{Compression: codec}); err != nil {
					b.Fatal(err)
				}
				segment, err := OpenSegment(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					seen := 0
					for r, err := range segment.Records() {
						if err != nil || r.ID != uint64(seen*2) || len(r.Payload) != 32 {
							b.Fatal(r.ID, err)
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
