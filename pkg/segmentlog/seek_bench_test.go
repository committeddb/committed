package segmentlog

import (
	"bytes"
	"fmt"
	"testing"
)

// BenchmarkSegmentSeek isolates one sparse lookup inside an already-open block.
// It includes full block validation; metadata opening and file I/O are excluded.
func BenchmarkSegmentSeek(b *testing.B) {
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
				// An absent odd ID must resolve to the following even ID.
				query := uint64(count - 1)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					record, err := segment.Seek(query)
					if err != nil || record.ID != query+1 || len(record.Payload) != 32 {
						b.Fatal(record.ID, err)
					}
				}
			})
		}
	}
}
