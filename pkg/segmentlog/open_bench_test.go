package segmentlog

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

// BenchmarkOpenSegment isolates metadata parsing with one empty-payload record
// per block. It does not measure filesystem I/O or payload validation.
func BenchmarkOpenSegment(b *testing.B) {
	for _, count := range []int{0, 1, 80, 4096, format.MaxBlocks} {
		b.Run(fmt.Sprintf("blocks=%d", count), func(b *testing.B) {
			var buf bytes.Buffer
			coverage := Coverage{0, uint64(count*3 + 1)}
			input := func(yield func(Record, error) bool) {
				for i := range count {
					if !yield(Record{ID: uint64(i * 3)}, nil) {
						return
					}
				}
			}
			if err := WriteSegment(&buf, coverage, input, Options{BlockSize: format.FrameOverhead}); err != nil {
				b.Fatal(err)
			}
			reader := bytes.NewReader(buf.Bytes())
			b.ReportAllocs()
			for b.Loop() {
				s, err := OpenSegment(reader, int64(buf.Len()))
				if err != nil {
					b.Fatal(err)
				}
				if len(s.blocks) != count || s.Count() != uint64(count) || s.Coverage() != coverage {
					b.Fatal("incorrect segment metadata")
				}
			}
		})
	}
}
