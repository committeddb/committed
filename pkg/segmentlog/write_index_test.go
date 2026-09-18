package segmentlog

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"iter"
	"testing"

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

func emptyPayloadRecords(count int) iter.Seq2[Record, error] {
	return func(yield func(Record, error) bool) {
		for i := range count {
			if !yield(Record{ID: uint64(i * 3)}, nil) {
				return
			}
		}
	}
}

func TestWriteSegmentBlockLimit(t *testing.T) {
	const count = format.MaxBlocks
	coverage := Coverage{0, (count + 1) * 3}
	opts := Options{BlockSize: format.FrameOverhead}
	var buf bytes.Buffer
	if err := WriteSegment(&buf, coverage, emptyPayloadRecords(count), opts); err != nil {
		t.Fatal(err)
	}
	s := open(t, buf.Bytes())
	if len(s.blocks) != count || s.Count() != count {
		t.Fatal("incorrect block or record count")
	}
	if err := s.Verify(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Read((count - 1) * 3); err != nil {
		t.Fatal("last record", err)
	}
	buf.Reset()
	if err := WriteSegment(&buf, coverage, emptyPayloadRecords(count+1), opts); !errors.Is(err, ErrInvalid) {
		t.Fatal("accepted too many blocks", err)
	}
	// Failure leaves only the header and permitted data blocks, no index/footer.
	if buf.Len() != format.HeaderSize+count*format.FrameOverhead {
		t.Fatal("unexpected partial output size", buf.Len())
	}
}

func BenchmarkWriteSegmentIndex(b *testing.B) {
	for _, count := range []int{0, 1, 80, 4096, format.MaxBlocks} {
		b.Run(fmt.Sprintf("blocks=%d", count), func(b *testing.B) {
			input := emptyPayloadRecords(count)
			coverage := Coverage{0, uint64(count*3 + 1)}
			b.ReportAllocs()
			for b.Loop() {
				if err := WriteSegment(io.Discard, coverage, input, Options{BlockSize: format.FrameOverhead}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
