package segmentlog

import (
	"bytes"
	"fmt"
	"io"
	"iter"
	"testing"
)

type rewriteReadCounter struct {
	*bytes.Reader
	bytes, calls int64
}

func (r *rewriteReadCounter) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.Reader.ReadAt(p, off)
	r.bytes += int64(n)
	r.calls++
	return n, err
}

// BenchmarkRewriteSourceReads includes opening and change detection, but not
// replacement encoding or publication. ReadAt counts are logical reads from an
// in-memory source, not physical device I/O.
func BenchmarkRewriteSourceReads(b *testing.B) {
	const count, payloadSize = 5120, 4080
	records := make([]Record, count)
	for i := range records {
		records[i] = Record{uint64(i * 3), bytes.Repeat([]byte("x"), payloadSize)}
	}
	coverage := Coverage{0, count * 3}
	for _, encoding := range []string{"append", "indexed-plain", "indexed-zstd"} {
		var buf bytes.Buffer
		ref := SegmentRef{Coverage: coverage, Count: count}
		if encoding == "append" {
			if err := WriteTailHeader(&buf, 0); err != nil {
				b.Fatal(err)
			}
			for start := 0; start < count; start += 64 {
				buf.Write(encodeTailGroup(records[start:start+64], 64*(payloadSize+16)))
			}
			ref.TailBytes = int64(buf.Len())
		} else {
			codec := NoCompression
			if encoding == "indexed-zstd" {
				codec = ZstdDefault
			}
			if err := WriteSegment(&buf, coverage, sequence(records...), Options{Compression: codec}); err != nil {
				b.Fatal(err)
			}
		}
		for _, mode := range []string{"noop", "first", "last"} {
			b.Run(fmt.Sprintf("%s/%s", encoding, mode), func(b *testing.B) {
				reader := &rewriteReadCounter{Reader: bytes.NewReader(buf.Bytes())}
				b.ReportAllocs()
				b.SetBytes(count * (payloadSize + 16))
				for b.Loop() {
					source, err := openRangeSource(reader, int64(buf.Len()), ref)
					if err != nil {
						b.Fatal(err)
					}
					calls, delivered := 0, 0
					changedID := uint64(0)
					if mode == "last" {
						changedID = (count - 1) * 3
					}
					changed, err := prepareRewrite(b.Context(), source.Records(), func(r Record) ([]byte, bool, error) {
						calls++
						if mode != "noop" && r.ID == changedID {
							r.Payload[0] = 'y'
						}
						return r.Payload, true, nil
					}, func(output iter.Seq2[Record, error]) error {
						for r, err := range output {
							if err != nil {
								return err
							}
							want := byte('x')
							if r.ID == changedID {
								want = 'y'
							}
							if r.ID != uint64(delivered*3) || len(r.Payload) != payloadSize || r.Payload[0] != want {
								return fmt.Errorf("unexpected output record %d", delivered)
							}
							delivered++
						}
						return nil
					})
					wantDelivered := count
					if mode == "noop" {
						wantDelivered = 0
					}
					if err != nil || changed != (mode != "noop") || calls != count || delivered != wantDelivered {
						b.Fatal(changed, calls, delivered, err)
					}
				}
				b.ReportMetric(float64(reader.bytes)/float64(b.N), "read-B/op")
				b.ReportMetric(float64(reader.calls)/float64(b.N), "read-calls/op")
				b.ReportMetric(float64(buf.Len()), "source-B")
			})
		}
	}
}

var _ io.ReaderAt = (*rewriteReadCounter)(nil)
