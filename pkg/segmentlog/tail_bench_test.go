package segmentlog

import (
	"bytes"
	"fmt"
	"testing"
)

func BenchmarkTailScan(b *testing.B) {
	for _, count := range []int{16, 4096} {
		for _, deliver := range []bool{false, true} {
			b.Run(fmt.Sprintf("records=%d/deliver=%t", count, deliver), func(b *testing.B) {
				records := make([]Record, count)
				for i := range records {
					records[i] = Record{uint64(i * 2), bytes.Repeat([]byte("x"), 32)}
				}
				var buf bytes.Buffer
				if err := WriteTailHeader(&buf, 0); err != nil {
					b.Fatal(err)
				}
				buf.Write(encodeTailGroup(records, count*48))
				reader := bytes.NewReader(buf.Bytes())
				var visit func(Record) error
				seen := 0
				if deliver {
					visit = func(r Record) error {
						if r.ID != uint64(seen*2) || len(r.Payload) != 32 {
							b.Fatal(r)
						}
						seen++
						return nil
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					seen = 0
					state, err := ScanTail(reader, int64(buf.Len()), visit)
					if err != nil || state.Count != uint64(count) || state.End != int64(buf.Len()) || (deliver && seen != count) {
						b.Fatal(state, seen, err)
					}
				}
			})
		}
	}
}
