package segmentlog

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

func TestTailDescriptorReuseOwnershipAndCorruption(t *testing.T) {
	var data bytes.Buffer
	if err := WriteTailHeader(&data, 100); err != nil {
		t.Fatal(err)
	}
	expected := make([]Record, 0, 15)
	for _, count := range []int{2, 8, 1, 4} {
		records := make([]Record, count)
		for i := range records {
			records[i] = Record{100 + uint64(len(expected)*3), []byte("value")}
			expected = append(expected, records[i])
		}
		data.Write(encodeTailGroup(records, count*21))
	}
	goodEnd := data.Len()
	bad := encodeTailGroup([]Record{{150, []byte("first")}, {153, []byte("last")}}, 41)
	bad[len(bad)-groupTrailerSize-1] ^= 1 // Corrupt only the final frame CRC.
	format.LE.PutUint32(bad[len(bad)-4:], format.CRC(bad[:len(bad)-4]))
	data.Write(bad)
	for _, corrupt := range []bool{false, true} {
		size := goodEnd
		if corrupt {
			size = data.Len()
		}
		retained := make([]Record, 0, len(expected))
		state, err := ScanTail(bytes.NewReader(data.Bytes()), int64(size), func(r Record) error { retained = append(retained, r); return nil })
		if (corrupt && !errors.Is(err, ErrCorrupt)) || (!corrupt && err != nil) || state.Count != uint64(len(expected)) || state.End != int64(goodEnd) || len(retained) != len(expected) {
			t.Fatal(corrupt, state, len(retained), err)
		}
		for i, r := range retained {
			if r.ID != expected[i].ID || !bytes.Equal(r.Payload, expected[i].Payload) {
				t.Fatal("overwritten retained record", i)
			}
		}
	}
}

func BenchmarkMultiGroupTailScan(b *testing.B) {
	for _, size := range []int{32, 4080} {
		for _, deliver := range []bool{false, true} {
			b.Run(fmt.Sprintf("payload=%d/deliver=%t", size, deliver), func(b *testing.B) {
				const groups = 80
				perGroup := (256 << 10) / (size + 16)
				payload := bytes.Repeat([]byte("x"), size)
				var data bytes.Buffer
				if err := WriteTailHeader(&data, 0); err != nil {
					b.Fatal(err)
				}
				for g := range groups {
					records := make([]Record, perGroup)
					for i := range records {
						records[i] = Record{uint64((g*perGroup + i) * 3), payload}
					}
					data.Write(encodeTailGroup(records, perGroup*(size+16)))
				}
				count := groups * perGroup
				b.ReportAllocs()
				b.SetBytes(int64(data.Len()))
				for b.Loop() {
					seen := 0
					var visit func(Record) error
					if deliver {
						visit = func(r Record) error {
							if r.ID != uint64(seen*3) || !bytes.Equal(r.Payload, payload) {
								b.Fatal(seen)
							}
							seen++
							return nil
						}
					}
					state, err := ScanTail(bytes.NewReader(data.Bytes()), int64(data.Len()), visit)
					if err != nil || state.Count != uint64(count) || state.End != int64(data.Len()) || (deliver && seen != count) {
						b.Fatal(state, seen, err)
					}
				}
			})
		}
	}
}
