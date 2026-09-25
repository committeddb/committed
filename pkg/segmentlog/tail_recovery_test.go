package segmentlog

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"testing"
)

func TestTailScanRetainsPayloadsAcrossGroups(t *testing.T) {
	var data bytes.Buffer
	if err := WriteTailHeader(&data, 0); err != nil {
		t.Fatal(err)
	}
	// Grow, shrink, and reuse group sizes to exercise buffer ownership.
	records := []Record{{0, bytes.Repeat([]byte("a"), 32)}, {5, bytes.Repeat([]byte("b"), 128)}, {10, []byte("c")}, {15, []byte("d")}}
	for _, record := range records {
		data.Write(encodeTailGroup([]Record{record}, len(record.Payload)+16))
	}
	var retained []Record
	state, err := ScanTail(bytes.NewReader(data.Bytes()), int64(data.Len()), func(r Record) error {
		retained = append(retained, r)
		return nil
	})
	if err != nil || len(retained) != len(records) {
		t.Fatal(state, err)
	}
	for i, record := range retained {
		if record.ID != records[i].ID || !bytes.Equal(record.Payload, records[i].Payload) {
			t.Fatal("overwritten retained record", i)
		}
	}
	digest := sha256.New()
	checked, err := scanTailHashed(bytes.NewReader(data.Bytes()), int64(data.Len()), nil, nil, nil, digest)
	if err != nil || checked != state {
		t.Fatal(checked, state, err)
	}
	want := sha256.Sum256(data.Bytes())
	if !bytes.Equal(digest.Sum(nil), want[:]) {
		t.Fatal("wrong physical digest")
	}
}

// BenchmarkTailRecoveryScan isolates validation and digest reconstruction from
// filesystem reads and syncs. The full rollover benchmark measures actual opens.
func BenchmarkTailRecoveryScan(b *testing.B) {
	const target = 20 << 20
	const frameBytes = 4096
	for _, batch := range []int{1, 64} {
		b.Run(fmt.Sprintf("records-per-group=%d", batch), func(b *testing.B) {
			var data bytes.Buffer
			if err := WriteTailHeader(&data, 0); err != nil {
				b.Fatal(err)
			}
			var next uint64
			for range target / frameBytes / batch {
				records := make([]Record, batch)
				for i := range records {
					records[i] = Record{next, bytes.Repeat([]byte("r"), frameBytes-16)}
					next++
				}
				data.Write(encodeTailGroup(records, batch*frameBytes))
			}
			expected := sha256.Sum256(data.Bytes())
			b.ReportAllocs()
			b.SetBytes(int64(data.Len()))
			for b.Loop() {
				digest := sha256.New()
				state, err := scanTailHashed(bytes.NewReader(data.Bytes()), int64(data.Len()), nil, nil, nil, digest)
				if err != nil || state.Count != next || state.End != int64(data.Len()) || state.Framed != target || !bytes.Equal(digest.Sum(nil), expected[:]) {
					b.Fatal(state, err)
				}
			}
		})
	}
}
