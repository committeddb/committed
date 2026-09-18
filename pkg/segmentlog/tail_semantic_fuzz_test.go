package segmentlog

import (
	"bytes"
	"errors"
	"testing"

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

// Keep checksums valid so mutations reach semantic validation rather than
// stopping at the first checksum. The expected result comes from the input IDs.
func FuzzTailGroupValidation(f *testing.F) {
	for _, ids := range [][]uint64{{0}, {0, 10}, {10, 10}, {10, 0}, {^uint64(0)}} {
		var encoded []byte
		for _, id := range ids {
			encoded = format.LE.AppendUint64(encoded, id)
		}
		f.Add(encoded, uint32(len(ids)), ids[len(ids)-1])
		f.Add(encoded, uint32(0), ids[len(ids)-1])
		f.Add(encoded, uint32(len(ids)+1), ids[len(ids)-1])
		f.Add(encoded, uint32(len(ids)), ids[len(ids)-1]^1)
	}
	f.Fuzz(func(t *testing.T, encoded []byte, declaredCount uint32, declaredLast uint64) {
		count := min(len(encoded)/8, 32)
		if count == 0 {
			return
		}
		records := make([]Record, count)
		valid := uint64(declaredCount) == uint64(count)
		for i := range records {
			id := format.LE.Uint64(encoded[i*8:])
			records[i] = Record{id, bytes.Repeat([]byte{byte(i)}, 16)}
			if id == ^uint64(0) || (i > 0 && id <= records[i-1].ID) {
				valid = false
			}
		}
		valid = valid && declaredLast == records[count-1].ID
		group := encodeTailGroup(records, count*32)
		format.LE.PutUint32(group[12:], declaredCount)
		format.LE.PutUint64(group[16:], declaredLast)
		format.LE.PutUint32(group[28:], format.CRC(group[:28]))
		format.LE.PutUint32(group[len(group)-4:], format.CRC(group[:len(group)-4]))
		var data bytes.Buffer
		if err := WriteTailHeader(&data, 0); err != nil {
			t.Fatal(err)
		}
		data.Write(group)
		reader := bytes.NewReader(data.Bytes())
		verified, verifyErr := ScanTail(reader, int64(data.Len()), nil)
		delivered := 0
		scanned, scanErr := ScanTail(reader, int64(data.Len()), func(r Record) error {
			if !valid || delivered >= count || r.ID != records[delivered].ID || !bytes.Equal(r.Payload, records[delivered].Payload) {
				t.Fatal("unexpected delivery", delivered, r)
			}
			delivered++
			return nil
		})
		if scanned != verified {
			t.Fatal("verification and delivery disagree", verified, scanned)
		}
		if valid {
			want := TailState{Start: 0, Last: declaredLast, Count: uint64(count), OriginalCount: uint64(count), Framed: uint64(count * 32), End: int64(data.Len()), HasRecords: true}
			if verifyErr != nil || scanErr != nil || delivered != count || scanned != want {
				t.Fatal("rejected valid group", scanned, want, delivered, verifyErr, scanErr)
			}
		} else if !errors.Is(verifyErr, ErrCorrupt) || !errors.Is(scanErr, ErrCorrupt) || delivered != 0 || scanned != (TailState{End: tailHeaderSize}) {
			t.Fatal("accepted invalid group", scanned, delivered, verifyErr, scanErr)
		}
	})
}
