package segmentlog

import (
	"bytes"
	"errors"
	"testing"

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

func FuzzSegmentIndexValidation(f *testing.F) {
	for _, ids := range [][]uint64{{0}, {0, 10}, {10, 10}, {10, 0}, {^uint64(0)}} {
		var encoded []byte
		for _, id := range ids {
			encoded = format.LE.AppendUint64(encoded, id)
		}
		f.Add(encoded, uint32(len(ids)), ids[0], ids[len(ids)-1], uint64(1))
		f.Add(encoded, uint32(len(ids)+1), ids[0], ids[len(ids)-1], uint64(0))
		f.Add(encoded, uint32(len(ids)), ids[0]^1, ids[len(ids)-1], uint64(0))
		f.Add(encoded, uint32(len(ids)), ids[0], ids[len(ids)-1]^1, uint64(0))
	}
	f.Fuzz(func(t *testing.T, encoded []byte, declaredCount uint32, first, last, query uint64) {
		count := min(len(encoded)/8, 32)
		if count == 0 {
			return
		}
		records := make([]Record, count)
		template := make([]Record, count)
		var frames []byte
		valid := uint64(declaredCount) == uint64(count)
		for i := range records {
			id := format.LE.Uint64(encoded[i*8:])
			payload := bytes.Repeat([]byte{byte(i)}, 16)
			records[i] = Record{id, payload}
			template[i] = Record{uint64(i), payload}
			frames = format.AppendFrame(frames, id, payload)
			if id == ^uint64(0) || (i > 0 && id <= records[i-1].ID) {
				valid = false
			}
		}
		valid = valid && first == records[0].ID && last == records[count-1].ID
		// Start with a valid one-block file, then replace its frames and index
		// claims. Repair all checksums to reach validation beyond byte integrity.
		var buf bytes.Buffer
		if err := WriteSegment(&buf, Coverage{0, ^uint64(0)}, sequence(template...), Options{}); err != nil {
			t.Fatal(err)
		}
		data := buf.Bytes()
		copy(data[format.HeaderSize:], frames)
		index := data[format.HeaderSize+len(frames) : len(data)-format.FooterSize]
		if len(index) != format.IndexEntrySize {
			t.Fatal("fixture must have one block")
		}
		format.LE.PutUint64(index, first)
		format.LE.PutUint64(index[8:], last)
		format.LE.PutUint32(index[28:], declaredCount)
		format.LE.PutUint32(index[32:], format.CRC(frames))
		footer := data[len(data)-format.FooterSize:]
		format.LE.PutUint64(footer[12:], uint64(declaredCount))
		format.LE.PutUint32(footer[20:], format.CRC(index))
		format.LE.PutUint32(footer[28:], format.CRC(footer[:28]))
		s, err := OpenSegment(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			if valid || !errors.Is(err, ErrCorrupt) {
				t.Fatal("unexpected open result", valid, err)
			}
			return
		}
		verifyErr := s.Verify()
		delivered, failures := 0, 0
		for r, err := range s.Records() {
			if err != nil {
				if !errors.Is(err, ErrCorrupt) {
					t.Fatal(err)
				}
				failures++
				continue
			}
			if !valid || delivered >= count || r.ID != records[delivered].ID || !bytes.Equal(r.Payload, records[delivered].Payload) {
				t.Fatal("unexpected delivery", delivered, r)
			}
			delivered++
		}
		if !valid {
			if !errors.Is(verifyErr, ErrCorrupt) || delivered != 0 || failures != 1 {
				t.Fatal("accepted invalid segment", verifyErr, delivered, failures)
			}
			// ID zero selects the block even if its first-ID claim is wrong.
			if r, err := s.Seek(0); !errors.Is(err, ErrCorrupt) {
				t.Fatal("lookup accepted invalid block", r, err)
			}
			return
		}
		if verifyErr != nil || delivered != count || failures != 0 {
			t.Fatal("rejected valid segment", verifyErr, delivered, failures)
		}
		want := count
		for i, r := range records {
			if r.ID >= query {
				want = i
				break
			}
		}
		r, err := s.Seek(query)
		if want == count {
			if !errors.Is(err, ErrNotFound) {
				t.Fatal("lookup beyond survivors", r, err)
			}
		} else if err != nil || r.ID != records[want].ID || !bytes.Equal(r.Payload, records[want].Payload) {
			t.Fatal("lookup disagrees with input", r, err)
		}
	})
}
