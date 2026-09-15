package format

import (
	"bytes"
	"testing"
)

func FuzzFrame(f *testing.F) {
	f.Add(AppendFrame(nil, 0, []byte("payload")))
	f.Add(AppendFrame(nil, 100, nil))
	f.Fuzz(func(t *testing.T, data []byte) {
		// Repair the outer checksum to exercise structural parsing beyond the
		// checksum barrier. Incorrect declared lengths must still fail safely.
		if len(data) >= FrameOverhead {
			data = bytes.Clone(data)
			LE.PutUint32(data[len(data)-4:], CRC(data[:len(data)-4]))
		}
		id, payload, rest, err := Frame(data)
		if err != nil {
			return
		}
		encoded := AppendFrame(nil, id, payload)
		if !bytes.Equal(encoded, data[:len(data)-len(rest)]) {
			t.Fatal("round trip differs")
		}
		if cap(payload) != len(payload) {
			t.Fatal("payload can overwrite next frame")
		}
	})
}
