package format

import (
	"bytes"
	"fmt"
	"slices"
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

// Test-only candidate: reserving each complete frame reduced allocations but
// did not establish a full-segment encoding benefit.
func appendFrameReserved(dst []byte, id uint64, payload []byte) []byte {
	// Reserve the entire frame before appending its header, payload, and CRC.
	dst = slices.Grow(dst, len(payload)+FrameOverhead)
	start := len(dst)
	dst = dst[:start+len(payload)+FrameOverhead]
	frame := dst[start:]
	LE.PutUint32(frame, uint32(len(payload))) // #nosec G115 -- Internal encoder contract: callers bound payloads to MaxPayload before encoding.
	LE.PutUint64(frame[4:], id)
	copy(frame[12:], payload)
	LE.PutUint32(frame[len(frame)-4:], CRC(frame[:len(frame)-4]))
	return dst
}

func TestAppendFrameCapacityBoundaries(t *testing.T) {
	for _, size := range []int{0, 1, 48, 49, 4080, 4096, 65536} {
		payload := bytes.Repeat([]byte("x"), size)
		prefix := []byte("existing bytes")
		expected := AppendFrame(bytes.Clone(prefix), 123, payload)
		for _, spare := range []int{0, 4, 12, size + FrameOverhead - 1, size + FrameOverhead} {
			dst := make([]byte, len(prefix), len(prefix)+spare)
			copy(dst, prefix)
			actual := appendFrameReserved(dst, 123, payload)
			if !bytes.Equal(actual, expected) {
				t.Fatal("changed encoding", size, spare)
			}
			id, decoded, rest, err := Frame(actual[len(prefix):])
			if err != nil || id != 123 || !bytes.Equal(decoded, payload) || len(rest) != 0 {
				t.Fatal(size, spare, err)
			}
		}
	}
}

func BenchmarkFrameGrowth(b *testing.B) {
	for _, size := range []int{0, 49, 4080} {
		payload := bytes.Repeat([]byte("x"), size)
		for _, reuse := range []bool{false, true} {
			b.Run(fmt.Sprintf("payload=%d/reserve=%t", size, reuse), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var encoded []byte
					if reuse {
						encoded = appendFrameReserved(nil, 123, payload)
					} else {
						encoded = AppendFrame(nil, 123, payload)
					}
					if len(encoded) != size+FrameOverhead {
						b.Fatal(len(encoded))
					}
				}
			})
		}
	}
}
