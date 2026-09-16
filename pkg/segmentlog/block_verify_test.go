package segmentlog

import (
	"errors"
	"testing"

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

func TestBlockValidationWithoutRecordCollection(t *testing.T) {
	for _, name := range []string{"valid", "duplicate", "decreasing", "wrong-first", "wrong-last", "too-many", "too-few", "bad-final-frame"} {
		t.Run(name, func(t *testing.T) {
			ids := []uint64{0, 2, 4}
			b := block{first: 0, last: 4, count: 3, codec: format.Plain}
			switch name {
			case "duplicate":
				ids = []uint64{0, 2, 2, 4}
				b.count = 4
			case "decreasing":
				ids = []uint64{0, 3, 2, 4}
				b.count = 4
			case "wrong-first":
				b.first = 0
				ids[0] = 1
			case "wrong-last":
				b.last = 5
			case "too-many":
				b.count = 2
			case "too-few":
				b.count = 4
			}
			var data []byte
			for _, id := range ids {
				data = format.AppendFrame(data, id, []byte("payload"))
			}
			if name == "bad-final-frame" {
				data[len(data)-1] ^= 1
			}
			// The block checksum is valid even when its records or index claims are not.
			b.crc = format.CRC(data)
			b.size, b.decoded = uint32(len(data)), uint32(len(data))
			err := walkBlock(b, data, nil)
			records, readErr := decodeBlock(b, data)
			if name == "valid" {
				if err != nil || readErr != nil || len(records) != 3 {
					t.Fatal(records, err, readErr)
				}
				return
			}
			if !errors.Is(err, ErrCorrupt) || !errors.Is(readErr, ErrCorrupt) {
				t.Fatal(err, readErr)
			}
			if records != nil {
				t.Fatal("failed read exposed a partially validated block")
			}
		})
	}
}
