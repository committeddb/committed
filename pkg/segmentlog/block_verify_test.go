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

func TestBlockDescriptorReuseOwnership(t *testing.T) {
	var decoder format.Decoder
	defer decoder.Close()
	var scratch []Record
	retained := make([]Record, 0, 4)
	for _, count := range []int{2, 8, 1, 4} {
		var data []byte
		for i := range count {
			data = format.AppendFrame(data, uint64(i*3), []byte("payload"))
		}
		b := block{first: 0, last: uint64((count - 1) * 3), count: uint32(count), size: uint32(len(data)), decoded: uint32(len(data)), crc: format.CRC(data), codec: format.Plain}
		previous := scratch
		records, err := decodeBlockWithDecoder(b, data, &decoder, scratch)
		if err != nil || len(records) != count {
			t.Fatal(count, err)
		}
		for _, old := range previous[min(len(previous), count):] {
			if old.Payload != nil {
				t.Fatal("retained unused payload reference")
			}
		}
		retained = append(retained, records[0])
		scratch = records
	}
	for _, r := range retained {
		if r.ID != 0 || string(r.Payload) != "payload" {
			t.Fatal("descriptor reuse changed retained record", r)
		}
	}
	// A corrupt final frame still prevents all delivery from the block even
	// when the caller supplies an already allocated descriptor list.
	data := format.AppendFrame(nil, 0, []byte("valid"))
	data = format.AppendFrame(data, 3, []byte("corrupt"))
	data[len(data)-1] ^= 1
	b := block{first: 0, last: 3, count: 2, size: uint32(len(data)), decoded: uint32(len(data)), crc: format.CRC(data), codec: format.Plain}
	records, err := decodeBlockWithDecoder(b, data, &decoder, scratch)
	if !errors.Is(err, ErrCorrupt) || records != nil {
		t.Fatal("exposed corrupt block", records, err)
	}
}
