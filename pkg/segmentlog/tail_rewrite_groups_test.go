package segmentlog

import (
	"bytes"
	"crypto/sha256"
	"path/filepath"
	"testing"

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

func TestTailRewriteGroupsCopyReusedPayloads(t *testing.T) {
	l := newLog(t, 20<<20)
	records := make([]Record, 7)
	for i := range records {
		records[i].ID = uint64(i * 10)
	}
	if err := l.Append(records); err != nil {
		t.Fatal(err)
	}
	before, err := l.tail.State()
	if err != nil {
		t.Fatal(err)
	}
	sizes := []int{1, 4096, 0, (256 << 10) - format.FrameOverhead, (256 << 10) + 1, format.MaxPayload, 1}
	buffer := make([]byte, format.MaxPayload)
	want := map[uint64][sha256.Size]byte{}
	result, err := l.Rewrite(t.Context(), 1, func(r Record) ([]byte, bool, error) {
		size := sizes[r.ID/10]
		if size == 0 {
			return nil, false, nil
		}
		payload := buffer[:size]
		payload[0], payload[len(payload)-1] = byte(r.ID), byte(r.ID+1)
		want[r.ID] = sha256.Sum256(payload)
		return payload, true, nil
	})
	if err != nil || !result.Published || !result.TailChanged {
		t.Fatal(result, err)
	}
	c, err := l.InspectCatalog()
	if err != nil {
		t.Fatal(err)
	}
	raw := readBytes(t, filepath.Join(l.path, c.Active.File))
	state, err := ScanTail(bytes.NewReader(raw), int64(len(raw)), func(r Record) error {
		digest, ok := want[r.ID]
		if !ok || sha256.Sum256(r.Payload) != digest {
			t.Fatal("lost or aliased record", r.ID)
		}
		delete(want, r.ID)
		return nil
	})
	if err != nil || len(want) != 0 || state.Count != 6 {
		t.Fatal(state, err, len(want))
	}
	groups := 0
	for offset := 32; offset < len(raw); groups++ {
		size := int(format.LE.Uint32(raw[offset+8:]))
		offset += groupHeaderSize + size + groupTrailerSize
	}
	if groups != 5 {
		t.Fatal("unexpected packing", groups)
	}
	l = reopenLog(t, l)
	recovered, err := l.tail.State()
	if err != nil || recovered.Framed != before.Framed || recovered.OriginalCount != before.OriginalCount || recovered.Last != before.Last {
		t.Fatal(before, recovered, err)
	}
	if err := l.Append([]Record{{70, []byte("after rewrite")}}); err != nil {
		t.Fatal(err)
	}
	l = reopenLog(t, l)
	if err := l.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
}
