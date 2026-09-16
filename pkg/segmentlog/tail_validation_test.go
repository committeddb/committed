package segmentlog

import (
	"bytes"
	"errors"
	"testing"

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

func TestTailValidationWithAndWithoutDelivery(t *testing.T) {
	for _, name := range []string{"valid", "too-few", "too-many", "wrong-last", "bad-frame"} {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteTailHeader(&buf, 0); err != nil {
				t.Fatal(err)
			}
			payload := bytes.Repeat([]byte("x"), 16)
			group := encodeTailGroup([]Record{{0, payload}, {10, payload}}, 64)
			switch name {
			case "too-few":
				format.LE.PutUint32(group[12:], 1)
			case "too-many":
				format.LE.PutUint32(group[12:], 3)
			case "wrong-last":
				format.LE.PutUint64(group[16:], 11)
			case "bad-frame":
				group[len(group)-groupTrailerSize-1] ^= 1
			}
			// Keep the outer checksums valid so frame/metadata validation must
			// detect the problem, even when recovery requests no delivery.
			format.LE.PutUint32(group[28:], format.CRC(group[:28]))
			format.LE.PutUint32(group[len(group)-4:], format.CRC(group[:len(group)-4]))
			buf.Write(group)
			reader := bytes.NewReader(buf.Bytes())
			verified, verifyErr := ScanTail(reader, int64(buf.Len()), nil)
			delivered := 0
			scanned, scanErr := ScanTail(reader, int64(buf.Len()), func(r Record) error {
				if r.ID != uint64(delivered*10) {
					t.Fatal(r)
				}
				delivered++
				return nil
			})
			if verified != scanned {
				t.Fatal("verification and delivery disagree", verified, scanned)
			}
			if name == "valid" {
				if verifyErr != nil || scanErr != nil || delivered != 2 || verified.Count != 2 || verified.Last != 10 || verified.End != int64(buf.Len()) {
					t.Fatal(verified, delivered, verifyErr, scanErr)
				}
			} else if !errors.Is(verifyErr, ErrCorrupt) || !errors.Is(scanErr, ErrCorrupt) || delivered != 0 || verified.End != tailHeaderSize || verified.Count != 0 {
				t.Fatal("accepted corrupt group", verified, delivered, verifyErr, scanErr)
			}
		})
	}
}
