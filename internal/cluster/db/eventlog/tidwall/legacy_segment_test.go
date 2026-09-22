package tidwall

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestInspectLegacySegment(t *testing.T) {
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	records := binary.AppendUvarint(nil, 3)
	records = append(records, "one"...)
	records = binary.AppendUvarint(records, 3)
	records = append(records, "two"...)
	errVerify := errors.New("invalid frame")
	for _, tc := range []struct {
		name, filename string
		data           []byte
		reject         bool
		wantCalls      int
	}{
		{"plain", "00000000000000000010", records, false, 2},
		{"compressed", "00000000000000000010.zst", encoder.EncodeAll(records, nil), false, 2},
		{"empty", "00000000000000000010", nil, true, 0},
		{"compressed-empty", "00000000000000000010.zst", encoder.EncodeAll(nil, nil), true, 0},
		{"bad-zstd", "00000000000000000010.zst", []byte("bad"), true, 0},
		{"torn-prefix", "00000000000000000010", []byte{0x80}, true, 0},
		{"torn-record", "00000000000000000010", []byte{3, 'x'}, true, 0},
		{"huge-size", "00000000000000000010", binary.AppendUvarint(nil, ^uint64(0)), true, 0},
		{"bad-second-frame", "00000000000000000010", append(append([]byte{}, records[:4]...), 1, '!'), true, 2},
		{"zero-name", "00000000000000000000", records, true, 0},
		{"short-name", "10", records, true, 0},
		{"other-suffix", "00000000000000000010.tmp", records, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), tc.filename)
			if err := os.WriteFile(path, tc.data, 0o600); err != nil {
				t.Fatal(err)
			}
			calls := 0
			got, err := InspectLegacySegment(path, func(raw []byte) error {
				calls++
				if !bytes.Equal(raw, []byte("one")) && !bytes.Equal(raw, []byte("two")) {
					return errVerify
				}
				return nil
			})
			if (err != nil) != tc.reject || calls != tc.wantCalls {
				t.Fatal(got, calls, err)
			}
			if !tc.reject && (got.FirstSeq != 10 || got.Path != path || got.Compressed != (filepath.Ext(path) == ".zst")) {
				t.Fatal(got)
			}
			if tc.name == "bad-second-frame" && !errors.Is(err, errVerify) {
				t.Fatal(err)
			}
			after, readErr := os.ReadFile(path)
			if readErr != nil || !bytes.Equal(after, tc.data) {
				t.Fatal("inspection modified staged file", readErr)
			}
		})
	}
}
