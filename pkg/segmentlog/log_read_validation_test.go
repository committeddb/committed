package segmentlog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestLogReadsRejectSegmentCatalogMismatch(t *testing.T) {
	for _, tc := range []struct {
		name     string
		coverage Coverage
		records  []Record
	}{
		{"coverage", Coverage{0, 20}, []Record{{0, nil}, {10, nil}}},
		{"count", Coverage{0, 11}, []Record{{0, nil}}},
		{"empty", Coverage{0, 11}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := newLog(t, 32)
			if err := log.Append([]Record{{0, nil}, {10, nil}, {20, nil}}); err != nil {
				t.Fatal(err)
			}
			catalog, err := log.catalog.Current()
			if err != nil {
				t.Fatal(err)
			}
			if len(catalog.Segments) != 1 || catalog.Segments[0].Coverage != (Coverage{0, 11}) || catalog.Segments[0].Count != 2 {
				t.Fatal("unexpected fixture layout", catalog)
			}
			// Simulate a misplaced file after startup verification. The replacement
			// is internally valid, including checksums, but disagrees with the catalog.
			var replacement bytes.Buffer
			if err := WriteSegment(&replacement, tc.coverage, sequence(tc.records...), Options{}); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(log.path, catalog.Segments[0].File), replacement.Bytes(), 0o600); err != nil {
				t.Fatal(err)
			}
			for _, id := range []uint64{0, 10} {
				if r, err := log.Seek(id); !errors.Is(err, ErrCorrupt) || r.ID != 0 || r.Payload != nil {
					t.Fatal("seek accepted mismatched segment", id, r, err)
				}
				if r, err := log.Read(id); !errors.Is(err, ErrCorrupt) || r.ID != 0 || r.Payload != nil {
					t.Fatal("read accepted mismatched segment", id, r, err)
				}
			}
			delivered := 0
			err = log.Scan(t.Context(), Coverage{0, 11}, func(Record) error { delivered++; return nil })
			if !errors.Is(err, ErrCorrupt) || delivered != 0 {
				t.Fatal("scan accepted mismatched segment", delivered, err)
			}
		})
	}
}
