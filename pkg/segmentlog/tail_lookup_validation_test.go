package segmentlog

import (
	"errors"
	"fmt"
	"testing"
)

func TestTailLookupEnforcesRewriteCheckpoint(t *testing.T) {
	for _, eraseAll := range []bool{false, true} {
		for _, badID := range []uint64{50, 100} {
			t.Run(fmt.Sprintf("eraseAll=%t/id=%d", eraseAll, badID), func(t *testing.T) {
				log := newLog(t, 1024)
				if err := log.Append([]Record{{0, nil}, {100, nil}}); err != nil {
					t.Fatal(err)
				}
				if _, err := log.Rewrite(t.Context(), 1, func(r Record) ([]byte, bool, error) {
					return r.Payload, !eraseAll && r.ID == 0, nil
				}); err != nil {
					t.Fatal(err)
				}
				log = reopenLog(t, log)
				catalog, err := log.catalog.Current()
				if err != nil {
					t.Fatal(err)
				}
				if catalog.Active.Checkpoint == nil || catalog.Active.Checkpoint.Last != 100 {
					t.Fatal("missing erased append frontier", catalog.Active)
				}
				if err := log.Append([]Record{{200, nil}}); err != nil {
					t.Fatal(err)
				}
				if r, err := log.Seek(1); err != nil || r.ID != 200 {
					t.Fatal("valid append after checkpoint", r, err)
				}
				// Replace the later group with valid framing and checksums but an ID
				// at or below the erased frontier. Physical ordering alone accepts it.
				group := encodeTailGroup([]Record{{badID, nil}}, 16)
				if n, err := log.file.WriteAt(group, catalog.Active.Checkpoint.End); err != nil || n != len(group) {
					t.Fatal(n, err)
				}
				if r, err := log.Seek(1); !errors.Is(err, ErrCorrupt) {
					t.Fatal("seek accepted checkpoint violation", r, err)
				}
				if r, err := log.Read(badID); !errors.Is(err, ErrCorrupt) {
					t.Fatal("read accepted checkpoint violation", r, err)
				}
				delivered := 0
				err = log.Scan(t.Context(), Coverage{1, 201}, func(Record) error { delivered++; return nil })
				if !errors.Is(err, ErrCorrupt) || delivered != 0 {
					t.Fatal("scan accepted checkpoint violation", delivered, err)
				}
			})
		}
	}
}
