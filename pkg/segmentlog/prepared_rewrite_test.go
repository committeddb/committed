package segmentlog

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"
)

func TestPreparedRewriteVisibilityAndOwnership(t *testing.T) {
	for _, publish := range []bool{false, true} {
		name := "abandon"
		if publish {
			name = "publish"
		}
		t.Run(name, func(t *testing.T) {
			l := cachedLog(t, 42)
			if err := l.Append([]Record{{1, []byte("value")}, {10, []byte("value")}, {20, []byte("value")}}); err != nil {
				t.Fatal(err)
			}
			l.mutationMu.Lock()
			defer l.mutationMu.Unlock()
			cursor := l.NewCursor()
			defer func() { _ = cursor.Close() }()
			if _, err := cursor.Seek(1); err != nil {
				t.Fatal(err)
			}
			before, err := l.InspectCatalog()
			if err != nil {
				t.Fatal(err)
			}
			oldFile, oldResident, oldEpoch := l.file, l.resident, l.cursorEpoch
			plan := &preparedLogRewrite{baseRevision: before.Revision, generation: 1, result: RewriteResult{SealedRewriteResult: SealedRewriteResult{SealedEnd: before.Active.Start}}}
			defer func() { _ = plan.close() }()
			calls := 0
			l.mu.Lock()
			err = plan.prepare(l, t.Context(), before, func(r Record) ([]byte, bool, error) { calls++; r.Payload[0] = 'X'; return r.Payload, true, nil }, true)
			l.mu.Unlock()
			if err != nil || calls != 3 || plan.result.ChangedSegments != 1 || !plan.result.TailChanged || plan.result.Published {
				t.Fatal(plan.result, calls, err)
			}
			preparedFile := plan.file
			if preparedFile == nil || preparedFile == oldFile {
				t.Fatal("replacement tail not owned separately")
			}
			current, err := l.InspectCatalog()
			if err != nil || !reflect.DeepEqual(current, before) {
				t.Fatal("preparation published metadata", err)
			}
			if l.file != oldFile || l.resident != oldResident || l.cursorEpoch != oldEpoch {
				t.Fatal("preparation changed live state")
			}
			for _, id := range []uint64{1, 10, 20} {
				r, err := cursor.Seek(id)
				if err != nil || string(r.Payload) != "value" {
					t.Fatal("unpublished data visible", r, err)
				}
			}
			if publish {
				l.mu.Lock()
				err = plan.verify(l)
				if err == nil {
					err = plan.publish(l, t.Context())
				}
				l.mu.Unlock()
				if err != nil || !plan.result.Published {
					t.Fatal(plan.result, err)
				}
				if plan.file != nil || l.file != preparedFile || l.cursorEpoch == oldEpoch {
					t.Fatal("publication did not transfer ownership")
				}
				if _, err := oldFile.Stat(); !errors.Is(err, os.ErrClosed) {
					t.Fatal("old tail not closed", err)
				}
			} else {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				l.mu.Lock()
				err = plan.publish(l, ctx)
				l.mu.Unlock()
				if !errors.Is(err, context.Canceled) || plan.result.Published {
					t.Fatal(plan.result, err)
				}
			}
			if err := plan.close(); err != nil {
				t.Fatal(err)
			}
			if err := plan.close(); err != nil {
				t.Fatal(err)
			}
			if !publish {
				if _, err := preparedFile.Stat(); !errors.Is(err, os.ErrClosed) {
					t.Fatal("unused handle retained", err)
				}
			}
			want := "value"
			if publish {
				want = "Xalue"
			}
			for _, id := range []uint64{1, 10, 20} {
				r, err := cursor.Seek(id)
				if err != nil || string(r.Payload) != want {
					t.Fatal(r, err)
				}
			}
			if calls != 3 {
				t.Fatal("publication repeated transforms", calls)
			}
			if err := l.Verify(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
