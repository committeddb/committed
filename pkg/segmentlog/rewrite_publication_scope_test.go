package segmentlog

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestRewritePublicationPreservesUnchangedTail(t *testing.T) {
	for _, mode := range []string{"sealed-noop", "sealed-change", "whole-noop"} {
		t.Run(mode, func(t *testing.T) {
			l := rotatedLog(t)
			// Establish a nontrivial checkpoint, including erased append progress.
			if _, err := l.Rewrite(t.Context(), 1, func(r Record) ([]byte, bool, error) {
				return r.Payload, r.ID != 6, nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := l.Append([]Record{{7, []byte("tail")}}); err != nil {
				t.Fatal(err)
			}
			// The tiny fixture rotates on append; rewrite again to checkpoint its tail.
			if _, err := l.Rewrite(t.Context(), 2, func(r Record) ([]byte, bool, error) {
				if r.ID == 7 {
					return []byte("rewritten tail"), true, nil
				}
				return r.Payload, true, nil
			}); err != nil {
				t.Fatal(err)
			}
			before, err := l.InspectCatalog()
			if err != nil || before.Active.Checkpoint == nil {
				t.Fatal(before, err)
			}
			path := filepath.Join(l.path, before.Active.File)
			hidden := path + ".held"
			// Keep the open descriptor readable, but make any publication-time open
			// by filename fail. This is fault injection, not a supported user action.
			if err := os.Rename(path, hidden); err != nil {
				t.Fatal(err)
			}
			transform := func(r Record) ([]byte, bool, error) {
				if mode == "sealed-change" {
					return []byte("changed"), true, nil
				}
				return r.Payload, true, nil
			}
			if mode == "whole-noop" {
				result, err := l.Rewrite(t.Context(), 3, transform)
				if err != nil || !result.Published || result.TailChanged || result.ChangedSegments != 0 {
					t.Fatal(result, err)
				}
			} else {
				result, err := l.RewriteSealed(t.Context(), 3, transform)
				if err != nil || !result.Published || (mode == "sealed-change" && result.ChangedSegments == 0) {
					t.Fatal(result, err)
				}
			}
			after, err := l.InspectCatalog()
			if err != nil || after.Revision != before.Revision+1 || after.Generation != 3 || !reflect.DeepEqual(before.Active, after.Active) {
				t.Fatal(after, err)
			}
			// Explicit full-log verification must still notice the missing tail.
			if err := l.Verify(t.Context()); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("verification skipped tail", err)
			}
			if err := os.Rename(hidden, path); err != nil {
				t.Fatal(err)
			}
			l = reopenLog(t, l)
			if err := l.Verify(t.Context()); err != nil {
				t.Fatal(err)
			}
			if id, ok, err := l.LastAppended(); err != nil || !ok || id != 7 {
				t.Fatal(id, ok, err)
			}
			r, err := l.Read(7)
			if err != nil || string(r.Payload) != "rewritten tail" {
				t.Fatal(r, err)
			}
			if err := l.Append([]Record{{8, []byte("next")}}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRewritePublicationChecksReplacementTail(t *testing.T) {
	l := rotatedLog(t)
	before, err := l.InspectCatalog()
	if err != nil {
		t.Fatal(err)
	}
	replacement := *before.Active
	replacement.File, err = uniqueName("tail", replacement.Start, ".active")
	if err != nil {
		t.Fatal(err)
	}
	err = l.catalog.publishRewrite(before.Revision, 1, nil, &replacement)
	if !errors.Is(err, os.ErrNotExist) || !errors.Is(err, ErrCatalogPoisoned) {
		t.Fatal("accepted absent replacement tail", err)
	}
	l = reopenLog(t, l)
	after, err := l.InspectCatalog()
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("selected failed replacement", after, err)
	}
	if err := l.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkRewriteSealedWithFullTail(b *testing.B) {
	l := tailRewriteFixture(b, 5120)
	generation := uint64(1)
	b.ReportAllocs()
	for b.Loop() {
		result, err := l.RewriteSealed(b.Context(), generation, func(Record) ([]byte, bool, error) {
			b.Fatal("visited active record in sealed-only rewrite")
			return nil, false, nil
		})
		if err != nil || !result.Published || result.ChangedSegments != 0 {
			b.Fatal(result, err)
		}
		generation++
	}
	if err := l.Verify(b.Context()); err != nil {
		b.Fatal(err)
	}
	if id, ok, err := l.LastAppended(); err != nil || !ok || id != 5119 {
		b.Fatal(id, ok, err)
	}
}
