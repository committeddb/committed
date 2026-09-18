package segmentlog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestRewriteSealedPreservesUnaffectedFiles(t *testing.T) {
	log := rotatedLog(t)
	before, _ := log.catalog.Current()
	originals := map[string]os.FileInfo{}
	contents := map[string][]byte{}
	for name := range referencedNames(t, log) {
		info, err := os.Stat(filepath.Join(log.path, name))
		if err != nil {
			t.Fatal(err)
		}
		originals[name] = info
		contents[name] = readBytes(t, filepath.Join(log.path, name))
	}
	calls := map[uint64]int{}
	result, err := log.RewriteSealed(t.Context(), 1, func(r Record) ([]byte, bool, error) {
		calls[r.ID]++
		switch r.ID {
		case 2:
			return []byte("public"), true, nil
		case 4:
			return nil, false, nil
		default:
			return r.Payload, true, nil
		}
	})
	if err != nil || result != (SealedRewriteResult{SealedEnd: 6, ChangedSegments: 2, EmptiedSegments: 1, Published: true}) {
		t.Fatal(result, err)
	}
	if !reflect.DeepEqual(calls, map[uint64]int{1: 1, 2: 1, 3: 1, 4: 1, 5: 1}) {
		t.Fatal(calls)
	}
	after, _ := log.catalog.Current()
	if after.Revision != before.Revision+1 || after.Generation != 1 || *after.Active != *before.Active {
		t.Fatal(after)
	}
	for i, ref := range before.Segments {
		got := after.Segments[i]
		if got.Coverage != ref.Coverage {
			t.Fatal("coverage changed", got)
		}
		if i == 1 || i == 3 {
			continue
		}
		if got != ref {
			t.Fatal("unaffected reference changed", got)
		}
	}
	if after.Segments[3] != (SegmentRef{Coverage: before.Segments[3].Coverage}) {
		t.Fatal("erased range has a payload file")
	}
	for name := range referencedNames(t, log) {
		old, exists := originals[name]
		if !exists || name == boltCatalogName {
			continue
		}
		info, err := os.Stat(filepath.Join(log.path, name))
		if err != nil || !os.SameFile(old, info) || !old.ModTime().Equal(info.ModTime()) || !bytes.Equal(contents[name], readBytes(t, filepath.Join(log.path, name))) {
			t.Fatal("changed retained file", name, err)
		}
	}
	log = reopenLog(t, log)
	for id := uint64(1); id <= 6; id++ {
		r, err := log.Read(id)
		if id == 4 {
			if !errors.Is(err, ErrNotFound) {
				t.Fatal(err)
			}
			continue
		}
		want := "value"
		if id == 2 {
			want = "public"
		}
		if err != nil || string(r.Payload) != want {
			t.Fatal(r, err)
		}
	}
	if r, err := log.Seek(4); err != nil || r.ID != 5 {
		t.Fatal(r, err)
	}
	if _, err := log.ReclaimOrphans(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{1, 3} {
		if _, err := os.Stat(filepath.Join(log.path, before.Segments[i].File)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("old payload retained", err)
		}
	}
	if err := log.Append([]Record{{7, []byte("next")}}); err != nil {
		t.Fatal(err)
	}
	current, _ := log.catalog.Current()
	if current.Segments[len(current.Segments)-1].Coverage != (Coverage{6, 7}) {
		t.Fatal("rotation frontier changed", current)
	}
}

func TestRewriteSealedNoopAndEmptyRanges(t *testing.T) {
	log := rotatedLog(t)
	before, _ := log.catalog.Current()
	result, err := log.RewriteSealed(t.Context(), 1, func(r Record) ([]byte, bool, error) { return r.Payload, true, nil })
	after, _ := log.catalog.Current()
	if err != nil || !result.Published || result.ChangedSegments != 0 || !reflect.DeepEqual(before.Segments, after.Segments) {
		t.Fatal(result, err)
	}
	// Erase all sealed records, then prove subsequent passes skip the empty ranges.
	result, err = log.RewriteSealed(t.Context(), 2, func(Record) ([]byte, bool, error) { return nil, false, nil })
	if err != nil || result.EmptiedSegments != 5 {
		t.Fatal(result, err)
	}
	log = reopenLog(t, log)
	if r, err := log.Seek(0); err != nil || r.ID != 6 {
		t.Fatal(r, err)
	}
	result, err = log.RewriteSealed(t.Context(), 3, func(Record) ([]byte, bool, error) {
		t.Fatal("visited empty range or active tail")
		return nil, false, nil
	})
	if err != nil || !result.Published || result.ChangedSegments != 0 {
		t.Fatal(result, err)
	}
}

func TestRewriteSealedPreparationFailures(t *testing.T) {
	for _, mode := range []string{"callback", "cancel", "install-before", "install-after"} {
		t.Run(mode, func(t *testing.T) {
			log := rotatedLog(t)
			before, _ := log.catalog.Current()
			boom := errors.New("rewrite failure")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if mode == "install-before" || mode == "install-after" {
				log.dir = &rotationInstaller{fileInstaller: log.dir, failAt: 2, after: mode == "install-after", boom: boom}
			}
			result, err := log.RewriteSealed(ctx, 1, func(r Record) ([]byte, bool, error) {
				if r.ID == 2 {
					if mode == "callback" {
						return nil, false, boom
					}
					if mode == "cancel" {
						cancel()
					}
				}
				return []byte("replacement"), true, nil
			})
			want := boom
			if mode == "cancel" {
				want = context.Canceled
			}
			if !errors.Is(err, want) || !errors.Is(err, ErrLogPoisoned) || result.Published {
				t.Fatal(result, err)
			}
			if _, err := log.Read(1); !errors.Is(err, ErrLogPoisoned) {
				t.Fatal(err)
			}
			log = reopenLog(t, log)
			recovered, _ := log.catalog.Current()
			if !reflect.DeepEqual(before, recovered) {
				t.Fatal("published partial transaction", recovered)
			}
			if r, err := log.Read(1); err != nil || string(r.Payload) != "value" {
				t.Fatal(r, err)
			}
			if _, err := log.ReclaimOrphans(t.Context()); err != nil {
				t.Fatal(err)
			}
			entries, err := os.ReadDir(log.path)
			if err != nil || len(entries) != len(referencedNames(t, log)) {
				t.Fatal("unpublished files retained", err)
			}
		})
	}
}

func TestRewriteSealedPublicationFailure(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprint(after), func(t *testing.T) {
			log := rotatedLog(t)
			boom := errors.New("metadata commit failure")
			failMetadataCommit(log, 1, after, boom)
			result, err := log.RewriteSealed(t.Context(), 1, func(r Record) ([]byte, bool, error) { return []byte("replacement"), r.ID != 4, nil })
			if !errors.Is(err, boom) || !errors.Is(err, ErrLogPoisoned) || result.Published {
				t.Fatal(result, err)
			}
			log = reopenLog(t, log)
			c, _ := log.catalog.Current()
			wantGen := uint64(0)
			if after {
				wantGen = 1
			}
			if c.Generation != wantGen {
				t.Fatal(c)
			}
			for id := uint64(1); id <= 6; id++ {
				r, err := log.Read(id)
				if after && id == 4 {
					if !errors.Is(err, ErrNotFound) {
						t.Fatal(err)
					}
					continue
				}
				want := "value"
				if after && id < 6 {
					want = "replacement"
				}
				if err != nil || string(r.Payload) != want {
					t.Fatal("mixed generation", r, err)
				}
			}
		})
	}
}

func TestRewriteSealedWithinMultiRecordRange(t *testing.T) {
	log := newLog(t, 100)
	if err := log.Append([]Record{{1, []byte("a")}, {10, []byte("b")}, {20, []byte("c")}, {30, []byte("d")}, {40, []byte("e")}, {50, []byte("f")}}); err != nil {
		t.Fatal(err)
	}
	calls := map[uint64]int{}
	result, err := log.RewriteSealed(t.Context(), 1, func(r Record) ([]byte, bool, error) {
		calls[r.ID]++
		if r.ID == 20 {
			r.Payload[0] = 'X'
		}
		return r.Payload, r.ID != 30, nil
	})
	if err != nil || result.ChangedSegments != 1 || !reflect.DeepEqual(calls, map[uint64]int{1: 1, 10: 1, 20: 1, 30: 1, 40: 1}) {
		t.Fatal(result, calls, err)
	}
	log = reopenLog(t, log)
	for id, want := range map[uint64]string{1: "a", 10: "b", 20: "X", 40: "e", 50: "f"} {
		r, err := log.Read(id)
		if err != nil || string(r.Payload) != want {
			t.Fatal(r, err)
		}
	}
	if r, err := log.Seek(21); err != nil || r.ID != 40 {
		t.Fatal(r, err)
	}
}

func TestRewriteSealedRejectsInvalidRequests(t *testing.T) {
	log := rotatedLog(t)
	keep := func(r Record) ([]byte, bool, error) { return r.Payload, true, nil }
	if _, err := log.RewriteSealed(t.Context(), 0, keep); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := log.RewriteSealed(t.Context(), 1, nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := log.RewriteSealed(ctx, 1, keep); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := log.RewriteSealed(t.Context(), 1, keep); err != nil {
		t.Fatal("invalid input poisoned handle", err)
	}
	if _, err := log.RewriteSealed(t.Context(), 1, keep); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}
