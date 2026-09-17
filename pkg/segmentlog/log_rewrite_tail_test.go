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

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

func TestRewriteWholeLogPreservesRotation(t *testing.T) {
	for _, erase := range []bool{false, true} {
		t.Run(fmt.Sprint(erase), func(t *testing.T) {
			log := newLog(t, 100)
			baseline := newLog(t, 100)
			records := []Record{{0, []byte("original")}, {10, []byte("original")}, {20, []byte("original")}, {30, []byte("original")}, {40, []byte("original")}, {50, []byte("original")}}
			for _, l := range []*Log{log, baseline} {
				if err := l.Append(records); err != nil {
					t.Fatal(err)
				}
			}
			before, _ := log.catalog.Current()
			oldState, _ := log.tail.State()
			calls := map[uint64]int{}
			result, err := log.Rewrite(t.Context(), 1, func(r Record) ([]byte, bool, error) { calls[r.ID]++; return []byte("x"), !erase, nil })
			if err != nil || !result.Published || !result.TailChanged || result.ChangedSegments != 1 {
				t.Fatal(result, err)
			}
			for _, r := range records {
				if calls[r.ID] != 1 {
					t.Fatal(calls)
				}
			}
			log = reopenLog(t, log)
			state, _ := log.tail.State()
			if state.Last != oldState.Last || state.Framed != oldState.Framed || state.OriginalCount != oldState.OriginalCount || !state.HasRecords {
				t.Fatal("lost append accounting", oldState, state)
			}
			if erase && state.Count != 0 {
				t.Fatal(state)
			}
			if err := log.Append([]Record{{50, nil}}); !errors.Is(err, ErrInvalid) {
				t.Fatal("reused erased index", err)
			}
			// Append inside the rewritten tail, reopen, and rewrite it a second time.
			more := []Record{{60, []byte("original")}}
			for _, l := range []*Log{log, baseline} {
				if err := l.Append(more); err != nil {
					t.Fatal(err)
				}
			}
			log = reopenLog(t, log)
			result, err = log.Rewrite(t.Context(), 2, func(r Record) ([]byte, bool, error) { return []byte("expanded payload"), r.ID != 60, nil })
			if err != nil || !result.TailChanged {
				t.Fatal(result, err)
			}
			log = reopenLog(t, log)
			// These force rotation according to original sizes, regardless of erasure or growth.
			more = []Record{{70, []byte("original")}, {80, []byte("original")}, {90, []byte("original")}}
			for _, l := range []*Log{log, baseline} {
				if err := l.Append(more); err != nil {
					t.Fatal(err)
				}
			}
			got, _ := log.catalog.Current()
			want, _ := baseline.catalog.Current()
			if len(got.Segments) != len(want.Segments) || got.Active.Start != want.Active.Start {
				t.Fatal(got, want)
			}
			for i, s := range got.Segments {
				if s.Coverage != want.Segments[i].Coverage {
					t.Fatal(s, want.Segments[i])
				}
			}
			if erase && (got.Segments[0].File != "" || got.Segments[1].Count != 1) {
				t.Fatal(got.Segments)
			}
			if _, err := log.ReclaimOrphans(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(log.path, before.Active.File)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("old tail retained", err)
			}
			if r, err := log.Read(90); err != nil || string(r.Payload) != "original" {
				t.Fatal(r, err)
			}
		})
	}
}

func TestRewriteErasedTailRotatesAsEmptyRange(t *testing.T) {
	log := newLog(t, 40)
	if err := log.Append([]Record{{100, bytes.Repeat([]byte("x"), 50)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Rewrite(t.Context(), 1, func(Record) ([]byte, bool, error) { return nil, false, nil }); err != nil {
		t.Fatal(err)
	}
	log = reopenLog(t, log)
	if r, err := log.Seek(0); !errors.Is(err, ErrNotFound) {
		t.Fatal(r, err)
	}
	if err := log.Append([]Record{{101, []byte("next")}}); err != nil {
		t.Fatal(err)
	}
	c, _ := log.catalog.Current()
	if len(c.Segments) != 1 || c.Segments[0] != (SegmentRef{Coverage: Coverage{0, 101}}) {
		t.Fatal(c)
	}
	if _, err := log.ReclaimOrphans(t.Context()); err != nil {
		t.Fatal(err)
	}
	log = reopenLog(t, log)
	if r, err := log.Seek(0); err != nil || r.ID != 101 {
		t.Fatal(r, err)
	}
}

func TestRewriteWholeLogNoop(t *testing.T) {
	log := rotatedLog(t)
	before, _ := log.catalog.Current()
	raw := readBytes(t, filepath.Join(log.path, before.Active.File))
	result, err := log.Rewrite(t.Context(), 1, func(r Record) ([]byte, bool, error) { return r.Payload, true, nil })
	after, _ := log.catalog.Current()
	if err != nil || !result.Published || result.TailChanged || result.ChangedSegments != 0 || !reflect.DeepEqual(before.Active, after.Active) || !reflect.DeepEqual(before.Segments, after.Segments) || !bytes.Equal(raw, readBytes(t, filepath.Join(log.path, after.Active.File))) {
		t.Fatal(result, err)
	}
}

func TestRewriteWholeLogFailures(t *testing.T) {
	for _, mode := range []string{"callback", "cancel", "tail-install-before", "tail-install-after", "commit-before", "commit-after"} {
		t.Run(mode, func(t *testing.T) {
			log := newLog(t, 40)
			if err := log.Append([]Record{{1, []byte("value")}, {2, []byte("value")}}); err != nil {
				t.Fatal(err)
			}
			before, _ := log.catalog.Current()
			boom := errors.New("rewrite failure")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch mode {
			case "tail-install-before", "tail-install-after":
				log.dir = &rotationInstaller{fileInstaller: log.dir, failAt: 2, after: mode == "tail-install-after", boom: boom}
			case "commit-before", "commit-after":
				failMetadataCommit(log, 1, mode == "commit-after", boom)
			}
			result, err := log.Rewrite(ctx, 1, func(r Record) ([]byte, bool, error) {
				if r.ID == 2 {
					if mode == "callback" {
						return nil, false, boom
					}
					if mode == "cancel" {
						cancel()
					}
				}
				return []byte("changed"), true, nil
			})
			wantErr := boom
			if mode == "cancel" {
				wantErr = context.Canceled
			}
			if !errors.Is(err, wantErr) || !errors.Is(err, ErrLogPoisoned) || result.Published {
				t.Fatal(result, err)
			}
			log = reopenLog(t, log)
			c, _ := log.catalog.Current()
			want := "value"
			if mode == "commit-after" {
				want = "changed"
				if c.Generation != 1 || c.Active.Checkpoint == nil {
					t.Fatal(c)
				}
			} else if !reflect.DeepEqual(c, before) {
				t.Fatal("adopted partial rewrite", c)
			}
			for _, id := range []uint64{1, 2} {
				r, err := log.Read(id)
				if err != nil || string(r.Payload) != want {
					t.Fatal("mixed transaction", r, err)
				}
			}
			if _, err := log.ReclaimOrphans(t.Context()); err != nil {
				t.Fatal(err)
			}
			entries, err := os.ReadDir(log.path)
			if err != nil || len(entries) != len(referencedNames(t, log)) {
				t.Fatal("orphan remained", err)
			}
		})
	}
}

func TestTailCheckpointValidation(t *testing.T) {
	var b bytes.Buffer
	if err := WriteTailHeader(&b, 0); err != nil {
		t.Fatal(err)
	}
	group := encodeTailGroup([]Record{{1, []byte("x")}}, 17)
	b.Write(group)
	end := int64(b.Len())
	valid := TailCheckpoint{End: end, Last: 10, Count: 2, Framed: 40}
	for _, mutate := range []func(*TailCheckpoint){
		func(c *TailCheckpoint) { c.End-- }, func(c *TailCheckpoint) { c.End++ }, func(c *TailCheckpoint) { c.Last = 0 }, func(c *TailCheckpoint) { c.Count = 0 }, func(c *TailCheckpoint) { c.Framed = 1 },
	} {
		c := valid
		mutate(&c)
		if _, err := scanTail(bytes.NewReader(b.Bytes()), int64(b.Len()), &c, nil); !errors.Is(err, ErrCorrupt) {
			t.Fatal(c, err)
		}
	}
	state, err := scanTail(bytes.NewReader(b.Bytes()), int64(b.Len()), &valid, nil)
	if err != nil || state.Last != 10 || state.Count != 1 || state.OriginalCount != 2 || state.Framed != 40 {
		t.Fatal(state, err)
	}
	// A valid checksummed append may not reuse an erased ID.
	for _, id := range []uint64{5, 10, 11} {
		suffix := encodeTailGroup([]Record{{id, nil}}, format.FrameOverhead)
		data := append(bytes.Clone(b.Bytes()), suffix...)
		state, err := scanTail(bytes.NewReader(data), int64(len(data)), &valid, nil)
		if id <= 10 {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatal(id, err)
			}
		} else if err != nil || state.Last != 11 || state.Framed != 56 || state.OriginalCount != 3 {
			t.Fatal(state, err)
		}
	}
}

func TestTailCheckpointCatalogCopies(t *testing.T) {
	log := rotatedLog(t)
	if _, err := log.Rewrite(t.Context(), 1, func(r Record) ([]byte, bool, error) { return nil, false, nil }); err != nil {
		t.Fatal(err)
	}
	c, _ := log.catalog.Current()
	original := *c.Active.Checkpoint
	c.Active.Checkpoint.Last = 12345
	again, _ := log.catalog.Current()
	if *again.Active.Checkpoint != original {
		t.Fatal("catalog copy aliases checkpoint")
	}
}

func TestRewrittenTailRecoversFailedAppend(t *testing.T) {
	for _, short := range []bool{false, true} {
		t.Run(fmt.Sprint(short), func(t *testing.T) {
			log := newLog(t, 100)
			if err := log.Append([]Record{{0, nil}, {10, nil}}); err != nil {
				t.Fatal(err)
			}
			if _, err := log.Rewrite(t.Context(), 1, func(Record) ([]byte, bool, error) { return nil, false, nil }); err != nil {
				t.Fatal(err)
			}
			f := &faultyTail{File: log.file, short: short}
			if !short {
				f.syncErr = errors.New("failed sync")
			}
			log.tail.file = f
			if err := log.Append([]Record{{20, []byte("new")}}); !errors.Is(err, ErrLogPoisoned) {
				t.Fatal(err)
			}
			c, _ := log.catalog.Current()
			path := filepath.Join(log.path, c.Active.File)
			data := readBytes(t, path)
			if err := log.Close(); err != nil {
				t.Fatal(err)
			}
			next, err := OpenLog(log.path, log.encoding)
			if short {
				if !errors.Is(err, ErrIncompleteTail) || !bytes.Equal(data, readBytes(t, path)) {
					t.Fatal("changed incomplete tail", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = next.Close() })
			state, _ := next.tail.State()
			if state.Count != 1 || state.OriginalCount != 3 || state.Framed != 51 || state.Last != 20 {
				t.Fatal(state)
			}
			if r, err := next.Read(20); err != nil || string(r.Payload) != "new" {
				t.Fatal(r, err)
			}
		})
	}
}

func TestRewriteEmptyLogAndEraseIDZero(t *testing.T) {
	log := newLog(t, 40)
	result, err := log.Rewrite(t.Context(), 1, func(Record) ([]byte, bool, error) { t.Fatal("visited empty log"); return nil, false, nil })
	if err != nil || result.TailChanged || !result.Published {
		t.Fatal(result, err)
	}
	if err := log.Append([]Record{{0, nil}}); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Rewrite(t.Context(), 2, func(Record) ([]byte, bool, error) { return nil, false, nil }); err != nil {
		t.Fatal(err)
	}
	log = reopenLog(t, log)
	if err := log.Append([]Record{{0, nil}}); !errors.Is(err, ErrInvalid) {
		t.Fatal("reused erased zero", err)
	}
	if err := log.Append([]Record{{1, nil}}); err != nil {
		t.Fatal(err)
	}
}
