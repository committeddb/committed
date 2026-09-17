package segmentlog

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

const boltCrashExit = 73

// The helper exits without running deferred cleanup, closing the log, or rolling
// back an open transaction. This exercises process death, not loss of power or
// writes reordered by a storage device.
func TestBoltLogCrashHelper(t *testing.T) {
	dir := os.Getenv("SEGMENTLOG_CRASH_DIR")
	if dir == "" {
		return
	}
	l, err := OpenBoltLog(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	operation, stage := os.Getenv("SEGMENTLOG_CRASH_OPERATION"), os.Getenv("SEGMENTLOG_CRASH_STAGE")
	s := l.catalog.(*boltCatalog)
	s.commit = func(fn func(*bolt.Tx) error) error {
		if stage == "before" {
			os.Exit(boltCrashExit)
		}
		err := s.db.Update(func(tx *bolt.Tx) error {
			if e := fn(tx); e != nil {
				return e
			}
			if stage == "inside" {
				os.Exit(boltCrashExit)
			}
			return nil
		})
		if err == nil && stage == "after" {
			os.Exit(boltCrashExit)
		}
		return err
	}
	if stage == "first-removal" {
		l.remover = crashAfterRemoval{l.remover}
	}
	switch operation {
	case "rollover":
		err = l.Append([]Record{{20, nil}})
	case "rewrite":
		_, err = l.Rewrite(t.Context(), 1, eraseEvenTens)
	case "reclaim":
		_, err = l.Reclaim(t.Context())
	case "orphans":
		_, err = l.ReclaimOrphans(t.Context())
	default:
		t.Fatal("unknown operation", operation)
	}
	t.Fatal("did not reach crash boundary", err)
}

type crashAfterRemoval struct{ fileRemover }

func (r crashAfterRemoval) Remove(name string) (bool, error) {
	removed, err := r.fileRemover.Remove(name)
	if err == nil && removed {
		os.Exit(boltCrashExit)
	}
	return removed, err
}

func eraseEvenTens(r Record) ([]byte, bool, error) {
	return r.Payload, r.ID%20 != 0, nil
}

func runBoltCrash(t *testing.T, dir, operation, stage string) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestBoltLogCrashHelper$", "-test.count=1")
	cmd.Env = append(os.Environ(), "SEGMENTLOG_CRASH_DIR="+dir,
		"SEGMENTLOG_CRASH_OPERATION="+operation, "SEGMENTLOG_CRASH_STAGE="+stage)
	output, err := cmd.CombinedOutput()
	var exited *exec.ExitError
	if ctx.Err() != nil || !errors.As(err, &exited) || exited.ExitCode() != boltCrashExit {
		t.Fatalf("helper did not exit at boundary: %v (context %v)\n%s", err, ctx.Err(), output)
	}
}

func TestBoltLogProcessCrash(t *testing.T) {
	for _, operation := range []string{"rollover", "rewrite", "reclaim"} {
		stages := []string{"before", "inside", "after"}
		if operation == "reclaim" {
			stages = append(stages, "first-removal")
		}
		for _, stage := range stages {
			t.Run(operation+"/"+stage, func(t *testing.T) {
				l := newBoltLog(t, 32)
				records := []Record{{0, nil}, {10, nil}}
				if operation != "rollover" {
					records = append(records, Record{20, nil}, Record{30, nil}, Record{40, nil})
				}
				if err := l.Append(records); err != nil {
					t.Fatal(err)
				}
				original, err := l.catalog.Current()
				if err != nil {
					t.Fatal(err)
				}
				if operation == "reclaim" {
					if _, err := l.Rewrite(t.Context(), 1, eraseEvenTens); err != nil {
						t.Fatal(err)
					}
				}
				before, err := l.catalog.Current()
				if err != nil {
					t.Fatal(err)
				}
				if err := l.Close(); err != nil {
					t.Fatal(err)
				}
				runBoltCrash(t, l.path, operation, stage)
				l = reopenBoltLog(t, l)
				after, err := l.catalog.Current()
				if err != nil {
					t.Fatal(err)
				}
				published := operation != "reclaim" && stage == "after"
				if !published && !reflect.DeepEqual(before, after) {
					t.Fatalf("uncommitted layout changed: before %+v, after %+v", before, after)
				}
				if published && after.Revision != before.Revision+1 {
					t.Fatalf("committed revision missing: %+v", after)
				}
				erased := operation == "reclaim" || (operation == "rewrite" && published)
				queued := 0
				if erased && (operation != "reclaim" || stage != "after") {
					queued = 3 // Two closed ranges and the active tail.
				}
				if err := l.catalog.(*boltCatalog).db.View(func(tx *bolt.Tx) error {
					if got := tx.Bucket(boltRetiredBucket).Stats().KeyN; got != queued {
						t.Errorf("recovered retirement count = %d, want %d", got, queued)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				checkBoltCrashRecords(t, l, records, erased)
				if operation == "rollover" {
					if _, err := l.Read(20); !errors.Is(err, ErrNotFound) {
						t.Fatal("append passed interrupted publication", err)
					}
				}
				// Retried reclamation must tolerate a queue whose files were already
				// removed, and must never delete the selected replacements.
				if _, err := l.Reclaim(t.Context()); err != nil {
					t.Fatal(err)
				}
				if result, err := l.Reclaim(t.Context()); err != nil || result.RemovedFiles != 0 {
					t.Fatal(result, err)
				}
				if err := l.catalog.(*boltCatalog).db.View(func(tx *bolt.Tx) error {
					if k, _ := tx.Bucket(boltRetiredBucket).Cursor().First(); k != nil {
						t.Error("retirement queue was not drained")
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if erased {
					names := make([]string, 0, len(original.Segments)+1)
					for _, ref := range original.Segments {
						names = append(names, ref.File)
					}
					names = append(names, original.Active.File)
					for _, name := range names {
						if _, err := os.Stat(filepath.Join(l.path, name)); !errors.Is(err, os.ErrNotExist) {
							t.Fatal("retired file still present", name, err)
						}
					}
				}
				checkBoltCrashRecords(t, l, records, erased)
				wantOrphans := uint64(0)
				if !published && operation == "rollover" {
					wantOrphans = 1
				} else if !published && operation == "rewrite" {
					wantOrphans = 3
				}
				if result, err := l.ReclaimOrphans(t.Context()); err != nil || result.RemovedFiles != wantOrphans {
					t.Fatal("orphan sweep", result, "want", wantOrphans, err)
				}
				checkBoltCrashRecords(t, l, records, erased)
				if err := l.Verify(t.Context()); err != nil {
					t.Fatal(err)
				}
				// Appending after recovery also exercises the newly selected empty
				// tail and its checkpoint when the highest record was erased.
				next := records[len(records)-1].ID + 10
				if err := l.Append([]Record{{next, []byte("after recovery")}}); err != nil {
					t.Fatal(err)
				}
				l = reopenBoltLog(t, l)
				if record, err := l.Read(next); err != nil || string(record.Payload) != "after recovery" {
					t.Fatal(record, err)
				}
			})
		}
	}
}

func checkBoltCrashRecords(t *testing.T, l *Log, records []Record, erased bool) {
	t.Helper()
	want := make([]uint64, 0, len(records))
	for _, record := range records {
		if !erased || record.ID%20 != 0 {
			want = append(want, record.ID)
		}
	}
	var got []uint64
	if err := l.Scan(t.Context(), Coverage{0, 100}, func(r Record) error {
		got = append(got, r.ID)
		return nil
	}); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("recovered records", got, "want", want, err)
	}
	if id, ok, err := l.LastAppended(); err != nil || !ok || id != records[len(records)-1].ID {
		t.Fatal("lost append progress", id, ok, err)
	}
}
