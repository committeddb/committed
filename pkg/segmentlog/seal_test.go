package segmentlog

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

func scannedIDs(t *testing.T, l *Log) []uint64 {
	t.Helper()
	var ids []uint64
	if err := l.Scan(t.Context(), Coverage{0, ^uint64(0)}, func(r Record) error {
		ids = append(ids, r.ID)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return ids
}

func TestSealClosesTheActiveTail(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprintf("cache=%t", cached), func(t *testing.T) {
			l := newLog(t, 1<<20)
			if cached {
				l = cachedLog(t, 1<<20)
			}
			if err := l.Append([]Record{{1, []byte("one")}, {10, []byte("ten")}}); err != nil {
				t.Fatal(err)
			}
			end, err := l.Seal()
			if err != nil || end != 11 {
				t.Fatal(end, err)
			}
			c, err := l.InspectCatalog()
			if err != nil {
				t.Fatal(err)
			}
			if c.Active.Start != 11 || len(c.Segments) != 1 || c.Segments[0].Coverage != (Coverage{0, 11}) || c.Segments[0].Count != 2 {
				t.Fatalf("%+v", c)
			}
			// An empty tail stays: a second seal publishes nothing.
			if end, err = l.Seal(); err != nil || end != 11 {
				t.Fatal(end, err)
			}
			if again, err := l.InspectCatalog(); err != nil || again.Revision != c.Revision {
				t.Fatal(again.Revision, c.Revision, err)
			}
			if err := l.Append([]Record{{12, []byte("twelve")}}); err != nil {
				t.Fatal(err)
			}
			if got := scannedIDs(t, l); !reflect.DeepEqual(got, []uint64{1, 10, 12}) {
				t.Fatal(got)
			}
			l = reopenLog(t, l)
			if got := scannedIDs(t, l); !reflect.DeepEqual(got, []uint64{1, 10, 12}) {
				t.Fatal(got)
			}
			result, err := l.RewriteSealed(t.Context(), 1, func(r Record) ([]byte, bool, error) { return r.Payload, r.ID != 10, nil })
			if err != nil || result.SealedEnd != 11 || result.ChangedSegments != 1 {
				t.Fatal(result, err)
			}
			if got := scannedIDs(t, l); !reflect.DeepEqual(got, []uint64{1, 12}) {
				t.Fatal(got)
			}
		})
	}
}

// A tail whose records a whole-log rewrite erased keeps its append history,
// so sealing it still advances the scope past every ID appended.
func TestSealErasedTail(t *testing.T) {
	l := newLog(t, 1<<20)
	if err := l.Append([]Record{{1, []byte("one")}, {2, []byte("two")}}); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Rewrite(t.Context(), 1, func(Record) ([]byte, bool, error) { return nil, false, nil }); err != nil {
		t.Fatal(err)
	}
	end, err := l.Seal()
	if err != nil || end != 3 {
		t.Fatal(end, err)
	}
	if err := l.Append([]Record{{3, []byte("three")}}); err != nil {
		t.Fatal(err)
	}
	l = reopenLog(t, l)
	if got := scannedIDs(t, l); !reflect.DeepEqual(got, []uint64{3}) {
		t.Fatal(got)
	}
}

// Holders of the caller's read lock may append while a sealed rewrite
// prepares; publication then waits for them.
func TestSealedRewritePublicationLockAllowsAppends(t *testing.T) {
	l := newLog(t, 1<<20)
	if err := l.Append([]Record{{1, []byte("one")}, {2, []byte("two")}}); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Seal(); err != nil {
		t.Fatal(err)
	}
	var reads sync.RWMutex
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		first := true
		_, err := l.RewriteSealedWithPublicationLock(t.Context(), 1, func(r Record) ([]byte, bool, error) {
			if first {
				first = false
				close(entered)
				<-release
			}
			return r.Payload, r.ID != 1, nil
		}, &reads)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("rewrite did not pause")
	}
	appended := make(chan error, 1)
	go func() {
		reads.RLock()
		defer reads.RUnlock()
		appended <- l.Append([]Record{{3, []byte("three")}})
	}()
	select {
	case err := <-appended:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("append waited for the sealed rewrite")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := scannedIDs(t, l); !reflect.DeepEqual(got, []uint64{2, 3}) {
		t.Fatal(got)
	}
}
