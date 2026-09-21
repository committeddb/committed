package segmentlog

import (
	"sync"
	"testing"
	"time"
)

// Signal while the rewrite still holds mu, before source acquisition starts.
type acquisitionBarrierLayout struct {
	layout
	entered chan struct{}
}

func (c acquisitionBarrierLayout) preflight() error {
	err := c.layout.preflight()
	close(c.entered)
	return err
}

func TestSealedRewriteAcquisitionAllowsTailAccess(t *testing.T) {
	l := cachedLog(t, 42)
	if err := l.Append([]Record{{1, []byte("value")}, {10, []byte("value")}, {20, []byte("value")}}); err != nil {
		t.Fatal(err)
	}
	// Start cold and block acquisition at the cache boundary. Live-tail reads
	// and an append that fits the tail must complete before acquisition resumes.
	l.cache = newSegmentCache(0, 1<<20)
	l.cache.mu.Lock()
	unblock := sync.OnceFunc(l.cache.mu.Unlock)
	defer unblock()
	entered := make(chan struct{})
	l.catalog = acquisitionBarrierLayout{l.catalog, entered}
	done := make(chan error, 1)
	go func() {
		_, err := l.RewriteSealed(t.Context(), 1, func(r Record) ([]byte, bool, error) {
			r.Payload[0] = 'X'
			return r.Payload, true, nil
		})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("rewrite did not reach preparation")
	}
	access := make(chan error, 1)
	go func() {
		_, err := l.Read(20)
		if err == nil {
			err = l.Append([]Record{{30, []byte("later")}})
		}
		access <- err
	}()
	if err := awaitOperation(t, access); err != nil {
		t.Fatal(err)
	}
	unblock()
	if err := awaitOperation(t, done); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[uint64]string{1: "Xalue", 10: "Xalue", 20: "value", 30: "later"} {
		r, err := l.Read(id)
		if err != nil || string(r.Payload) != want {
			t.Fatalf("record %d: %q, %v", id, r.Payload, err)
		}
	}
	if err := l.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
}
