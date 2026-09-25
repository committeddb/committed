package segmentlog

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type observedPublicationLock struct {
	sync.RWMutex
	entered chan struct{}
}

func (g *observedPublicationLock) Lock() {
	close(g.entered)
	g.RWMutex.Lock()
}

func TestRewritePublicationLock(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for _, cancelWhileWaiting := range []bool{false, true} {
			t.Run(fmt.Sprintf("cached=%t/cancel=%t", cached, cancelWhileWaiting), func(t *testing.T) {
				var l *Log
				if cached {
					l = cachedLog(t, 42)
				} else {
					l = newLog(t, 42)
				}
				if err := l.Append([]Record{{1, []byte("value")}, {10, []byte("value")}, {20, []byte("value")}}); err != nil {
					t.Fatal(err)
				}
				gate := &observedPublicationLock{entered: make(chan struct{})}
				gate.RLock()
				release := sync.OnceFunc(gate.RUnlock)
				defer release()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				calls := 0
				var result RewriteResult
				go func() {
					var err error
					result, err = l.RewriteWithPublicationLock(ctx, 1, func(r Record) ([]byte, bool, error) { calls++; r.Payload[0] = 'X'; return r.Payload, true, nil }, gate)
					done <- err
				}()
				select {
				case <-gate.entered:
				case <-time.After(10 * time.Second):
					t.Fatal("preparation waited for publication lock")
				}
				if calls != 3 {
					t.Fatal("publication lock acquired before preparation finished", calls)
				}
				// The reader still owns the old view and can make additional log calls
				// while publication waits; this catches the gate/mu lock-order deadlock.
				reads := make(chan error, 1)
				go func() {
					for _, id := range []uint64{1, 10, 20} {
						r, err := l.Read(id)
						if err != nil {
							reads <- err
							return
						}
						if string(r.Payload) != "value" {
							reads <- errors.New("unpublished replacement visible")
							return
						}
					}
					reads <- nil
				}()
				if err := awaitOperation(t, reads); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-done:
					t.Fatalf("publication ignored read lifetime: %v", err)
				default:
				}
				if cancelWhileWaiting {
					cancel()
				}
				release()
				err := awaitOperation(t, done)
				if cancelWhileWaiting {
					if !errors.Is(err, context.Canceled) || result.Published {
						t.Fatal(result, err)
					}
				} else if err != nil || !result.Published {
					t.Fatal(result, err)
				}
				// Publication must release the caller's lock on either path.
				if !gate.RWMutex.TryLock() {
					t.Fatal("publication lock leaked")
				}
				gate.RWMutex.Unlock()
				l = reopenLog(t, l)
				want := "Xalue"
				if cancelWhileWaiting {
					want = "value"
				}
				for _, id := range []uint64{1, 10, 20} {
					r, err := l.Read(id)
					if err != nil || string(r.Payload) != want {
						t.Fatal(r, err)
					}
				}
				if err := l.Verify(t.Context()); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestRewriteRequiresPublicationLock(t *testing.T) {
	l := newLog(t, 42)
	_, err := l.RewriteWithPublicationLock(t.Context(), 1, func(r Record) ([]byte, bool, error) { return r.Payload, true, nil }, nil)
	if !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := l.Append([]Record{{1, []byte("value")}}); err != nil {
		t.Fatal("invalid input poisoned log", err)
	}
}
