package segmentlog

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestSealedRewriteConcurrentRollover(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for _, cancelRewrite := range []bool{false, true} {
			t.Run(fmt.Sprintf("cache=%t/cancel=%t", cached, cancelRewrite), func(t *testing.T) {
				var l *Log
				if cached {
					l = cachedLog(t, 42)
				} else {
					l = newLog(t, 42)
				}
				if err := l.Append([]Record{{1, []byte("value")}, {10, []byte("value")}, {20, []byte("value")}}); err != nil {
					t.Fatal(err)
				}
				before, err := l.InspectCatalog()
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				entered, release := make(chan struct{}), make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				defer unblock()
				done := make(chan error, 1)
				var result SealedRewriteResult
				calls := 0
				go func() {
					var err error
					result, err = l.RewriteSealed(ctx, 1, func(r Record) ([]byte, bool, error) {
						calls++
						if calls == 1 {
							close(entered)
							<-release
						}
						r.Payload[0] = 'X'
						return r.Payload, true, nil
					})
					done <- err
				}()
				select {
				case <-entered:
				case <-time.After(10 * time.Second):
					t.Fatal("rewrite did not pause")
				}
				// An iterator transaction spanning preparation can deadlock rollover when
				// bbolt needs to grow its mmap. No read transaction may remain here.
				if n := l.catalog.(*boltCatalog).db.Stats().OpenTxN; n != 0 {
					t.Fatal("preparation pinned catalog transaction", n)
				}
				appended := make(chan error, 1)
				go func() {
					for i := uint64(0); i < 128; i++ {
						if err := l.Append([]Record{{30 + i*10, []byte("later")}}); err != nil {
							appended <- err
							return
						}
					}
					appended <- nil
				}()
				if err := awaitOperation(t, appended); err != nil {
					t.Fatal(err)
				}
				during, err := l.InspectCatalog()
				if err != nil {
					t.Fatal(err)
				}
				if len(during.Segments) <= len(before.Segments) || during.Generation != before.Generation {
					t.Fatal("rollover did not extend the old generation")
				}
				if r, err := l.Read(1); err != nil || string(r.Payload) != "value" {
					t.Fatal("unpublished rewrite visible", r, err)
				}
				if cancelRewrite {
					cancel()
				}
				unblock()
				err = awaitOperation(t, done)
				if cancelRewrite {
					if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrLogPoisoned) || result.Published {
						t.Fatal(result, err)
					}
				} else if err != nil || !result.Published || result.SealedEnd != before.Active.Start || calls != 2 {
					t.Fatal(result, calls, err)
				}
				l = reopenLog(t, l)
				after, err := l.InspectCatalog()
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(after.Segments[1:], during.Segments[1:]) || !reflect.DeepEqual(after.Active, during.Active) {
					t.Fatal("rewrite changed appended ranges or active tail")
				}
				want := "Xalue"
				if cancelRewrite {
					want = "value"
				}
				for _, id := range []uint64{1, 10} {
					r, err := l.Read(id)
					if err != nil || string(r.Payload) != want {
						t.Fatal(r, err)
					}
				}
				if r, err := l.Read(20); err != nil || string(r.Payload) != "value" {
					t.Fatal("rewrote the captured active tail", r, err)
				}
				for i := uint64(0); i < 128; i++ {
					r, err := l.Read(30 + i*10)
					if err != nil || string(r.Payload) != "later" {
						t.Fatal("lost acknowledged append", r, err)
					}
				}
				if err := l.Verify(t.Context()); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestSealedRewriteRejectsConcurrentAppendFailure(t *testing.T) {
	l := cachedLog(t, 42)
	if err := l.Append([]Record{{1, []byte("value")}, {10, []byte("value")}, {20, []byte("value")}}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	done := make(chan error, 1)
	go func() {
		_, err := l.RewriteSealed(t.Context(), 1, func(r Record) ([]byte, bool, error) {
			if r.ID == 1 {
				close(entered)
				<-release
			}
			return nil, false, nil
		})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("rewrite did not pause")
	}
	boom := errors.New("append sync failed")
	l.mu.Lock()
	l.tail.file = &faultyTail{File: l.file, syncErr: boom}
	l.mu.Unlock()
	if err := l.Append([]Record{{30, []byte("later")}}); !errors.Is(err, boom) {
		t.Fatal(err)
	}
	unblock()
	if err := awaitOperation(t, done); !errors.Is(err, ErrLogPoisoned) {
		t.Fatal(err)
	}
	l = reopenLog(t, l)
	if r, err := l.Read(1); err != nil || string(r.Payload) != "value" {
		t.Fatal("published after append failure", r, err)
	}
}

func TestSealedRewritesRemainSerialized(t *testing.T) {
	l := cachedLog(t, 42)
	if err := l.Append([]Record{{1, []byte("value")}, {10, []byte("value")}, {20, []byte("value")}}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	first := make(chan error, 1)
	go func() {
		_, err := l.RewriteSealed(t.Context(), 1, func(r Record) ([]byte, bool, error) {
			if r.ID == 1 {
				close(entered)
				<-release
			}
			r.Payload[0] = 'X'
			return r.Payload, true, nil
		})
		first <- err
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("rewrite did not pause")
	}
	if l.maintenanceMu.TryLock() {
		l.maintenanceMu.Unlock()
		t.Fatal("rewrite lost maintenance ownership")
	}
	second := make(chan error, 1)
	go func() {
		_, err := l.RewriteSealed(t.Context(), 2, func(r Record) ([]byte, bool, error) {
			if r.Payload[0] != 'X' {
				return nil, false, errors.New("second rewrite observed unpublished generation")
			}
			r.Payload[0] = 'Y'
			return r.Payload, true, nil
		})
		second <- err
	}()
	select {
	case err := <-second:
		t.Fatalf("second rewrite escaped: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	unblock()
	if err := awaitOperation(t, first); err != nil {
		t.Fatal(err)
	}
	if err := awaitOperation(t, second); err != nil {
		t.Fatal(err)
	}
	if r, err := l.Read(1); err != nil || string(r.Payload) != "Yalue" {
		t.Fatal(r, err)
	}
}
