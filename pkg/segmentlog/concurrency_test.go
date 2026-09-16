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

func awaitOperation(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("operation did not finish")
		return nil
	}
}

func TestScanLifetimeSerializesMutations(t *testing.T) {
	for _, operation := range []string{"append", "rewrite", "reclaim", "close"} {
		for _, exit := range []string{"complete", "cancel", "error"} {
			t.Run(operation+"/"+exit, func(t *testing.T) {
				log := newLog(t, 32)
				if err := log.Append([]Record{{0, nil}, {10, nil}, {20, nil}}); err != nil {
					t.Fatal(err)
				}
				if _, err := log.Rewrite(t.Context(), 1, func(r Record) ([]byte, bool, error) {
					if r.ID == 0 {
						return []byte("kept"), true, nil
					}
					return r.Payload, true, nil
				}); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				entered, release := make(chan struct{}), make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				defer unblock()
				scanDone := make(chan error, 1)
				var seen []uint64
				stopped := errors.New("visitor stopped")
				go func() {
					scanDone <- log.Scan(ctx, Coverage{0, 30}, func(r Record) error {
						seen = append(seen, r.ID)
						if len(seen) == 1 {
							close(entered)
							<-release
							if exit == "error" {
								return stopped
							}
						}
						return nil
					})
				}()
				select {
				case <-entered:
				case <-time.After(10 * time.Second):
					t.Fatal("scan did not enter callback")
				}
				// Check actual lock ownership at the controlled pause, independently of
				// whether the competing goroutine happens to be scheduled promptly.
				if log.mu.TryLock() {
					log.mu.Unlock()
					t.Fatal("scan released its view during callback")
				}
				started, done := make(chan struct{}), make(chan error, 1)
				go func() {
					close(started)
					var err error
					switch operation {
					case "append":
						err = log.Append([]Record{{30, nil}})
					case "rewrite":
						_, err = log.Rewrite(context.Background(), 2, func(r Record) ([]byte, bool, error) { return r.Payload, r.ID != 0, nil })
					case "reclaim":
						var result ReclaimResult
						result, err = log.Reclaim(context.Background())
						if err == nil && result.RemovedFiles == 0 {
							err = errors.New("reclaim removed no obsolete files")
						}
					case "close":
						err = log.Close()
					}
					done <- err
				}()
				<-started
				if exit == "cancel" {
					cancel()
				}
				select {
				case err := <-done:
					t.Fatalf("%s finished while scan callback was active: %v", operation, err)
				case <-time.After(20 * time.Millisecond):
				}
				unblock()
				err := awaitOperation(t, scanDone)
				want := []uint64{0, 10, 20}
				switch exit {
				case "cancel":
					want = want[:1]
					if !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
				case "error":
					want = want[:1]
					if !errors.Is(err, stopped) {
						t.Fatal(err)
					}
				default:
					if err != nil {
						t.Fatal(err)
					}
				}
				if !reflect.DeepEqual(seen, want) {
					t.Fatal("scan mixed views", seen, want)
				}
				if err := awaitOperation(t, done); err != nil {
					t.Fatal(err)
				}
				if operation == "close" {
					if _, err := log.Seek(0); !errors.Is(err, ErrClosed) {
						t.Fatal(err)
					}
				} else {
					log = reopenLog(t, log)
					if operation == "rewrite" {
						if _, err := log.Read(0); !errors.Is(err, ErrNotFound) {
							t.Fatal(err)
						}
					} else {
						if r, err := log.Read(0); err != nil || string(r.Payload) != "kept" {
							t.Fatal(r, err)
						}
					}
					if operation == "append" {
						if _, err := log.Read(30); err != nil {
							t.Fatal(err)
						}
					}
				}
			})
		}
	}
}

func TestConcurrentLogHistory(t *testing.T) {
	log := newLog(t, 256)
	payload := func(id uint64) []byte { return []byte(fmt.Sprint(id)) }
	if err := log.Append([]Record{{0, payload(0)}}); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	run := func(fn func() error) {
		wg.Go(func() {
			<-start
			if err := fn(); err != nil {
				t.Error(err)
			}
		})
	}
	run(func() error {
		for id := uint64(1); id <= 32; id++ {
			if err := log.Append([]Record{{id, payload(id)}}); err != nil {
				return err
			}
		}
		return nil
	})
	eraseOdd := func(r Record) ([]byte, bool, error) { return r.Payload, r.ID%2 == 0, nil }
	run(func() error {
		for generation := uint64(1); generation <= 4; generation++ {
			if _, err := log.Rewrite(context.Background(), generation, eraseOdd); err != nil {
				return err
			}
		}
		return nil
	})
	run(func() error {
		for range 4 {
			if _, err := log.Reclaim(context.Background()); err != nil {
				return err
			}
		}
		return nil
	})
	for range 3 {
		run(func() error {
			for range 12 {
				var previous uint64
				first := true
				if err := log.Scan(context.Background(), Coverage{0, 33}, func(r Record) error {
					if (!first && r.ID <= previous) || string(r.Payload) != string(payload(r.ID)) {
						return fmt.Errorf("invalid scan record: %v", r)
					}
					first = false
					previous = r.ID
					return nil
				}); err != nil {
					return err
				}
				if r, err := log.Seek(5); err != nil && !errors.Is(err, ErrNotFound) {
					return err
				} else if err == nil && (r.ID < 5 || string(r.Payload) != string(payload(r.ID))) {
					return fmt.Errorf("invalid seek record: %v", r)
				}
			}
			return nil
		})
	}
	close(start)
	done := make(chan error, 1)
	go func() { wg.Wait(); done <- nil }()
	_ = awaitOperation(t, done)
	if t.Failed() {
		return
	}
	// A final ordered rewrite makes the expected history independent of which
	// concurrent append/rewrite won each lock acquisition.
	if _, err := log.Rewrite(t.Context(), 5, eraseOdd); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Reclaim(t.Context()); err != nil {
		t.Fatal(err)
	}
	log = reopenLog(t, log)
	for id := uint64(0); id <= 32; id++ {
		r, err := log.Read(id)
		if id%2 == 1 {
			if !errors.Is(err, ErrNotFound) {
				t.Fatal(id, r, err)
			}
		} else if err != nil || string(r.Payload) != string(payload(id)) {
			t.Fatal(id, r, err)
		}
	}
	if id, ok, err := log.LastAppended(); err != nil || !ok || id != 32 {
		t.Fatal(id, ok, err)
	}
}

func TestConcurrentClosePreservesAcknowledgedAppends(t *testing.T) {
	log := newLog(t, 128)
	if err := log.Append([]Record{{0, []byte("value")}}); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	lastAck := make(chan uint64, 1)
	writerDone := make(chan error, 1)
	go func() {
		<-start
		last := uint64(0)
		defer func() { lastAck <- last }()
		for id := uint64(1); id <= 16; id++ {
			err := log.Append([]Record{{id, []byte("value")}})
			if errors.Is(err, ErrClosed) {
				writerDone <- nil
				return
			}
			if err != nil {
				writerDone <- err
				return
			}
			last = id
		}
		writerDone <- nil
	}()
	readDone := make(chan error, 1)
	go func() {
		<-start
		for range 16 {
			r, err := log.Read(0)
			if errors.Is(err, ErrClosed) {
				readDone <- nil
				return
			}
			if err != nil || string(r.Payload) != "value" {
				readDone <- fmt.Errorf("concurrent read: %v, %w", r, err)
				return
			}
		}
		readDone <- nil
	}()
	closes := make([]chan error, 2)
	for i := range closes {
		closes[i] = make(chan error, 1)
		done := closes[i]
		go func() { <-start; done <- log.Close() }()
	}
	close(start)
	for _, done := range closes {
		if err := awaitOperation(t, done); err != nil {
			t.Fatal(err)
		}
	}
	if err := awaitOperation(t, writerDone); err != nil {
		t.Fatal(err)
	}
	if err := awaitOperation(t, readDone); err != nil {
		t.Fatal(err)
	}
	last := <-lastAck
	log = reopenLog(t, log)
	for id := uint64(0); id <= last; id++ {
		if r, err := log.Read(id); err != nil || string(r.Payload) != "value" {
			t.Fatal("lost acknowledged append", id, r, err)
		}
	}
	if _, err := log.Read(last + 1); !errors.Is(err, ErrNotFound) {
		t.Fatal("unacknowledged append survived clean close", err)
	}
	if id, ok, err := log.LastAppended(); err != nil || !ok || id != last {
		t.Fatal("recovered wrong append position", id, ok, err)
	}
}
