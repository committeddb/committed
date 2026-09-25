package wal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"
	"github.com/committeddb/committed/pkg/segmentlog"
)

func TestEventLogProtectedReadConcurrentReleaseAndRewrite(t *testing.T) {
	factories := eventLogTestBackends()
	for name, create := range factories {
		t.Run(name, func(t *testing.T) {
			log, err := create(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = log.Close() })
			adapter := &eventLogAdapter{log: log}
			if err := adapter.appendRaw([][]byte{experimentEntry(t, 10, pb.EntryNormal, experimentRow("key", "value"))}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			resolver := eventTestResolver(func(ref cluster.TypeRef) (*cluster.Type, error) {
				close(entered)
				<-release
				return eventTestType(ref)
			})
			reader, err := adapter.protectedReaderAt(ctx, 0, resolver, func() uint64 { return 100 })
			if err != nil {
				t.Fatal(err)
			}
			readDone := make(chan error, 1)
			go func() {
				actual, err := reader.Read()
				if actual != nil {
					readDone <- errors.New("canceled read returned an Actual")
					return
				}
				readDone <- err
			}()
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("read did not enter resolver")
			}
			if adapter.mu.TryLock() {
				adapter.mu.Unlock()
				t.Fatal("decode did not hold adapter read lock")
			}
			cancel()
			closeDone := make(chan error, 1)
			var closers sync.WaitGroup
			for range 4 {
				closers.Go(func() { _ = reader.Close() })
			}
			go func() { closers.Wait(); closeDone <- nil }()
			rewriteDone := make(chan error, 1)
			erase := func([]byte) (bool, []byte, error) { return false, nil, nil }
			go func() { _, err := adapter.rewriteRaw(context.Background(), 1, erase); rewriteDone <- err }()
			select {
			case err := <-rewriteDone:
				t.Fatalf("rewrite finished during decode: %v", err)
			case err := <-closeDone:
				t.Fatalf("close finished during decode: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			if adapter.protectedReadCount() != 1 {
				t.Fatal("in-flight read lost protection")
			}
			unblock()
			wait := func(done <-chan error) error {
				t.Helper()
				select {
				case err := <-done:
					return err
				case <-time.After(10 * time.Second):
					t.Fatal("adapter operation did not finish")
					return nil
				}
			}
			if err := wait(readDone); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if err := wait(closeDone); err != nil {
				t.Fatal(err)
			}
			if reader.Position() != 0 || adapter.protectedReadCount() != 0 {
				t.Fatal("canceled read advanced or leaked protection")
			}
			err = wait(rewriteDone)
			// A rewrite that wins the lock before release must defer; one that follows
			// release may publish. Both must become usable after reader closure completes.
			if errors.Is(err, errEventRewriteDeferred) {
				_, err = adapter.rewriteRaw(t.Context(), 1, erase)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := adapter.readRaw(10); !errors.Is(err, eventlog.ErrNotFound) {
				t.Fatal("rewrite did not erase record", err)
			}
			if _, err := reader.Read(); !errors.Is(err, context.Canceled) {
				t.Fatal("canceled reader resumed", err)
			}
			if _, err := log.Reclaim(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Followers have independent cursors over shared cached bytes. Most start near
// the head; a few bootstrap from the beginning while appends roll the tail over.
func TestEventLogCachedStreamingReaders(t *testing.T) {
	log, err := segmented.Create(t.TempDir(), 1, segmentlog.LogOptions{
		SegmentBytes: 1024,
		Cache:        segmentlog.CacheOptions{RecentBytes: 8 << 10, HistoricalBytes: 8 << 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	adapter := &eventLogAdapter{log: log}
	var applied atomic.Uint64
	appendThrough := func(first, last uint64) {
		t.Helper()
		records := make([][]byte, 0, last-first+1)
		for id := first; id <= last; id++ {
			records = append(records, experimentEntry(t, id*10, pb.EntryNormal, experimentRow("key", "value")))
		}
		if _, err := adapter.appendCommittedRaw(records); err != nil {
			t.Fatal(err)
		}
		applied.Store(last * 10)
	}
	appendThrough(1, 32)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	var workers sync.WaitGroup
	// Cancel and join readers before closing their log, including on test failure.
	defer func() { cancel(); workers.Wait() }()
	results := make(chan error, 100)
	ready := make(chan struct{}, 100)
	for i := range 100 {
		start := uint64(24)
		if i < 4 {
			start = 0
		}
		reader, err := adapter.readerAt(start*10, eventTestResolver(eventTestType), applied.Load)
		if err != nil {
			t.Fatal(err)
		}
		workers.Go(func() {
			ready <- struct{}{}
			for want := start + 1; want <= 64; {
				if err := ctx.Err(); err != nil {
					results <- err
					return
				}
				actual, err := reader.Read()
				if errors.Is(err, io.EOF) {
					select {
					case <-ctx.Done():
						results <- ctx.Err()
						return
					case <-time.After(time.Millisecond):
					}
					continue
				}
				if err != nil {
					results <- err
					return
				}
				if actual == nil || actual.Index != want*10 || reader.Position() != want*10 {
					results <- fmt.Errorf("reader starting at %d: expected index %d, got %v", start*10, want*10, actual)
					return
				}
				want++
			}
			results <- nil
		})
	}
	for range 100 {
		<-ready
	}
	for first := uint64(33); first <= 64; first += 4 {
		appendThrough(first, first+3)
	}
	for range 100 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}
