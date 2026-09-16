package wal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
	"github.com/committeddb/committed/pkg/segmentlog"
)

func TestEventLogProtectedReadConcurrentReleaseAndRewrite(t *testing.T) {
	factories := map[string]func(string) (eventlog.EventLog, error){
		"tidwall": func(path string) (eventlog.EventLog, error) {
			return tidwall.Create(path, 1, tidwall.Options{SegmentBytes: 128, Compress: true})
		},
		"segmented": func(path string) (eventlog.EventLog, error) {
			return segmented.Create(path, 1, segmentlog.LogOptions{SegmentBytes: 128, Encoding: segmentlog.Options{Compression: segmentlog.ZstdDefault}})
		},
	}
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
			resolver := segmentTestResolver(func(ref cluster.TypeRef) (*cluster.Type, error) {
				close(entered)
				<-release
				return segmentTestType(ref)
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
