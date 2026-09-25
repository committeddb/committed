package wal

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"
	"github.com/committeddb/committed/pkg/segmentlog"
)

type observedEventPublication struct {
	eventlog.EventLog
	entered chan struct{}
}

type eventPublicationLock struct {
	sync.Locker
	entered chan struct{}
}

func (l eventPublicationLock) Lock() { close(l.entered); l.Locker.Lock() }

func (l observedEventPublication) RewriteWithPublicationLock(ctx context.Context, generation uint64, transform eventlog.Transform, lock sync.Locker) (eventlog.RewriteResult, error) {
	return l.EventLog.RewriteWithPublicationLock(ctx, generation, transform, eventPublicationLock{lock, l.entered})
}

func TestEventLogActualReadDuringRewritePreparation(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprintf("cached=%t", cached), func(t *testing.T) {
			opts := segmentlog.LogOptions{SegmentBytes: 16}
			if cached {
				opts.Cache = segmentlog.CacheOptions{RecentBytes: 1024, HistoricalBytes: 1024}
			}
			log, err := segmented.Create(t.TempDir(), 1, opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = log.Close() })
			publishing := make(chan struct{})
			adapter := &eventLogAdapter{log: observedEventPublication{log, publishing}}
			first := experimentEntry(t, 10, pb.EntryNormal, experimentRow("one", "old"))
			second := experimentEntry(t, 20, pb.EntryNormal, experimentRow("two", "old"))
			third := experimentEntry(t, 30, pb.EntryNormal, experimentRow("three", "new"))
			if err := adapter.appendRaw([][]byte{first, second}); err != nil {
				t.Fatal(err)
			}
			transforming, transformRelease := make(chan struct{}), make(chan struct{})
			releaseTransform := sync.OnceFunc(func() { close(transformRelease) })
			defer releaseTransform()
			rewriteDone := make(chan error, 1)
			go func() {
				_, err := adapter.rewriteRaw(t.Context(), 1, func(raw []byte) (bool, []byte, error) {
					if bytes.Equal(raw, first) {
						close(transforming)
						<-transformRelease
						return false, nil, nil
					}
					return true, raw, nil
				})
				rewriteDone <- err
			}()
			await := func(ch <-chan struct{}) {
				t.Helper()
				select {
				case <-ch:
				case <-time.After(10 * time.Second):
					t.Fatal("operation did not reach barrier")
				}
			}
			await(transforming)
			decoding, decodeRelease := make(chan struct{}), make(chan struct{})
			releaseDecode := sync.OnceFunc(func() { close(decodeRelease) })
			defer releaseDecode()
			var once sync.Once
			resolver := eventTestResolver(func(ref cluster.TypeRef) (*cluster.Type, error) {
				once.Do(func() { close(decoding); <-decodeRelease })
				return eventTestType(ref)
			})
			reader, err := adapter.readerAt(0, resolver, func() uint64 { return 30 })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reader.Close() })
			readDone := make(chan error, 1)
			go func() {
				a, err := reader.Read()
				if err == nil && (a == nil || a.Index != 10) {
					err = fmt.Errorf("unexpected original Actual: %v", a)
				}
				readDone <- err
			}()
			await(decoding)
			// Queue both operations while preparation is active. Neither may take the
			// publication read lock while waiting for the engine's mutation ownership.
			appendDone, protectedDone := make(chan error, 1), make(chan error, 1)
			go func() { appendDone <- adapter.appendRaw([][]byte{third}) }()
			go func() {
				ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
				defer cancel()
				r, err := adapter.protectedReaderAt(ctx, 0, eventTestResolver(eventTestType), func() uint64 { return 30 })
				if err == nil {
					err = r.Close()
				}
				protectedDone <- err
			}()
			releaseTransform()
			await(publishing)
			select {
			case err := <-rewriteDone:
				t.Fatalf("rewrite published during decoding: %v", err)
			default:
			}
			select {
			case err := <-protectedDone:
				t.Fatalf("protected reader registered during rewrite: %v", err)
			default:
			}
			releaseDecode()
			for _, ch := range []<-chan error{readDone, rewriteDone, appendDone, protectedDone} {
				select {
				case err := <-ch:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("publication lock-order deadlock")
				}
			}
			// The same cursor crosses publication using stable Raft indexes.
			a, err := reader.Read()
			if err != nil || a == nil || a.Index != 20 {
				t.Fatal(a, err)
			}
			fresh, err := adapter.readerAt(0, eventTestResolver(eventTestType), func() uint64 { return 30 })
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = fresh.Close() }()
			a, err = fresh.Read()
			if err != nil || a == nil || a.Index != 20 {
				t.Fatal("erased Actual visible after publication", a, err)
			}
			a, err = fresh.Read()
			if err != nil || a == nil || a.Index != 30 {
				t.Fatal("queued append lost", a, err)
			}
		})
	}
}
