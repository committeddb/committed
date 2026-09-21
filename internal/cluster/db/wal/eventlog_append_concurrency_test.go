package wal

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
	"github.com/committeddb/committed/pkg/segmentlog"
)

func TestEventLogAppendDuringActualDecode(t *testing.T) {
	for _, backend := range []string{"tidwall", "segmented", "segmented-cached"} {
		t.Run(backend, func(t *testing.T) {
			var log eventlog.EventLog
			var err error
			if backend == "tidwall" {
				log, err = tidwall.Create(t.TempDir(), 1, tidwall.Options{SegmentBytes: 128})
			} else {
				opts := segmentlog.LogOptions{SegmentBytes: 128}
				if backend == "segmented-cached" {
					opts.Cache = segmentlog.CacheOptions{RecentBytes: 1024, HistoricalBytes: 1024}
				}
				log, err = segmented.Create(t.TempDir(), 1, opts)
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = log.Close() })
			adapter := &eventLogAdapter{log: log}
			first := experimentEntry(t, 10, pb.EntryNormal, experimentRow("one", "value"))
			second := experimentEntry(t, 20, pb.EntryNormal, experimentRow("two", "value"))
			if err := adapter.appendRaw([][]byte{first}); err != nil {
				t.Fatal(err)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			var once sync.Once
			resolver := eventTestResolver(func(ref cluster.TypeRef) (*cluster.Type, error) {
				once.Do(func() { close(entered); <-release })
				return eventTestType(ref)
			})
			var applied atomic.Uint64
			applied.Store(10)
			reader, err := adapter.readerAt(0, resolver, applied.Load)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reader.Close() })
			readDone := make(chan error, 1)
			go func() {
				a, err := reader.Read()
				if err == nil && (a == nil || a.Index != 10) {
					err = fmt.Errorf("unexpected first Actual: %v", a)
				}
				readDone <- err
			}()
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("reader did not pause in decoding")
			}
			// Concurrent replay of the same committed batch must append it once, even
			// while an earlier Actual is still being decoded by a streaming reader.
			appendDone := make(chan error, 8)
			for range 8 {
				go func() {
					index, err := adapter.appendCommittedRaw([][]byte{first, second})
					if err == nil && index != 20 {
						err = fmt.Errorf("frontier %d", index)
					}
					appendDone <- err
				}()
			}
			wait := func(ch <-chan error) {
				t.Helper()
				select {
				case err := <-ch:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("operation blocked during decode")
				}
			}
			for range 8 {
				wait(appendDone)
			}
			applied.Store(20)
			unblock()
			wait(readDone)
			a, err := reader.Read()
			if err != nil || a == nil || a.Index != 20 {
				t.Fatal(a, err)
			}
			count := 0
			if err := adapter.scanRaw(t.Context(), eventlog.Coverage{Start: 1, End: 21}, func(uint64, []byte) error { count++; return nil }); err != nil || count != 2 {
				t.Fatal("concurrent replay duplicated entries", count, err)
			}
		})
	}
}
