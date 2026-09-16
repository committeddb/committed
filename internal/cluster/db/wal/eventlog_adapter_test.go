package wal

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
	"github.com/committeddb/committed/pkg/segmentlog"
)

// The exact same application adapter/reader/selection code runs on both backends.
func TestEventLogAdapterBackends(t *testing.T) {
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
			log, e := create(t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() { _ = log.Close() })
			adapter := &eventLogAdapter{log: log}
			meta := func(i uint64) *cluster.Entity {
				e, err := cluster.NewUpsertSyncableIndexEntity(&cluster.SyncableIndex{ID: "worker", Index: i})
				if err != nil {
					t.Fatal(err)
				}
				return e
			}
			records := [][]byte{selectionEntry(t, 1, meta(1)), selectionEntry(t, 10, meta(2)), experimentEntry(t, 20, pb.EntryNormal, experimentRow("key", "value"))}
			if index, e := adapter.appendCommittedRaw(records); e != nil || index != 20 {
				t.Fatal(index, e)
			}
			applied := uint64(10)
			reader, e := adapter.readerAt(0, segmentTestResolver(segmentTestType), func() uint64 { return applied })
			if e != nil {
				t.Fatal(e)
			}
			if _, e := reader.Read(); !errors.Is(e, io.EOF) || reader.Position() != 10 {
				t.Fatal(e)
			}
			applied = 20
			actual, e := reader.Read()
			if e != nil || actual.Index != 20 {
				t.Fatal(actual, e)
			}
			actual, e = adapter.actualAt(20, segmentTestResolver(segmentTestType), func() uint64 { return applied })
			if e != nil || actual.Index != 20 {
				t.Fatal(actual, e)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			protected, e := adapter.protectedReaderAt(ctx, 0, segmentTestResolver(segmentTestType), func() uint64 { return applied })
			if e != nil {
				t.Fatal(e)
			}
			if _, e := adapter.rewriteMetadata(t.Context(), 1, 10, func() uint64 { return applied }); !errors.Is(e, errEventRewriteDeferred) {
				t.Fatal(e)
			}
			if e := protected.Close(); e != nil {
				t.Fatal(e)
			}
			result, e := adapter.rewriteMetadata(t.Context(), 1, 10, func() uint64 { return applied })
			if e != nil || !result.Published || result.ChangedRecords != 1 {
				t.Fatal(result, e)
			}
			if _, e := adapter.readRaw(1); !errors.Is(e, eventlog.ErrNotFound) {
				t.Fatal(e)
			}
			if index, e := adapter.appendCommittedRaw(records); e != nil || index != 20 {
				t.Fatal(index, e)
			}
			if _, e := adapter.readRaw(1); !errors.Is(e, eventlog.ErrNotFound) {
				t.Fatal("replay resurrected metadata", e)
			}
			if _, e := log.Reclaim(t.Context()); e != nil {
				t.Fatal(e)
			}
			var count int
			if e := adapter.scanRaw(t.Context(), eventlog.Coverage{Start: 1, End: 21}, func(uint64, []byte) error { count++; return nil }); e != nil || count != 2 {
				t.Fatal(count, e)
			}
		})
	}
}
