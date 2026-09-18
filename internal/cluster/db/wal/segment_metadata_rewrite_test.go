package wal

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/pkg/segmentlog"
)

func TestSegmentMetadataRewriteBoundAndPartialRecord(t *testing.T) {
	adapter, path := newSegmentEventExperiment(t)
	meta := func(index uint64) *cluster.Entity {
		entity, err := cluster.NewUpsertSyncableIndexEntity(&cluster.SyncableIndex{ID: "worker", Index: index})
		if err != nil {
			t.Fatal(err)
		}
		return entity
	}
	user := cluster.NewUpsertEntity(&cluster.Type{ID: "items", Version: 1}, []byte("key"), []byte("retained"))
	records := [][]byte{selectionEntry(t, 5, meta(1)), selectionEntry(t, 10, meta(2), user), selectionEntry(t, 20, meta(3)), selectionEntry(t, 30, meta(4))}
	if err := adapter.appendRaw(records); err != nil {
		t.Fatal(err)
	}
	result, err := adapter.rewriteMetadata(t.Context(), 1, 20, func() uint64 { return 20 })
	if err != nil || !result.Published || result.ChangedRecords == 0 {
		t.Fatal(result, err)
	}
	if _, err := adapter.readRaw(5); !errors.Is(err, eventlog.ErrNotFound) {
		t.Fatal("superseded metadata retained", err)
	}
	raw, err := adapter.readRaw(10)
	if err != nil {
		t.Fatal(err)
	}
	entry := new(pb.Entry)
	if err := proto.Unmarshal(raw, entry); err != nil {
		t.Fatal(err)
	}
	proposal := new(cluster.Proposal)
	if err := proposal.Unmarshal(entry.Data, segmentTestResolver(segmentTestType)); err != nil {
		t.Fatal(err)
	}
	if len(proposal.Entities) != 1 || proposal.Entities[0].Type.ID != "items" {
		t.Fatal(proposal)
	}
	for i, index := range []uint64{20, 30} {
		raw, err := adapter.readRaw(index)
		if err != nil || !bytes.Equal(raw, records[i+2]) {
			t.Fatal("changed retained latest/tail", index, err)
		}
	}
	if _, err := adapter.log.Reclaim(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := adapter.log.Close(); err != nil {
		t.Fatal(err)
	}
	log, err := segmented.Open(path, segmentlog.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	adapter = &eventLogAdapter{log: log}
	result, err = adapter.rewriteMetadata(t.Context(), 2, 20, func() uint64 { return 30 })
	if err != nil || !result.Published || result.ChangedRecords != 0 {
		t.Fatal("non-idempotent metadata rewrite", result, err)
	}
	if index, err := adapter.eventIndex(); err != nil || index != 30 {
		t.Fatal(index, err)
	}
}

func TestSegmentMetadataRewriteValidationAndProtection(t *testing.T) {
	adapter, _ := newSegmentEventExperiment(t)
	if err := adapter.appendRaw([][]byte{experimentEntry(t, 10, pb.EntryNormal)}); err != nil {
		t.Fatal(err)
	}
	for _, bounds := range [][2]uint64{{11, 10}, {10, 11}} {
		if _, err := adapter.rewriteMetadata(t.Context(), 1, bounds[0], func() uint64 { return bounds[1] }); !errors.Is(err, eventlog.ErrInvalid) {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	reader, err := adapter.protectedReaderAt(ctx, 0, segmentTestResolver(segmentTestType), func() uint64 { return 10 })
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	called := false
	if _, err := adapter.rewriteMetadata(t.Context(), 1, 10, func() uint64 { called = true; return 10 }); !errors.Is(err, errEventRewriteDeferred) || called {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	canceled, stop := context.WithCancel(t.Context())
	stop()
	if _, err := adapter.rewriteMetadata(canceled, 1, 10, func() uint64 { return 10 }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	result, err := adapter.rewriteMetadata(t.Context(), 1, 10, func() uint64 { return 10 })
	if err != nil || !result.Published {
		t.Fatal("validation/defer consumed generation", result, err)
	}
}

func TestSegmentMetadataRewriteSerializesAppend(t *testing.T) {
	adapter, _ := newSegmentEventExperiment(t)
	if err := adapter.appendRaw([][]byte{experimentEntry(t, 10, pb.EntryNormal)}); err != nil {
		t.Fatal(err)
	}
	entered, proceed := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	rewritten := make(chan error, 1)
	go func() {
		_, err := adapter.rewriteMetadata(t.Context(), 1, 10, func() uint64 { calls.Add(1); close(entered); <-proceed; return 10 })
		rewritten <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(proceed)
		t.Fatal("rewrite did not start")
	}
	// The exclusive view starts before watermark capture and remains held through
	// selection/publication; appending must finish after the transaction releases it.
	unlocked := adapter.mu.TryLock()
	if unlocked {
		adapter.mu.Unlock()
	}
	appended := make(chan error, 1)
	go func() { appended <- adapter.appendRaw([][]byte{experimentEntry(t, 20, pb.EntryNormal)}) }()
	close(proceed)
	for _, done := range []chan error{rewritten, appended} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("operation stalled")
		}
	}
	if unlocked || calls.Load() != 1 {
		t.Fatal("watermark outside exclusive view")
	}
	if index, err := adapter.eventIndex(); err != nil || index != 20 {
		t.Fatal(index, err)
	}
}
