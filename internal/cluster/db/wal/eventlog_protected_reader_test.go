package wal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"

	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
)

func TestEventLogProtectedReaderDefersRewriteAllowsAppend(t *testing.T) {
	adapter, _ := newSegmentedEventAdapter(t)
	if err := adapter.appendRaw([][]byte{experimentEntry(t, 10, pb.EntryNormal, experimentRow("old", "value"))}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	first, err := adapter.protectedReaderAt(ctx, 0, eventTestResolver(eventTestType), func() uint64 { return 100 })
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := adapter.protectedReaderAt(ctx, 0, eventTestResolver(eventTestType), func() uint64 { return 100 })
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if adapter.protectedReadCount() != 2 {
		t.Fatal("missing protections")
	}
	actual, err := first.Read()
	if err != nil || actual.Index != 10 {
		t.Fatal(actual, err)
	}
	called := false
	erase := func([]byte) (bool, []byte, error) { called = true; return false, nil, nil }
	if _, err := adapter.rewriteRaw(t.Context(), 1, erase); !errors.Is(err, errEventRewriteDeferred) || called {
		t.Fatal("prepared protected rewrite", err)
	}
	if err := adapter.appendRaw([][]byte{experimentEntry(t, 30, pb.EntryNormal, experimentRow("next", "value"))}); err != nil {
		t.Fatal(err)
	}
	actual, err = first.Read()
	if err != nil || actual.Index != 30 {
		t.Fatal(actual, err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if adapter.protectedReadCount() != 1 {
		t.Fatal("double release")
	}
	if _, err := first.Read(); !errors.Is(err, eventlog.ErrClosed) {
		t.Fatal(err)
	}
	if _, err := adapter.rewriteRaw(t.Context(), 1, erase); !errors.Is(err, errEventRewriteDeferred) {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	result, err := adapter.rewriteRaw(t.Context(), 1, erase)
	if err != nil || !result.Published || !called {
		t.Fatal(result, err)
	}
}

func TestEventLogProtectedReaderCancellation(t *testing.T) {
	adapter, _ := newSegmentedEventAdapter(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	r, err := adapter.protectedReaderAt(ctx, 0, eventTestResolver(eventTestType), func() uint64 { return 100 })
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	if _, err := r.Read(); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	// Concurrent Close and the cancellation callback must release only once.
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { _ = r.Close() })
	}
	wg.Wait()
	if adapter.protectedReadCount() != 0 {
		t.Fatal("leaked protection")
	}
	// No deadline is an invalid unbounded hold; an expired deadline cannot acquire.
	if _, err := adapter.protectedReaderAt(context.Background(), 0, eventTestResolver(eventTestType), func() uint64 { return 100 }); !errors.Is(err, eventlog.ErrInvalid) {
		t.Fatal(err)
	}
	expired, stop := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer stop()
	if _, err := adapter.protectedReaderAt(expired, 0, eventTestResolver(eventTestType), func() uint64 { return 100 }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}

func TestEventLogProtectedReaderExpiryReleasesWithoutClose(t *testing.T) {
	adapter, _ := newSegmentedEventAdapter(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	r, err := adapter.protectedReaderAt(ctx, 0, eventTestResolver(eventTestType), func() uint64 { return 100 })
	if err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	// The parent closes Done before propagating cancellation to its children.
	// Wait for automatic release to prove the reader's lifetime has expired
	// before checking Read; otherwise an empty reader can still return EOF.
	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for adapter.protectedReadCount() != 0 {
		select {
		case <-tick.C:
		case <-deadline:
			t.Fatal("expiry did not release")
		}
	}
	if _, err := r.Read(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if _, err := adapter.rewriteRaw(t.Context(), 1, func(raw []byte) (bool, []byte, error) { return true, raw, nil }); err != nil {
		t.Fatal(err)
	}
}

func TestEventLogProtectedReaderCancellationDuringDecode(t *testing.T) {
	adapter, _ := newSegmentedEventAdapter(t)
	if err := adapter.appendRaw([][]byte{experimentEntry(t, 10, pb.EntryNormal, experimentRow("key", "value"))}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	entered, proceed := make(chan struct{}), make(chan struct{})
	resolver := eventTestResolver(func(ref cluster.TypeRef) (*cluster.Type, error) {
		close(entered)
		<-proceed
		return eventTestType(ref)
	})
	r, err := adapter.protectedReaderAt(ctx, 0, resolver, func() uint64 { return 100 })
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := r.Read(); done <- err }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(proceed)
		t.Fatal("read did not start")
	}
	cancel()
	if adapter.protectedReadCount() != 1 {
		t.Fatal("in-flight read lost protection")
	}
	close(proceed)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("read did not finish")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if r.Position() != 0 || adapter.protectedReadCount() != 0 {
		t.Fatal("canceled read advanced or leaked")
	}
}
