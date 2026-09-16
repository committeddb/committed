package wal

import (
	"errors"
	"io"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"

	tidwal "github.com/tidwall/wal"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
)

type segmentTestResolver func(cluster.TypeRef) (*cluster.Type, error)

func (f segmentTestResolver) ResolveType(ref cluster.TypeRef) (*cluster.Type, error) { return f(ref) }

func segmentTestType(ref cluster.TypeRef) (*cluster.Type, error) {
	return &cluster.Type{ID: ref.ID, Version: ref.Version, Name: ref.ID}, nil
}

func TestSegmentReaderMatchesLegacy(t *testing.T) {
	adapter, _ := newSegmentEventExperiment(t)
	legacy, err := tidwal.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = legacy.Close() })
	// A minimal legacy reader fixture: explicit-version resolution uses the real
	// Storage cache; no background workers or BoltDB mutations are needed here.
	storage := &Storage{eventLog: legacy}
	typ, _ := segmentTestType(cluster.TypeRef{ID: "items", Version: 1})
	storage.typeCache.Store(cluster.TypeRef{ID: "items", Version: 1}, typeCacheEntry{t: typ})
	metadata, err := cluster.NewUpsertSyncableIndexEntity(&cluster.SyncableIndex{ID: "worker", Index: 5})
	if err != nil {
		t.Fatal(err)
	}
	internal := experimentRow("worker", "metadata")
	internal.Type.ID = metadata.Type.ID
	inputs := [][]byte{
		experimentEntry(t, 1, pb.EntryNormal),
		experimentEntry(t, 3, pb.EntryConfChange),
		experimentEntry(t, 10, pb.EntryNormal, internal),
		experimentEntry(t, 20, pb.EntryNormal, internal, experimentRow("first", "one")),
		experimentEntry(t, 30, pb.EntryNormal, experimentRow("second", "two"), internal),
		experimentEntry(t, 40, pb.EntryNormal, internal),
	}
	if err := adapter.appendRaw(inputs); err != nil {
		t.Fatal(err)
	}
	for i, raw := range inputs {
		if err := legacy.Write(uint64(i+1), frame(raw)); err != nil {
			t.Fatal(err)
		}
	}
	for _, checkpoint := range []uint64{0, 10, 15, 20, 39, 40, 100} {
		storage.appliedIndex.Store(100)
		want := &Reader{s: storage, raftIndex: checkpoint}
		got, err := adapter.readerAt(checkpoint, storage, storage.AppliedIndex)
		if err != nil {
			t.Fatal(err)
		}
		for {
			actual, werr := want.Read()
			result, gerr := got.Read()
			if !reflect.DeepEqual(actual, result) || !errors.Is(gerr, werr) || want.Position() != got.Position() {
				t.Fatalf("checkpoint %d: got %v/%v at %d, want %v/%v at %d", checkpoint, result, gerr, got.Position(), actual, werr, want.Position())
			}
			if errors.Is(werr, io.EOF) {
				break
			}
		}
	}
	// The real legacy reader and the experiment both pause before unapplied data,
	// then deliver it once the watermark advances.
	storage.appliedIndex.Store(10)
	want := &Reader{s: storage}
	got, err := adapter.readerAt(0, storage, storage.AppliedIndex)
	if err != nil {
		t.Fatal(err)
	}
	for _, watermark := range []uint64{10, 20, 30, 40} {
		storage.appliedIndex.Store(watermark)
		actual, werr := want.Read()
		result, gerr := got.Read()
		if !reflect.DeepEqual(actual, result) || !errors.Is(gerr, werr) || want.Position() != got.Position() {
			t.Fatal(watermark, result, gerr, actual, werr)
		}
	}
}

func TestSegmentReaderVisibilityAndRetry(t *testing.T) {
	adapter, _ := newSegmentEventExperiment(t)
	if err := adapter.appendRaw([][]byte{experimentEntry(t, 10, pb.EntryNormal, experimentRow("key", "value"))}); err != nil {
		t.Fatal(err)
	}
	var applied atomic.Uint64
	calls := 0
	missing := errors.New("type not available")
	fail := true
	resolver := segmentTestResolver(func(ref cluster.TypeRef) (*cluster.Type, error) {
		calls++
		if fail {
			return nil, missing
		}
		return segmentTestType(ref)
	})
	r, err := adapter.readerAt(0, resolver, applied.Load)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Read(); !errors.Is(err, io.EOF) || calls != 0 || r.Position() != 0 {
		t.Fatal("resolved unapplied type", err, calls)
	}
	applied.Store(10)
	for range 2 {
		if _, err := r.Read(); !errors.Is(err, missing) || r.Position() != 0 {
			t.Fatal("advanced past decode failure", err)
		}
	}
	fail = false
	actual, err := r.Read()
	if err != nil || actual.Index != 10 || r.Position() != 10 {
		t.Fatal(actual, err)
	}
	if _, err := r.Read(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	// EOF is temporary: append and apply another record to the same reader.
	if err := adapter.appendRaw([][]byte{experimentEntry(t, 50, pb.EntryNormal, experimentRow("next", "new"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Read(); !errors.Is(err, io.EOF) || r.Position() != 10 {
		t.Fatal(err)
	}
	applied.Store(50)
	actual, err = r.Read()
	if err != nil || actual.Index != 50 {
		t.Fatal(actual, err)
	}
}

func TestSegmentReaderResumeAfterRewrite(t *testing.T) {
	adapter, _ := newSegmentEventExperiment(t)
	if err := adapter.appendRaw([][]byte{experimentEntry(t, 10, pb.EntryNormal, experimentRow("first", "old")), experimentEntry(t, 30, pb.EntryNormal, experimentRow("second", "old")), experimentEntry(t, 90, pb.EntryNormal, experimentRow("last", "keep"))}); err != nil {
		t.Fatal(err)
	}
	r, err := adapter.readerAt(0, segmentTestResolver(segmentTestType), func() uint64 { return 100 })
	if err != nil {
		t.Fatal(err)
	}
	if actual, err := r.Read(); err != nil || actual.Index != 10 {
		t.Fatal(actual, err)
	}
	_, err = adapter.rewriteRaw(t.Context(), 1, func(raw []byte) (bool, []byte, error) {
		e := new(pb.Entry)
		if err := proto.Unmarshal(raw, e); err != nil {
			return false, nil, err
		}
		return e.GetIndex() == 90, raw, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.log.Reclaim(t.Context()); err != nil {
		t.Fatal(err)
	}
	if actual, err := r.Read(); err != nil || actual.Index != 90 {
		t.Fatal("stale cursor after erasure", actual, err)
	}
	if _, err := r.Read(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	maxReader, err := adapter.readerAt(^uint64(0), segmentTestResolver(segmentTestType), func() uint64 { return ^uint64(0) })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := maxReader.Read(); !errors.Is(err, io.EOF) {
		t.Fatal("checkpoint overflow", err)
	}
}

func TestSegmentReaderCorruptionDoesNotAdvance(t *testing.T) {
	for _, raw := range [][]byte{{0xff}, experimentEntry(t, 11, pb.EntryNormal)} {
		adapter, _ := newSegmentEventExperiment(t)
		if err := adapter.log.Append([]eventlog.Record{{ID: 10, Payload: raw}}); err != nil {
			t.Fatal(err)
		}
		r, err := adapter.readerAt(0, segmentTestResolver(segmentTestType), func() uint64 { return 100 })
		if err != nil {
			t.Fatal(err)
		}
		for range 2 {
			if _, err := r.Read(); !errors.Is(err, ErrCorruptEntry) || r.Position() != 0 {
				t.Fatal(err)
			}
		}
	}
}

func TestSegmentReaderProtectsDecodeFromRewrite(t *testing.T) {
	adapter, _ := newSegmentEventExperiment(t)
	if err := adapter.appendRaw([][]byte{experimentEntry(t, 10, pb.EntryNormal, experimentRow("key", "old"))}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	resolver := segmentTestResolver(func(ref cluster.TypeRef) (*cluster.Type, error) {
		close(entered)
		<-release
		return segmentTestType(ref)
	})
	r, err := adapter.readerAt(0, resolver, func() uint64 { return 100 })
	if err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() {
		actual, err := r.Read()
		if err == nil && (actual.Index != 10 || string(actual.Entities[0].Data) != "old") {
			err = errors.New("mixed read")
		}
		readDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("reader did not enter resolver")
	}
	// The lock must span decoding, not only the raw byte lookup.
	unlocked := adapter.mu.TryLock()
	if unlocked {
		adapter.mu.Unlock()
	}
	rewriteDone := make(chan error, 1)
	go func() {
		_, err := adapter.rewriteRaw(t.Context(), 1, func([]byte) (bool, []byte, error) { return false, nil, nil })
		rewriteDone <- err
	}()
	close(release)
	for _, done := range []chan error{readDone, rewriteDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("operation stalled")
		}
	}
	if unlocked {
		t.Fatal("rewrite could publish during decode")
	}
	if r.Position() != 10 {
		t.Fatal(r.Position())
	}
}

func TestSegmentReaderUnknownSystemTypes(t *testing.T) {
	for _, id := range []string{"c01177ed-0000-0000-0000-000000001fff", "c01177ed-0000-0000-0000-000000000fff"} {
		adapter, _ := newSegmentEventExperiment(t)
		unknown := experimentRow("system", "opaque")
		unknown.Type.ID = id
		if err := adapter.appendRaw([][]byte{experimentEntry(t, 10, pb.EntryNormal, unknown), experimentEntry(t, 20, pb.EntryNormal, experimentRow("user", "data"))}); err != nil {
			t.Fatal(err)
		}
		r, err := adapter.readerAt(0, segmentTestResolver(segmentTestType), func() uint64 { return 100 })
		if err != nil {
			t.Fatal(err)
		}
		actual, err := r.Read()
		if id[32] == '1' {
			if err != nil || actual.Index != 20 || r.Position() != 20 {
				t.Fatal(actual, err)
			}
		} else {
			var unknown *cluster.UnknownReservedTypeError
			if !errors.As(err, &unknown) || unknown.Skippable() || r.Position() != 0 {
				t.Fatal(err)
			}
			if _, err := r.Read(); !errors.As(err, &unknown) || r.Position() != 0 {
				t.Fatal("skipped must-understand record", err)
			}
		}
	}
}
