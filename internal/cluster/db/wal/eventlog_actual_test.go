package wal

import (
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	tidwallbackend "github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"

	tidwal "github.com/tidwall/wal"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
)

func TestEventLogActualMatchesLegacy(t *testing.T) {
	adapter, _ := newSegmentedEventAdapter(t)
	legacy, err := tidwal.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = legacy.Close() })
	storage := &Storage{eventLog: bindLegacyEventLog(tidwallbackend.OwnLegacy(legacy), nil)}
	typ, _ := eventTestType(cluster.TypeRef{ID: "items", Version: 1})
	storage.typeCache.Store(cluster.TypeRef{ID: "items", Version: 1}, typeCacheEntry{t: typ})
	storage.appliedIndex.Store(100)
	meta, err := cluster.NewUpsertSyncableIndexEntity(&cluster.SyncableIndex{ID: "worker", Index: 5})
	if err != nil {
		t.Fatal(err)
	}
	internal := experimentRow("worker", "metadata")
	internal.Type.ID = meta.Type.ID
	inputs := [][]byte{
		experimentEntry(t, 1, pb.EntryNormal), experimentEntry(t, 3, pb.EntryConfChange),
		experimentEntry(t, 10, pb.EntryNormal, internal),
		experimentEntry(t, 20, pb.EntryNormal, internal, experimentRow("user", "data")),
		experimentEntry(t, 30, pb.EntryNormal, experimentRow("next", "value")),
	}
	if err := adapter.appendRaw(inputs); err != nil {
		t.Fatal(err)
	}
	for i, raw := range inputs {
		if err := legacy.Write(uint64(i+1), frame(raw)); err != nil {
			t.Fatal(err)
		}
	}
	reader, err := adapter.readerAt(0, storage, storage.AppliedIndex)
	if err != nil {
		t.Fatal(err)
	}
	first, err := reader.Read()
	if err != nil || first.Index != 20 || len(first.Entities) != 1 {
		t.Fatal(first, err)
	}
	for _, index := range []uint64{0, 1, 2, 3, 10, 11, 20, 30, 31, ^uint64(0)} {
		got, gerr := adapter.actualAt(index, storage, storage.AppliedIndex)
		want, werr := storage.ActualAt(index)
		if !reflect.DeepEqual(got, want) || !errors.Is(gerr, werr) {
			t.Fatal(index, got, gerr, want, werr)
		}
	}
	if reader.Position() != 20 {
		t.Fatal("lookup moved stream cursor")
	}
	next, err := reader.Read()
	if err != nil || next.Index != 30 {
		t.Fatal(next, err)
	}
	// Erasing an exact match must not return its next surviving neighbor.
	_, err = adapter.rewriteRaw(t.Context(), 1, func(raw []byte) (bool, []byte, error) {
		entry := new(pb.Entry)
		if err := proto.Unmarshal(raw, entry); err != nil {
			return false, nil, err
		}
		return entry.GetIndex() != 20, raw, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.actualAt(20, storage, storage.AppliedIndex); !errors.Is(err, ErrActualNotFound) {
		t.Fatal(err)
	}
	if actual, err := adapter.actualAt(30, storage, storage.AppliedIndex); err != nil || actual.Index != 30 {
		t.Fatal(actual, err)
	}
}

func TestEventLogActualVisibilityAndErrors(t *testing.T) {
	adapter, _ := newSegmentedEventAdapter(t)
	if err := adapter.appendRaw([][]byte{experimentEntry(t, 10, pb.EntryNormal, experimentRow("key", "data"))}); err != nil {
		t.Fatal(err)
	}
	var applied atomic.Uint64
	calls := 0
	missing := errors.New("missing type")
	fail := true
	resolver := eventTestResolver(func(ref cluster.TypeRef) (*cluster.Type, error) {
		calls++
		if fail {
			return nil, missing
		}
		return eventTestType(ref)
	})
	if _, err := adapter.actualAt(10, resolver, applied.Load); !errors.Is(err, ErrActualNotFound) || calls != 0 {
		t.Fatal("resolved unapplied type", err, calls)
	}
	applied.Store(10)
	for range 2 {
		if _, err := adapter.actualAt(10, resolver, applied.Load); !errors.Is(err, missing) {
			t.Fatal(err)
		}
	}
	fail = false
	if actual, err := adapter.actualAt(10, resolver, applied.Load); err != nil || actual.Index != 10 {
		t.Fatal(actual, err)
	}
	if _, err := adapter.actualAt(10, nil, applied.Load); !errors.Is(err, eventlog.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := adapter.actualAt(10, resolver, nil); !errors.Is(err, eventlog.ErrInvalid) {
		t.Fatal(err)
	}
	if err := adapter.log.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.actualAt(10, resolver, applied.Load); !errors.Is(err, eventlog.ErrClosed) {
		t.Fatal(err)
	}
}

func TestEventLogActualCorruptionAndUnknownTypes(t *testing.T) {
	for _, payload := range [][]byte{{0xff}, experimentEntry(t, 11, pb.EntryNormal)} {
		adapter, _ := newSegmentedEventAdapter(t)
		if err := adapter.log.Append([]eventlog.Record{{ID: 10, Payload: payload}}); err != nil {
			t.Fatal(err)
		}
		if _, err := adapter.actualAt(10, eventTestResolver(eventTestType), func() uint64 { return 100 }); !errors.Is(err, ErrCorruptEntry) {
			t.Fatal(err)
		}
	}
	// Exact lookup returns unknown-type errors even for types a stream may skip.
	for _, id := range []string{"c01177ed-0000-0000-0000-000000001fff", "c01177ed-0000-0000-0000-000000000fff"} {
		adapter, _ := newSegmentedEventAdapter(t)
		unknown := experimentRow("key", "data")
		unknown.Type.ID = id
		if err := adapter.appendRaw([][]byte{experimentEntry(t, 10, pb.EntryNormal, unknown)}); err != nil {
			t.Fatal(err)
		}
		_, err := adapter.actualAt(10, eventTestResolver(eventTestType), func() uint64 { return 100 })
		var typed *cluster.UnknownReservedTypeError
		if !errors.As(err, &typed) {
			t.Fatal(err)
		}
	}
}
