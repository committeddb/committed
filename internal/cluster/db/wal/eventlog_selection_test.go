package wal

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"

	tidwal "github.com/tidwall/wal"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
)

func selectionEntry(t *testing.T, index uint64, entities ...*cluster.Entity) []byte {
	t.Helper()
	data, err := (&cluster.Proposal{Entities: entities}).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := proto.Marshal(&pb.Entry{Index: proto.Uint64(index), Term: proto.Uint64(1), Type: pb.EntryNormal.Enum(), Data: data})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestEventLogSelectionsMatchLegacyPrefix(t *testing.T) {
	adapter, _ := newSegmentedEventAdapter(t)
	legacy, err := tidwal.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = legacy.Close() })
	storage := &Storage{eventLog: legacy}
	snapshot := &cluster.Type{ID: "snapshot", Name: "snapshot", Version: 1, EntityKind: cluster.EntityKindSnapshot}
	revision := &cluster.Type{ID: "revision", Name: "revision", Version: 1, EntityKind: cluster.EntityKindRevision}
	snapshotReg, err := cluster.NewUpsertTypeEntity(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	revisionReg, err := cluster.NewUpsertTypeEntity(revision)
	if err != nil {
		t.Fatal(err)
	}
	meta1, err := cluster.NewUpsertSyncableIndexEntity(&cluster.SyncableIndex{ID: "worker", Index: 1})
	if err != nil {
		t.Fatal(err)
	}
	meta2, err := cluster.NewUpsertSyncableIndexEntity(&cluster.SyncableIndex{ID: "worker", Index: 2})
	if err != nil {
		t.Fatal(err)
	}
	changed := *snapshot
	changed.Version = 2
	changed.EntityKind = cluster.EntityKindEvent
	changedReg, err := cluster.NewUpsertTypeEntity(&changed)
	if err != nil {
		t.Fatal(err)
	}
	records := [][]byte{
		selectionEntry(t, 1, snapshotReg), selectionEntry(t, 2, revisionReg),
		selectionEntry(t, 10, cluster.NewUpsertEntity(snapshot, []byte("key"), []byte("old")), cluster.NewUpsertEntity(revision, []byte("key"), []byte("old"))),
		selectionEntry(t, 20, cluster.NewUpsertEntity(snapshot, []byte("key"), []byte("new")), cluster.NewUpsertEntity(revision, []byte("key"), []byte("new"))),
		selectionEntry(t, 30, meta1), selectionEntry(t, 40, meta2), selectionEntry(t, 50, cluster.NewDeleteEntity(snapshot, []byte("key"))),
		selectionEntry(t, 60, changedReg), selectionEntry(t, 70, cluster.NewUpsertEntity(&changed, []byte("key"), []byte("event"))),
	}
	if err := adapter.appendRaw(records); err != nil {
		t.Fatal(err)
	}
	for i, raw := range records {
		if err := legacy.Write(uint64(i+1), frame(raw)); err != nil {
			t.Fatal(err)
		}
	}
	snapshotKey := string(tombstoneKey(snapshot.ID, []byte("key")))
	metadataKey := string(tombstoneKey(meta1.Type.ID, []byte("worker")))
	for _, tt := range []struct {
		bound uint64
		want  map[string]uint64
	}{
		{0, map[string]uint64{}},
		{15, map[string]uint64{snapshotKey: 10}},
		{35, map[string]uint64{snapshotKey: 20, metadataKey: 30}},
		{55, map[string]uint64{snapshotKey: 50, metadataKey: 40}},
		{70, map[string]uint64{snapshotKey: 50, metadataKey: 40}},
		{^uint64(0), map[string]uint64{snapshotKey: 50, metadataKey: 40}},
	} {
		got, err := adapter.metadataSupersessions(t.Context(), tt.bound)
		if err != nil {
			t.Fatal(err)
		}
		old, err := storage.metadataSupersessions(tt.bound)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, tt.want) || !reflect.DeepEqual(got, old) {
			t.Fatal(tt.bound, got, old, tt.want)
		}
	}
	selection, err := adapter.metadataSupersessions(t.Context(), 55)
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.rewriteRaw(t.Context(), 1, func(raw []byte) (bool, []byte, error) { return scrubFilterEntry(raw, nil, selection, 0) })
	if err != nil || !result.Published {
		t.Fatal(result, err)
	}
	if _, err := adapter.readRaw(30); !errors.Is(err, eventlog.ErrNotFound) {
		t.Fatal("superseded internal entry retained", err)
	}
	for _, index := range []uint64{10, 20, 40, 50, 60, 70} {
		if _, err := adapter.readRaw(index); err != nil {
			t.Fatal("lost retained record", index, err)
		}
	}
	// A repeat selection after compaction is stable, despite missing superseded entries.
	again, err := adapter.metadataSupersessions(t.Context(), 55)
	if err != nil || !reflect.DeepEqual(again, selection) {
		t.Fatal(again, selection, err)
	}
}

func TestEventLogSelectionsDiscardPartialResults(t *testing.T) {
	adapter, _ := newSegmentedEventAdapter(t)
	meta, err := cluster.NewUpsertSyncableIndexEntity(&cluster.SyncableIndex{ID: "worker", Index: 1})
	if err != nil {
		t.Fatal(err)
	}
	registration, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "bad", Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	bad := cluster.NewUpsertEntity(registration.Type, registration.Key, []byte("invalid type JSON"))
	if err := adapter.appendRaw([][]byte{selectionEntry(t, 10, meta), selectionEntry(t, 100, bad)}); err != nil {
		t.Fatal(err)
	}
	if got, err := adapter.metadataSupersessions(t.Context(), 10); err != nil || len(got) != 1 {
		t.Fatal(got, err)
	}
	if got, err := adapter.metadataSupersessions(t.Context(), 100); err == nil || got != nil {
		t.Fatal("returned partial selection", got, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := adapter.metadataSupersessions(ctx, 10); !errors.Is(err, context.Canceled) || got != nil {
		t.Fatal(got, err)
	}
	// Selection errors do not poison the underlying log.
	if _, err := adapter.eventIndex(); err != nil {
		t.Fatal(err)
	}
}
