package wal_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// TestRestoreSnapshot_RespawnsSyncableWorker is the snapshot-worker regression: a
// syncable whose creating raft entry was compacted out of the log is learned by a
// lagging follower ONLY via InstallSnapshot. RestoreSnapshot swaps in the bbolt
// (config present) but must ALSO re-drive the config to the worker channel — the
// apply path that normally sends it is skipped for the compacted entry — or the
// node ends up with the config on disk and no worker, silently doing nothing if
// it is later elected leader without a process restart.
func TestRestoreSnapshot_RespawnsSyncableWorker(t *testing.T) {
	p := parser.New()
	sp := &clusterfakes.FakeSyncableParser{}
	sp.ParseReturns(&clusterfakes.FakeSyncable{}, nil)
	p.AddSyncableParser("test", sp)
	opts := []wal.Option{wal.WithoutFsync()}

	syncEnt, err := cluster.NewUpsertSyncableEntity(&cluster.Configuration{
		ID:       "sync-1",
		MimeType: "application/json",
		Data:     []byte(`{"syncable": {"name": "sync-1", "type": "test"}}`),
	})
	require.NoError(t, err)

	// Source: apply the syncable so its config lands in bbolt, then snapshot.
	srcCh := make(chan *db.SyncableWithID, 4)
	src, err := wal.Open(t.TempDir(), p, srcCh, nil, opts...)
	require.NoError(t, err)
	saveEntity(t, syncEnt, src, 1, 1)
	<-srcCh // drain the source apply-path send
	snap, err := src.CreateSnapshot(src.AppliedIndex(), &pb.ConfState{})
	require.NoError(t, err)
	require.NoError(t, src.Close())

	// Destination: a fresh node that never applied the syncable. Advance its event
	// log with an unrelated Type so RestoreSnapshot's invariant (snap.Index <=
	// EventIndex) holds WITHOUT seeding the syncable via the apply path — the
	// syncable must be learned only through the snapshot.
	dstCh := make(chan *db.SyncableWithID, 4)
	dst, err := wal.Open(t.TempDir(), p, dstCh, nil, opts...)
	require.NoError(t, err)
	defer dst.Close()
	typeEnt, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "t", Name: "t", Version: 1})
	require.NoError(t, err)
	saveEntity(t, typeEnt, dst, 1, 1)

	// Precondition: the syncable is unknown to dst before the restore (no worker send).
	select {
	case got := <-dstCh:
		t.Fatalf("unexpected pre-restore worker send for %q", got.ID)
	case <-time.After(200 * time.Millisecond):
	}

	require.NoError(t, dst.RestoreSnapshot(snap))

	// The restore must request a reconcile whose closure lists the
	// snapshot-learned syncable (the db listener executes it at dequeue).
	select {
	case got := <-dstCh:
		require.NotNil(t, got.ReconcileList, "restore must send a reconcile request")
		listed, err := got.ReconcileList()
		require.NoError(t, err)
		require.Len(t, listed, 1)
		require.Equal(t, "sync-1", listed[0].ID, "the reconcile must list the snapshot-learned syncable")
	case <-time.After(3 * time.Second):
		t.Fatal("RestoreSnapshot did not request a sync reconcile")
	}
}

// TestRestoreSnapshot_RespawnsIngestableWorker is the ingestable twin of
// TestRestoreSnapshot_RespawnsSyncableWorker: an ingestable learned only via
// InstallSnapshot must likewise get a worker after the restore.
func TestRestoreSnapshot_RespawnsIngestableWorker(t *testing.T) {
	p := parser.New()
	ip := &clusterfakes.FakeIngestableParser{}
	ip.ParseReturns(&clusterfakes.FakeIngestable{}, nil)
	p.AddIngestableParser("test", ip)
	opts := []wal.Option{wal.WithoutFsync()}

	ingEnt, err := cluster.NewUpsertIngestableEntity(&cluster.Configuration{
		ID:       "ingest-1",
		MimeType: "application/json",
		Data:     []byte(`{"ingestable": {"name": "ingest-1", "type": "test"}}`),
	})
	require.NoError(t, err)

	srcCh := make(chan *db.IngestableWithID, 4)
	src, err := wal.Open(t.TempDir(), p, nil, srcCh, opts...)
	require.NoError(t, err)
	saveEntity(t, ingEnt, src, 1, 1)
	<-srcCh
	snap, err := src.CreateSnapshot(src.AppliedIndex(), &pb.ConfState{})
	require.NoError(t, err)
	require.NoError(t, src.Close())

	dstCh := make(chan *db.IngestableWithID, 4)
	dst, err := wal.Open(t.TempDir(), p, nil, dstCh, opts...)
	require.NoError(t, err)
	defer dst.Close()
	typeEnt, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "t", Name: "t", Version: 1})
	require.NoError(t, err)
	saveEntity(t, typeEnt, dst, 1, 1)

	select {
	case got := <-dstCh:
		t.Fatalf("unexpected pre-restore worker send for %q", got.ID)
	case <-time.After(200 * time.Millisecond):
	}

	require.NoError(t, dst.RestoreSnapshot(snap))

	select {
	case got := <-dstCh:
		require.NotNil(t, got.ReconcileList, "restore must send a reconcile request")
		listed, err := got.ReconcileList()
		require.NoError(t, err)
		require.Len(t, listed, 1)
		require.Equal(t, "ingest-1", listed[0].ID, "the reconcile must list the snapshot-learned ingestable")
	case <-time.After(3 * time.Second):
		t.Fatal("RestoreSnapshot did not request an ingest reconcile")
	}
}
