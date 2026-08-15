package db_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// rematFakeSyncable is a keyed in-memory sink implementing Rematerializable:
// it records synced keys, begun epochs, and completion sweeps.
type rematFakeSyncable struct {
	mu        sync.Mutex
	keyed     bool
	synced    []string
	beganWith []uint64
	completed int
}

func (f *rematFakeSyncable) Sync(_ context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range a.Entities {
		if e.Type != nil && !cluster.IsInternal(e.ID) && e.Variant() == cluster.EntityVariantRow {
			f.synced = append(f.synced, string(e.Key))
		}
	}
	return true, nil
}
func (f *rematFakeSyncable) Close() error           { return nil }
func (f *rematFakeSyncable) CanRematerialize() bool { return f.keyed }

func (f *rematFakeSyncable) BeginRematerialization(_ context.Context, epoch uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.beganWith = append(f.beganWith, epoch)
	return nil
}

func (f *rematFakeSyncable) CompleteRematerialization(_ context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completed++
	return nil
}

func (f *rematFakeSyncable) snapshot() (synced []string, began []uint64, completed int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.synced...), append([]uint64(nil), f.beganWith...), f.completed
}

// newWalDBRemat wires a fixture whose "fake" syncable kind builds the given
// sink, with real pump channels so workers run.
func newWalDBRemat(t *testing.T, sink cluster.Syncable) (*db.DB, *wal.Storage) {
	t.Helper()
	dir := t.TempDir()
	p := parser.New()
	fakeParser := &clusterfakes.FakeSyncableParser{}
	fakeParser.ParseReturns(sink, nil)
	p.AddSyncableParser("fake", fakeParser)
	syncCh := make(chan *db.SyncableWithID, 32)
	ingestCh := make(chan *db.IngestableWithID, 32)
	s, err := wal.Open(dir, p, syncCh, ingestCh, wal.WithoutFsync())
	require.NoError(t, err)
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, syncCh, ingestCh, db.WithTickInterval(testTickInterval))
	t.Cleanup(func() { _ = d.Close(); _ = s.Close() })
	return d, s
}

// TestRematerialize_FullLifecycle drives the verb through the real worker: a
// synced topic re-materializes — the worker replays every row from index 0,
// begins epoch marking at the recorded target head, sweeps on completion,
// and clears the in-progress record. The sink keeps its identity throughout
// (no teardown — the non-destructive contract).
func TestRematerialize_FullLifecycle(t *testing.T) {
	sink := &rematFakeSyncable{keyed: true}
	d, s := newWalDBRemat(t, sink)

	proposeTypeTOML(t, d, "photos", "photos", "", "")
	tp, err := s.ResolveType(cluster.LatestTypeRef("photos"))
	require.NoError(t, err)
	for _, k := range []string{"k1", "k2", "k3"} {
		require.NoError(t, d.Propose(testCtx(t),
			&cluster.Proposal{Entities: []*cluster.Entity{cluster.NewUpsertEntity(tp, []byte(k), []byte(`{"a":1}`))}}))
	}

	require.NoError(t, d.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID: "photos-mirror", MimeType: "text/toml",
		Data: []byte("[syncable]\nname = \"photos-mirror\"\ntype = \"fake\"\n"),
	}))
	require.Eventually(t, func() bool {
		synced, _, _ := sink.snapshot()
		return len(synced) >= 3
	}, 10*time.Second, 10*time.Millisecond, "initial sync never completed")

	require.NoError(t, d.RematerializeSyncable(testCtx(t), "photos-mirror"))

	require.Eventually(t, func() bool {
		synced, began, completed := sink.snapshot()
		return len(began) >= 1 && completed >= 1 && len(synced) >= 6
	}, 15*time.Second, 10*time.Millisecond, "the replay never completed: %v", sink.snapshot)

	synced, began, completed := sink.snapshot()
	require.GreaterOrEqual(t, len(synced), 6, "every row replayed from index 0")
	require.Equal(t, []string{"k1", "k2", "k3"}, synced[len(synced)-3:], "the replay re-emitted the rows in log order")
	require.NotZero(t, began[0], "the epoch is the recorded target head")
	require.GreaterOrEqual(t, completed, 1, "the completion sweep ran")

	// The in-progress record cleared, and the sink was NEVER torn down (the
	// non-destructive contract: same instance, still serving).
	require.Eventually(t, func() bool {
		_, ok := s.SyncableRematerialization("photos-mirror")
		return !ok
	}, 10*time.Second, 10*time.Millisecond, "the in-progress record never cleared")

	// A new row after completion syncs normally.
	require.NoError(t, d.Propose(testCtx(t),
		&cluster.Proposal{Entities: []*cluster.Entity{cluster.NewUpsertEntity(tp, []byte("k4"), []byte(`{"a":2}`))}}))
	require.Eventually(t, func() bool {
		synced, _, _ := sink.snapshot()
		return synced[len(synced)-1] == "k4"
	}, 10*time.Second, 10*time.Millisecond)
}

// TestRematerialize_RefusesNonConvergingSinks pins the admission rule: a sink
// that cannot converge a replay in place (keyless, webhook) refuses with the
// typed error, before anything changes.
func TestRematerialize_RefusesNonConvergingSinks(t *testing.T) {
	sink := &rematFakeSyncable{keyed: false}
	d, s := newWalDBRemat(t, sink)

	proposeTypeTOML(t, d, "photos", "photos", "", "")
	require.NoError(t, d.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID: "photos-log", MimeType: "text/toml",
		Data: []byte("[syncable]\nname = \"photos-log\"\ntype = \"fake\"\n"),
	}))

	err := d.RematerializeSyncable(testCtx(t), "photos-log")
	require.ErrorIs(t, err, cluster.ErrNotRematerializable)

	_, ok := s.SyncableRematerialization("photos-log")
	require.False(t, ok, "a refused verb leaves no in-progress record")

	// An unknown id is a clean not-found.
	require.ErrorIs(t, d.RematerializeSyncable(testCtx(t), "nope"), cluster.ErrResourceNotFound)
}
