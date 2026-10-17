package db_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// stagedFakeSyncable carries node-local stage state, as a staged projection
// does: Sync folds and emits, advancing the frontier; FoldStagesOnly folds
// without emitting.
type stagedFakeSyncable struct {
	rematFakeSyncable
	frontier uint64
	folded   []string // row keys FoldStagesOnly folded
	lastSync uint64   // index of the last Actual Sync consumed
}

func (f *stagedFakeSyncable) Sync(ctx context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	snap, err := f.rematFakeSyncable.Sync(ctx, a)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.frontier, f.lastSync = a.Index, a.Index
	return snap, err
}

func (f *stagedFakeSyncable) StageFrontier() (uint64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.frontier, true, nil
}

func (f *stagedFakeSyncable) FoldStagesOnly(as []*cluster.Actual) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range as {
		for _, e := range a.Entities {
			if e.Type != nil && !cluster.IsInternal(e.ID) && e.Variant() == cluster.EntityVariantRow {
				f.folded = append(f.folded, string(e.Key))
			}
		}
		f.frontier = a.Index
	}
	return nil
}

// loseStore stands in for a node that comes back without its stage store: a
// live-backup restore (the capture holds no stage stores), a torn store
// file, or a deleted projections directory. Stage state starts from nothing
// and the destination's rows stay as they were.
func (f *stagedFakeSyncable) loseStore() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.frontier, f.folded, f.synced = 0, nil, nil
}

func (f *stagedFakeSyncable) state() (frontier uint64, folded, synced []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.frontier, append([]string(nil), f.folded...), append([]string(nil), f.synced...)
}

var _ cluster.StageRecoverer = (*stagedFakeSyncable)(nil)

// A staged syncable whose store comes back empty while its checkpoint is past
// zero re-derives its stage state from the log before consuming: every entry
// up to the checkpoint folds into the store without reaching the destination
// (it is already there), and consumption resumes after the checkpoint.
// Folding forward from the checkpoint with an empty store would emit
// aggregates missing everything before it.
func TestStageRecovery_LostStoreRefoldsToTheCheckpointWithoutEmitting(t *testing.T) {
	sink := &stagedFakeSyncable{}
	dir := t.TempDir()
	d1, s1 := openWalDBRematAt(t, dir, sink)
	proposeTypeTOML(t, d1, "photos", "photos", "", "")
	tp, err := s1.ResolveType(cluster.LatestTypeRef("photos"))
	require.NoError(t, err)
	for _, k := range []string{"k1", "k2"} {
		require.NoError(t, d1.Propose(testCtx(t),
			&cluster.Proposal{Entities: []*cluster.Entity{cluster.NewUpsertEntity(tp, []byte(k), []byte(`{"a":1}`))}}))
	}
	require.NoError(t, d1.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID: "photos-mirror", MimeType: "text/toml",
		Data: []byte("[syncable]\nname = \"photos-mirror\"\ntype = \"fake\"\n"),
	}))
	var checkpoint uint64
	require.Eventually(t, func() bool {
		_, _, synced := sink.state()
		sink.mu.Lock()
		last := sink.lastSync
		sink.mu.Unlock()
		checkpoint, _, _ = d1.SyncableProgress("photos-mirror")
		return len(synced) == 2 && checkpoint >= last
	}, 10*time.Second, 10*time.Millisecond, "never synced and checkpointed both rows")
	_, folded, _ := sink.state()
	require.Empty(t, folded, "a store in step with the checkpoint needs no recovery")
	require.NoError(t, d1.Close())
	require.NoError(t, s1.Close())

	sink.loseStore()
	d, s := openWalDBRematAt(t, dir, sink)
	t.Cleanup(func() { _ = d.Close(); _ = s.Close() })
	s.RequestSyncReconcile()
	require.NoError(t, d.Propose(testCtx(t),
		&cluster.Proposal{Entities: []*cluster.Entity{cluster.NewUpsertEntity(tp, []byte("k3"), []byte(`{"a":1}`))}}))

	require.Eventually(t, func() bool {
		_, _, synced := sink.state()
		return len(synced) > 0
	}, 10*time.Second, 10*time.Millisecond, "the restarted worker never consumed")
	frontier, folded, synced := sink.state()
	require.Equal(t, []string{"k1", "k2"}, folded, "everything up to the checkpoint re-folds into the store")
	require.Equal(t, []string{"k3"}, synced, "only entries after the checkpoint reach the destination")
	require.Greater(t, frontier, checkpoint, "the store then advances with consumption")
}
