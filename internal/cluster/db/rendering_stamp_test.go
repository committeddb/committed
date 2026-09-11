package db_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// stampedFakeSyncable is a sink that holds derived state in its destination:
// it carries a stamp (the version it was last converged under) and can, when
// keyed, converge in place by rematerialization.
type stampedFakeSyncable struct {
	rematFakeSyncable
	renders uint64 // the version "this binary" renders
	stamp   uint64 // the destination's stamp (0 = never stamped)
	stamped int    // StampRendering calls
}

func (f *stampedFakeSyncable) RenderingVersion() uint64 { return f.renders }

func (f *stampedFakeSyncable) RenderingStamp(context.Context) (uint64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stamp, f.stamp != 0, nil
}

func (f *stampedFakeSyncable) StampRendering(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stamp = f.renders
	f.stamped++
	return nil
}

func (f *stampedFakeSyncable) stampState() (stamp uint64, stamped int, synced int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stamp, f.stamped, len(f.synced)
}

var _ cluster.RenderingStamped = (*stampedFakeSyncable)(nil)

// teardownFakeSyncable is a stamped sink that drops its destination on
// delete (the SQL family's shape) but cannot converge in place.
type teardownFakeSyncable struct{ stampedFakeSyncable }

func (f *teardownFakeSyncable) Teardown(bool) (bool, error) { return true, nil }

var _ cluster.Teardownable = (*teardownFakeSyncable)(nil)

// startStampedSyncable wires sink into a DB: state is the fake's stamp
// bookkeeping; syncable is the value handed to the engine (a wrapper type
// when the test needs an extra capability).
func startStampedSyncable(t *testing.T, state *stampedFakeSyncable, syncable cluster.Syncable) (parkedMessage func() (string, bool)) {
	t.Helper()
	_ = state
	d, s := newWalDBRemat(t, syncable)
	proposeTypeTOML(t, d, "photos", "photos", "", "")
	tp, err := s.ResolveType(cluster.LatestTypeRef("photos"))
	require.NoError(t, err)
	for _, k := range []string{"k1", "k2"} {
		require.NoError(t, d.Propose(testCtx(t),
			&cluster.Proposal{Entities: []*cluster.Entity{cluster.NewUpsertEntity(tp, []byte(k), []byte(`{"a":1}`))}}))
	}
	require.NoError(t, d.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID: "photos-mirror", MimeType: "text/toml",
		Data: []byte("[syncable]\nname = \"photos-mirror\"\ntype = \"fake\"\n"),
	}))
	return func() (string, bool) {
		rec, ok, err := s.SyncableStuck("photos-mirror")
		require.NoError(t, err)
		if !ok || !rec.Parked {
			return "", false
		}
		return rec.Message, true
	}
}

// A destination stamped by another rendering version is not written into:
// the worker parks with the converging verb named — rematerialize, since a
// keyed sink converges in place.
func TestRenderingStamp_MismatchParksNamingRematerialize(t *testing.T) {
	sink := &stampedFakeSyncable{rematFakeSyncable: rematFakeSyncable{keyed: true}, renders: 2, stamp: 1}
	parked := startStampedSyncable(t, sink, sink)
	require.Eventually(t, func() bool { _, ok := parked(); return ok }, 10*time.Second, 10*time.Millisecond, "the worker never parked")
	msg, _ := parked()
	require.Contains(t, msg, "rendered under sink rendering version 1")
	require.Contains(t, msg, "this binary renders version 2")
	require.Contains(t, msg, "POST /v1/syncable/photos-mirror/rematerialize")
	_, stamped, synced := sink.stampState()
	require.Zero(t, synced, "nothing is written into rows rendered by another version")
	require.Zero(t, stamped, "a mismatch is never papered over by re-stamping")
}

// A keyless sink cannot converge in place, so the remedy it names is the
// destructive one: delete (dropping the table) and re-POST.
func TestRenderingStamp_MismatchNamesDeleteForNonConvergingSinks(t *testing.T) {
	sink := &teardownFakeSyncable{stampedFakeSyncable{rematFakeSyncable: rematFakeSyncable{keyed: false}, renders: 2, stamp: 1}}
	parked := startStampedSyncable(t, &sink.stampedFakeSyncable, sink)
	require.Eventually(t, func() bool { _, ok := parked(); return ok }, 10*time.Second, 10*time.Millisecond)
	msg, _ := parked()
	require.Contains(t, msg, "DELETE /v1/syncable/photos-mirror (drops the table if committed created it; otherwise drop it yourself), then re-POST the config")
}

// A sink that neither converges in place nor drops its destination on
// delete (Iceberg keeps its table) names the by-hand step, or the re-POST
// would meet the same stamp.
func TestRenderingStamp_MismatchNamesRecreateForSinksThatKeepTheirTable(t *testing.T) {
	sink := &stampedFakeSyncable{rematFakeSyncable: rematFakeSyncable{keyed: false}, renders: 2, stamp: 1}
	parked := startStampedSyncable(t, sink, sink)
	require.Eventually(t, func() bool { _, ok := parked(); return ok }, 10*time.Second, 10*time.Millisecond)
	msg, _ := parked()
	require.Contains(t, msg, "cannot drop its destination: recreate the table by hand, then DELETE /v1/syncable/photos-mirror and re-POST")
}

// 0.8.0 introduces the stamp: a never-stamped destination is stamped with
// the current version on first contact, then served.
func TestRenderingStamp_FirstContactStampsThenSyncs(t *testing.T) {
	sink := &stampedFakeSyncable{rematFakeSyncable: rematFakeSyncable{keyed: true}, renders: 2}
	parked := startStampedSyncable(t, sink, sink)
	require.Eventually(t, func() bool { _, _, synced := sink.stampState(); return synced >= 2 }, 10*time.Second, 10*time.Millisecond, "never synced")
	stamp, stamped, _ := sink.stampState()
	require.Equal(t, uint64(2), stamp)
	require.Equal(t, 1, stamped, "stamped exactly once, before serving")
	_, ok := parked()
	require.False(t, ok)
}

// A rematerialization converges the rows and re-stamps on completion — after
// the sweep, before the in-progress record clears — so a destination that
// parked on a mismatch is served again once the operator runs the verb.
func TestRenderingStamp_RematerializeRestamps(t *testing.T) {
	sink := &stampedFakeSyncable{rematFakeSyncable: rematFakeSyncable{keyed: true}, renders: 2, stamp: 2}
	d, s := newWalDBRemat(t, sink)
	proposeTypeTOML(t, d, "photos", "photos", "", "")
	tp, err := s.ResolveType(cluster.LatestTypeRef("photos"))
	require.NoError(t, err)
	require.NoError(t, d.Propose(testCtx(t),
		&cluster.Proposal{Entities: []*cluster.Entity{cluster.NewUpsertEntity(tp, []byte("k1"), []byte(`{"a":1}`))}}))
	require.NoError(t, d.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID: "photos-mirror", MimeType: "text/toml",
		Data: []byte("[syncable]\nname = \"photos-mirror\"\ntype = \"fake\"\n"),
	}))
	require.Eventually(t, func() bool { _, _, synced := sink.stampState(); return synced >= 1 }, 10*time.Second, 10*time.Millisecond)
	_, stamped, _ := sink.stampState()
	require.Zero(t, stamped, "a current stamp is left alone")

	require.NoError(t, d.RematerializeSyncable(testCtx(t), "photos-mirror"))
	require.Eventually(t, func() bool {
		_, ok := s.SyncableRematerialization("photos-mirror")
		_, stamped, _ := sink.stampState()
		return !ok && stamped == 1
	}, 15*time.Second, 10*time.Millisecond, "the replay never completed and re-stamped")
	_, _, completed := sink.snapshot()
	require.GreaterOrEqual(t, completed, 1, "the sweep ran before the stamp")
}
