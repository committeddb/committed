package db_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db"
)

// TestMembership_BackToBackChangesAreNotDropped pins the Ready loop's ordering
// behind a CI hang (2026-09-05): the applied broadcast fired before Advance,
// so a membership change returned while raft's own applied index still sat
// before the entry — and raft silently drops a conf change proposed in that
// window ("possible unapplied conf change"). The next change, proposed
// immediately, was dropped and its caller waited forever. Advance now
// precedes the broadcast, and every membership check also waits for Raft's
// applied index to cover the configuration entry: observing the final role
// before the broadcast must not let the next call race ahead.
func TestMembership_BackToBackChangesAreNotDropped(t *testing.T) {
	d, _ := newWalDB(t)
	const peerURL = "http://127.0.0.1:29331"
	for i := 0; i < 30; i++ {
		require.NoError(t, d.AddLearner(testCtx(t), 2, peerURL), "iteration %d: add", i)
		require.True(t, d.RaftAppliedCoversStorageForTest(),
			"iteration %d: AddLearner returned before raft's applied index passed the change — the next change would be dropped", i)
		require.NoError(t, d.RemoveMember(testCtx(t), 2), "iteration %d: remove", i)
		require.True(t, d.RaftAppliedCoversStorageForTest(), "iteration %d: RemoveMember returned early", i)
	}
}

// TestMembership_WaitIsBounded pins the belt for the other drop causes: a
// change that never takes effect fails with ErrMembershipUnsettled instead of
// hanging the request forever.
func TestMembership_WaitIsBounded(t *testing.T) {
	d, _ := newWalDB(t)
	defer db.SetMembershipSettleTimeoutForTest(200 * time.Millisecond)()
	start := time.Now()
	err := d.WaitForVoterForTest(context.Background(), 999)
	require.ErrorIs(t, err, db.ErrMembershipUnsettled)
	require.Less(t, time.Since(start), 5*time.Second)
}
