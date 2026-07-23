package db_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
)

// TestPropose_TimesOutInsteadOfHangingUnresolved pins the dropped-propose silent
// hang. A committed-but-unapplied proposal (slowApplyStorage blocks the user-data
// apply) never signals its waiter — the same terminal state a raft-DROPPED
// proposal reaches when no leader change follows (forwardProposeErr absorbs
// ErrProposalDropped, so nothing signals the waiter). Without the propose-timeout
// backstop db.Propose hangs there forever, wedging the calling worker SILENTLY
// (gauge stuck at 1, no stuck record, no freeze — a support nightmare). It must
// instead surface ErrProposalUnknown so the worker resolves the wedge visibly
// through the commit-ambiguity ladder.
func TestPropose_TimesOutInsteadOfHangingUnresolved(t *testing.T) {
	d, s := newIngestFailFastDBWith(t, db.WithProposeTimeout(150*time.Millisecond))
	t.Cleanup(func() { s.Unblock(); _ = d.Close() })

	require.Eventually(t, d.IsLeaderForTest, 5*time.Second, 5*time.Millisecond,
		"single-node db never became leader")

	// The user-data apply is blocked, so this proposal commits but its waiter is
	// never signaled.
	up := &cluster.Proposal{Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(&cluster.Type{ID: "evt", Version: 1}, []byte("k"), []byte(`{"x":1}`)),
	}}

	errc := make(chan error, 1)
	go func() { errc <- d.Propose(context.Background(), up) }()

	select {
	case err := <-errc:
		require.ErrorIs(t, err, db.ErrProposalUnknown,
			"an unresolved propose must time out to ErrProposalUnknown (the worker then freezes/re-syncs/stuck-records — visible), not hang")
	case <-time.After(5 * time.Second):
		t.Fatal("Propose hung on an unresolved (committed-but-unapplied) proposal instead of timing out to ErrProposalUnknown")
	}
}
