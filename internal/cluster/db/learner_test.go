package db_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestPromoteMember_RejectsNonLearner verifies PromoteMember's only guard: it
// refuses to promote an id that isn't a current learner. A single-node DB is
// enough — the validation reads the local applied configuration and returns
// before submitting any conf change. (The happy-path promotion is covered by
// the multi-node TestMembership_PromoteLearner integration test.)
func TestPromoteMember_RejectsNonLearner(t *testing.T) {
	d, _ := newWalDB(t)
	require.Eventually(t, func() bool { return d.Leader() == 1 }, 5*time.Second, 5*time.Millisecond,
		"single node should elect itself")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Unknown id — not in voters or learners.
	require.ErrorIs(t, d.PromoteMember(ctx, 99), cluster.ErrNotLearner)

	// An existing voter (this node) — promoting it is a no-op, so rejected.
	require.ErrorIs(t, d.PromoteMember(ctx, 1), cluster.ErrNotLearner)

	// Zero id — malformed.
	require.ErrorIs(t, d.PromoteMember(ctx, 0), cluster.ErrInvalidMember)
}
