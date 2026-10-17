package db_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// The membership API refuses, when proposed, an add-learner naming a voter
// (raft would demote it) and an add naming a removed id (a removed node
// rejoins under a new id).
func TestMembership_RefusedWhenProposed(t *testing.T) {
	d, _ := newWalDB(t)
	const peerURL = "http://127.0.0.1:29332"

	err := d.AddLearner(testCtx(t), 1, peerURL)
	require.ErrorIs(t, err, cluster.ErrInvalidMember)
	require.ErrorContains(t, err, "already a voter")

	require.NoError(t, d.AddLearner(testCtx(t), 2, peerURL))
	require.NoError(t, d.RemoveMember(testCtx(t), 2))
	for _, add := range []func() error{
		func() error { return d.AddLearner(testCtx(t), 2, peerURL) },
		func() error { return d.AddMember(testCtx(t), 2, peerURL) },
	} {
		err := add()
		require.ErrorIs(t, err, cluster.ErrInvalidMember)
		require.ErrorContains(t, err, "was removed from this cluster")
	}
}

// The leader switches the apply-time guard on by itself once every member
// runs a release that has it; a change that reaches the log invalid is then
// refused when applied, and its proposer gets the reason at once rather than
// waiting out the settle timeout.
func TestMembership_GuardRefusalReachesTheProposer(t *testing.T) {
	p := parser.New()
	s, err := wal.Open(t.TempDir(), p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	d := db.New(1, db.Peers{1: ""}, s, p, nil, nil, db.WithTickInterval(testTickInterval), db.WithVersionAnnounce())
	t.Cleanup(func() { _ = d.Close(); _ = s.Close() })
	require.Eventually(t, d.MembershipGuardOnForTest, 30*time.Second, 10*time.Millisecond,
		"the leader switches the guard on")

	start := time.Now()
	err = d.AddLearnerUncheckedForTest(testCtx(t), 1, "http://127.0.0.1:29333")
	require.ErrorIs(t, err, cluster.ErrInvalidMember)
	require.ErrorContains(t, err, "would demote it")
	require.Less(t, time.Since(start), 10*time.Second)

	require.NoError(t, d.AddLearner(testCtx(t), 2, "http://127.0.0.1:29333"),
		"the next change goes through")
}
