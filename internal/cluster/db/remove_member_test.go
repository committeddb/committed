package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestRemoveMember_RefusesLastVoter is the last-voter guard's success criterion:
// removing the sole voter is refused up front with ErrWouldRemoveLastVoter,
// instead of committing a conf change that raft rejects only at apply time — by
// panicking and permanently bricking the log so every node re-panics on restart.
// The test completing (not panicking, not hanging to the ctx deadline) is the
// adversarial no-panic assertion.
func TestRemoveMember_RefusesLastVoter(t *testing.T) {
	d, _ := newWalDB(t) // single node, id 1 — the sole voter

	err := d.RemoveMember(testCtx(t), 1)
	require.ErrorIs(t, err, cluster.ErrWouldRemoveLastVoter,
		"removing the only voter must be refused, not proposed to raft")
}
