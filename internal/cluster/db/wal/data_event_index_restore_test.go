package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestDataEventIndexSurvivesRestartBehindInternalTail is the field
// regression: a log whose TRAILING run of internal entries (syncable index
// bumps, positions, dead-letters) exceeds the Open-time backscan cap booted
// with DataEventIndex == 0 — so a freshly created syncable saw an empty
// replay range, folded NOTHING, and reported caughtUp/lag 0 over an empty
// table, completely silently (lag is computed against the same broken
// head). The trigger is an idle cluster: with capture quiet, checkpoints
// and coordination keep appending internal entries until the tail's
// internal run outgrows any fixed cap. The head is APPLIED STATE and must
// survive restart like the rest of applied state — by persistence, not
// derivation.
func TestDataEventIndexSurvivesRestartBehindInternalTail(t *testing.T) {
	id := "idle-tail"
	s := NewStorage(t, nil)
	defer s.Cleanup()

	pc := newProposalCreatorForString(t, s)

	// A little real data...
	user := pc.createAndSaveProposals(t, [][]string{{"d1"}, {"d2"}})
	_ = user

	// ...then an idle-cluster tail: a run of INTERNAL entries longer than
	// any backscan bound (the field log's shape after capture completed).
	internal := make([]*cluster.Proposal, 0, 5000)
	for i := 0; i < 5000; i++ {
		internal = append(internal, createSyncableIndexProposal(t, id))
	}
	pc.saveProposals(t, internal)

	headBefore := s.DataEventIndex()
	require.NotZero(t, headBefore, "precondition: the live head knows the data entries")

	// Restart: close and reopen the same directory.
	reopened, err := s.CloseAndReopen()
	require.NoError(t, err)
	defer reopened.Cleanup()

	require.Equal(t, headBefore, reopened.DataEventIndex(),
		"the data head must survive restart regardless of how long the trailing internal run is — head 0 makes a fresh syncable silently fold nothing while reporting caughtUp")
}
