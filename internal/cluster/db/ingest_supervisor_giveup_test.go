package db_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
)

// TestIngestSupervisor_GiveupCancelsFrozenContext pins #2: the give-up branch is
// the un-fixed sibling of the restart-path context-node leak (fixed in 51a9dfc).
// A worker that exits via ingestExitFreeze returns without cancelling its context,
// so workerCtx stays an un-cancelled child of the long-lived db.ctx. The restart
// path cancels it before dropping the handle; the terminal give-up branch must do
// the same, or the node leaks that context node (and pins the handle) until Close.
func TestIngestSupervisor_GiveupCancelsFrozenContext(t *testing.T) {
	d, s := newIngestFailFastDBWith(t,
		db.WithIngestSupervisorMaxAttempts(1),
	)
	t.Cleanup(func() {
		s.Unblock()
		_ = d.Close()
	})

	proposal := &cluster.Proposal{Entities: []*cluster.Entity{{
		Type: &cluster.Type{ID: "string"}, Key: []byte("k"), Data: []byte("v"),
	}}}
	ing := newFreezeRecordingIngestable(proposal, cluster.Position([]byte("pos")))

	frozenErr := d.SuperviseRestartIngestGiveupForTest("giveup-ctx", ing)
	require.ErrorIs(t, frozenErr, context.Canceled,
		"the supervisor give-up branch must cancel the frozen worker's context (else the node leaks a context node until Close)")
}
