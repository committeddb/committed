package db_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Closing the leader hands leadership off before raft stops: Close picks a
// target, fires the transfer, and waits for leadership to move — so a graceful
// restart (e.g. a rolling upgrade) doesn't force the cluster through a full
// election. The hooks stand in for a live cluster; the raft-level hand-off
// itself is covered by TestTransferLeadership_MovesLeaderToTarget.
func TestClose_TransfersLeadershipBeforeStopping(t *testing.T) {
	d := createDB()

	var transferred []uint64
	var stillLeader atomic.Bool
	stillLeader.Store(true)
	d.SetShutdownTransferHooksForTest(
		func() uint64 { return 2 },
		func(id uint64) {
			transferred = append(transferred, id)
			stillLeader.Store(false) // the hand-off completes
		},
		stillLeader.Load,
		2*time.Second,
	)

	require.NoError(t, d.Close())
	require.Equal(t, []uint64{2}, transferred, "Close handed off leadership before stopping raft")
}
