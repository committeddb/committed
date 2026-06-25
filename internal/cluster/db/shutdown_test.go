package db

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestPickShutdownTransferTarget(t *testing.T) {
	const self = uint64(1)

	t.Run("most caught-up voter wins, learners and self skipped", func(t *testing.T) {
		members := []memberView{
			{id: 1, match: 100},               // self — skipped
			{id: 2, match: 90},                // a voter, but behind id 3
			{id: 3, match: 95},                // most caught-up non-self voter
			{id: 4, learner: true, match: 99}, // learner — skipped even if caught up
		}
		require.Equal(t, uint64(3), pickShutdownTransferTarget(self, members))
	})

	t.Run("single voter (only self) has no successor", func(t *testing.T) {
		require.Zero(t, pickShutdownTransferTarget(self, []memberView{{id: 1, match: 50}}))
	})

	t.Run("only a learner besides self has no successor", func(t *testing.T) {
		require.Zero(t, pickShutdownTransferTarget(self, []memberView{{id: 1}, {id: 2, learner: true}}))
	})

	t.Run("no members", func(t *testing.T) {
		require.Zero(t, pickShutdownTransferTarget(self, nil))
	})
}

// On the leader, Close's hand-off triggers the transfer to the chosen target and
// returns as soon as leadership moves off this node.
func TestTransferLeadershipBeforeStop_TransfersAndWaits(t *testing.T) {
	var transferred []uint64
	var stillLeader atomic.Bool
	stillLeader.Store(true)

	d := &DB{logger: zap.NewNop(), shutdownTransferTimeout: 2 * time.Second}
	d.shutdownTransferTargetFn = func() uint64 { return 2 }
	d.transferLeadershipFn = func(id uint64) {
		transferred = append(transferred, id)
		stillLeader.Store(false) // the hand-off completes
	}
	d.isLeaderFn = stillLeader.Load

	start := time.Now()
	d.transferLeadershipBeforeStop()

	require.Equal(t, []uint64{2}, transferred, "leadership transferred to the chosen target")
	require.Less(t, time.Since(start), time.Second, "returned promptly once leadership moved")
}

// A non-leader (or a node with no voter successor) hands off nothing.
func TestTransferLeadershipBeforeStop_NoTargetSkips(t *testing.T) {
	called := false
	d := &DB{logger: zap.NewNop(), shutdownTransferTimeout: time.Second}
	d.shutdownTransferTargetFn = func() uint64 { return 0 }
	d.transferLeadershipFn = func(uint64) { called = true }
	d.isLeaderFn = func() bool { return true }

	d.transferLeadershipBeforeStop()

	require.False(t, called, "no transfer attempted when there is no target")
}

// If leadership never moves, the hand-off gives up after the bounded timeout and
// lets Close proceed rather than hanging.
func TestTransferLeadershipBeforeStop_TimeoutFallsThrough(t *testing.T) {
	var transferred []uint64
	d := &DB{logger: zap.NewNop(), shutdownTransferTimeout: 50 * time.Millisecond}
	d.shutdownTransferTargetFn = func() uint64 { return 2 }
	d.transferLeadershipFn = func(id uint64) { transferred = append(transferred, id) }
	d.isLeaderFn = func() bool { return true } // never hands off

	start := time.Now()
	d.transferLeadershipBeforeStop()
	elapsed := time.Since(start)

	require.Equal(t, []uint64{2}, transferred, "the transfer was attempted")
	require.GreaterOrEqual(t, elapsed, 50*time.Millisecond, "waited for the bounded timeout")
	require.Less(t, elapsed, time.Second, "did not wait far past the timeout")
}
