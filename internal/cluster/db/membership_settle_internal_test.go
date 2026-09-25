package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/quorum"
	"go.etcd.io/raft/v3/tracker"
)

type membershipStatusNode struct {
	raft.Node
	status raft.Status
}

func (n *membershipStatusNode) Status() raft.Status { return n.status }

// Model the window after ApplyConfChange exposes the final configuration,
// but before Advance acknowledges its entry. This must block even when the
// waiter checks without receiving an applied notification first.
func TestMembershipWaitRequiresRaftAppliedConfiguration(t *testing.T) {
	for _, target := range []membershipTarget{memberLearner, memberVoter, memberAbsent} {
		t.Run(map[membershipTarget]string{memberLearner: "learner", memberVoter: "voter", memberAbsent: "removed"}[target], func(t *testing.T) {
			node := &membershipStatusNode{status: raft.Status{
				BasicStatus: raft.BasicStatus{Applied: 81},
				Config:      tracker.Config{Voters: quorum.JointConfig{{1: {}}, nil}},
			}}
			switch target {
			case memberLearner:
				node.status.Config.Learners = map[uint64]struct{}{2: {}}
			case memberVoter:
				node.status.Config.Voters[0][2] = struct{}{}
			}
			r := &Raft{node: node}
			r.lastMembershipIndex.Store(82)
			d := &DB{raft: r, ctx: context.Background(), appliedNotifyCh: make(chan struct{})}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			require.ErrorIs(t, d.waitForMembership(ctx, 2, target), context.Canceled,
				"the final role alone must not satisfy the immediate check")

			node.status.Applied = 82
			require.NoError(t, d.waitForMembership(context.Background(), 2, target),
				"Advance must make the configuration eligible to satisfy the waiter")
		})
	}
}
