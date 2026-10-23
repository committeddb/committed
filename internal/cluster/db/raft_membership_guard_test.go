//go:build integration

package db_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster/db"
)

// guardMarkerCC is the UpdateNode the leader proposes to switch the
// membership guard on once every member runs a release that has it.
func guardMarkerCC(id uint64) *raftpb.ConfChangeV2 {
	return &raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto.Enum(),
		Changes:    []*raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeUpdateNode.Enum(), NodeId: &id}},
		Context:    []byte(db.MembershipGuardMarkerForTest),
	}
}

// switchOnGuard submits the marker through submit until every node in rs has
// applied it (raft drops a conf change proposed while another is pending).
func switchOnGuard(t *testing.T, rs Rafts, submit *Raft) {
	t.Helper()
	deadline := time.Now().Add(membershipSettleTimeout)
	for {
		submit.submitConfChange(guardMarkerCC(submit.id))
		slice := time.Now().Add(time.Second)
		for time.Now().Before(slice) {
			on := 0
			for _, r := range rs {
				if _, ok := r.storage.MembershipGuardIndex(); ok {
					on++
				}
			}
			if on == len(rs) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("membership guard not switched on everywhere within %s", membershipSettleTimeout)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// refused submits cc through submit until every node in rs has refused it.
func refused(t *testing.T, rs Rafts, submit *Raft, cc *raftpb.ConfChangeV2) {
	t.Helper()
	before := make([]uint64, len(rs))
	for i, r := range rs {
		before[i] = r.raft.GuardRejectionsForTest()
	}
	deadline := time.Now().Add(membershipSettleTimeout)
	for {
		submit.submitConfChange(cc)
		slice := time.Now().Add(time.Second)
		for time.Now().Before(slice) {
			done := 0
			for i, r := range rs {
				if r.raft.GuardRejectionsForTest() > before[i] {
					done++
				}
			}
			if done == len(rs) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("change not refused by every node within %s", membershipSettleTimeout)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// Once the guard is on, a committed add-learner naming a voter and a re-add of
// a removed id are refused by every node when applied — the voter stays a
// voter, the removed id stays out — and the cluster keeps committing.
func TestMembershipGuard_RefusesInvalidChangesAtApply(t *testing.T) {
	rafts := createRafts(3)
	defer rafts.Close()
	rafts.WaitForLeader(t)
	leader := rafts.LeaderRaft()
	switchOnGuard(t, rafts, leader)

	follower := rafts.FollowerRaft()
	refused(t, rafts, leader, addLearnerCC(follower.id, "http://127.0.0.1:1"))
	for _, r := range rafts {
		waitForVoter(t, r, follower.id)
	}

	removeNodeAndWait(t, leader, leader, follower.id, map[uint64]bool{follower.id: false})
	var survivors Rafts
	for _, r := range rafts {
		if r.id != follower.id {
			survivors = append(survivors, r)
		}
	}
	for _, r := range survivors {
		// removeNodeAndWait waits for the leader's apply; the other survivor
		// applies the removal on its own schedule.
		require.Eventually(t, func() bool { return r.raft.IsRemovedForTest(follower.id) },
			membershipSettleTimeout, 5*time.Millisecond, "the transport refuses a removed node's messages")
	}
	refused(t, survivors, leader, addNodeCC(follower.id, "http://127.0.0.1:1"))
	for _, r := range survivors {
		waitForMembership(t, r, map[uint64]bool{follower.id: false, leader.id: true})
	}
	proposeAndCheck(t, leader, "after-refusals")
}

// Removing the only voter, committed despite the proposal-time check, would
// panic raft on every node now and on every restart; the guard refuses it.
func TestMembershipGuard_RefusesRemovingTheLastVoter(t *testing.T) {
	rafts := createRafts(1)
	defer rafts.Close()
	rafts.WaitForLeader(t)
	leader := rafts.LeaderRaft()
	switchOnGuard(t, rafts, leader)

	refused(t, rafts, leader, removeNodeCC(leader.id))
	waitForVoter(t, leader, leader.id)
	proposeAndCheck(t, leader, "still-a-voter")
}

// A removal committed before the guard switched on doesn't count: members
// that ran an older release then didn't record it, so counting it would let
// nodes reach different verdicts. Such an id can be re-added afterwards.
func TestMembershipGuard_IgnoresRemovalsBeforeItWasOn(t *testing.T) {
	rafts := createRafts(3)
	defer rafts.Close()
	rafts.WaitForLeader(t)
	leader := rafts.LeaderRaft()

	follower := rafts.FollowerRaft()
	removeNodeAndWait(t, leader, leader, follower.id, map[uint64]bool{follower.id: false})
	var survivors Rafts
	for _, r := range rafts {
		if r.id != follower.id {
			survivors = append(survivors, r)
		}
	}
	switchOnGuard(t, survivors, leader)

	var url string
	for _, p := range follower.peers {
		if p.ID == follower.id {
			url = string(p.Context)
		}
	}
	leader.submitConfChange(addLearnerCC(follower.id, url))
	for _, r := range survivors {
		waitForLearner(t, r, follower.id)
		require.Zero(t, r.raft.GuardRejectionsForTest())
		require.False(t, r.raft.IsRemovedForTest(follower.id), "a member again, so its messages are accepted")
	}
}
