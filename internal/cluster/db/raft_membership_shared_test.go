package db_test

import (
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

// Shared helpers for the joint-consensus membership tests. This file carries
// no build tag so both the integration suite (raft_membership_test.go) and the
// adversarial suite (raft_adversarial_test.go) can use it — same pattern as the
// untagged cluster/transport harness in raft_test.go.

// membershipSettleTimeout bounds how long a membership test waits for a
// joint-consensus change (two committed entries: enter joint, then the
// JointImplicit auto-leave) to converge. Generous because it also covers a
// freshly-added node catching up on the full log.
const membershipSettleTimeout = 10 * time.Second

// addNodeCC / removeNodeCC build the exact ConfChangeV2 values db.AddMember /
// db.RemoveMember construct in production (JointImplicit + a single change).
// The *Raft test wrapper has no enclosing db.DB, so tests submit the change
// straight onto the propose channel and then observe the result.
func addNodeCC(id uint64, url string) raftpb.ConfChangeV2 {
	return raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionJointImplicit,
		Changes:    []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddNode, NodeID: id}},
		Context:    []byte(url),
	}
}

func removeNodeCC(id uint64) raftpb.ConfChangeV2 {
	return raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionJointImplicit,
		Changes:    []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeRemoveNode, NodeID: id}},
	}
}

// submitConfChange sends cc on this node's conf-change channel, the same path
// db.AddMember / db.RemoveMember feed.
func (rs *Raft) submitConfChange(cc raftpb.ConfChangeV2) {
	rs.mu.RLock()
	ch := rs.confChangeC
	rs.mu.RUnlock()
	ch <- cc
}

// members returns this node's observed voter set and whether it is still in a
// joint transition. Thin pass-through to the production memberStatus via the
// MembersForTest export.
func (rs *Raft) members() (map[uint64]struct{}, bool) {
	rs.mu.RLock()
	r := rs.raft
	rs.mu.RUnlock()
	return r.MembersForTest()
}

// waitForMembership blocks until r's observed configuration matches want
// (id → should-be-present) AND the joint transition has completed, or the
// deadline fires. A settled, non-joint configuration is what an operator (and
// db.waitForMembership) treats as "the change took effect".
func waitForMembership(t *testing.T, r *Raft, want map[uint64]bool) {
	t.Helper()
	deadline := time.Now().Add(membershipSettleTimeout)
	for {
		members, joint := r.members()
		if !joint && membershipMatches(members, want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("membership did not settle to %v within %s (have members=%v joint=%v)",
				want, membershipSettleTimeout, memberKeys(members), joint)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func membershipMatches(members map[uint64]struct{}, want map[uint64]bool) bool {
	for id, shouldBePresent := range want {
		_, present := members[id]
		if present != shouldBePresent {
			return false
		}
	}
	return true
}

func memberKeys(m map[uint64]struct{}) []uint64 {
	out := make([]uint64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
