//go:build integration

package db_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster/db"
)

// createJoiningRaft builds a node that joins an existing cluster: it knows the
// full peer set (so its transport can reach the existing members and bind its
// own listener) but uses db.WithJoin so it starts with no configuration and
// learns its membership from the leader, rather than bootstrapping a competing
// cluster from the static peer set. Mirrors createRaft otherwise.
func createJoiningRaft(id uint64, peers []raft.Peer, tick time.Duration) *Raft {
	proposeC := make(chan []byte)
	confChangeC := make(chan *raftpb.ConfChangeV2)
	s := NewMemoryStorage()

	errorC, r := db.NewRaft(id, peers, s, proposeC, confChangeC,
		db.WithTickInterval(tick), db.WithJoin())

	return &Raft{
		storage:      s,
		errorC:       errorC,
		raft:         r,
		peers:        peers,
		proposeC:     proposeC,
		confChangeC:  confChangeC,
		id:           id,
		tickInterval: tick,
	}
}

// TestMembership_V1BootstrapConfChangeApplies asserts that a freshly started
// 3-node cluster converges on exactly the bootstrap configuration {1,2,3} with
// no lingering joint state. StartNode emits the bootstrap membership as v1
// EntryConfChange entries (carrying each peer's URL in Context), so this also
// exercises the backward-compatible v1 branch of the apply switch — the same
// path an upgraded binary uses to replay v1 conf changes left by an old one.
func TestMembership_V1BootstrapConfChangeApplies(t *testing.T) {
	rafts := createRafts(3)
	defer rafts.Close()
	rafts.WaitForLeader(t)

	for _, r := range rafts {
		waitForMembership(t, r, map[uint64]bool{1: true, 2: true, 3: true})
	}
}

// TestMembership_RemoveNode removes a follower from a 3-node cluster via a
// joint-consensus ConfChangeV2 and verifies the configuration settles to the
// final two-node form (not joint) and the survivors keep committing.
func TestMembership_RemoveNode(t *testing.T) {
	rafts := createRafts(3)
	defer rafts.Close()
	rafts.WaitForLeader(t)

	leader := rafts.LeaderRaft()
	proposeAndCheck(t, leader, "before-removal")

	victim := rafts.FollowerRaft()
	victimID := victim.id

	leader.submitConfChange(removeNodeCC(victimID))

	// The leader (and the other survivor) settle on a config without the
	// victim, with the joint transition complete.
	waitForMembership(t, leader, map[uint64]bool{victimID: false})

	// The surviving two-node cluster still reaches quorum and commits.
	proposeAndCheck(t, leader, "after-removal")
}

// TestMembership_AddNode grows a 3-node cluster to four. The new node starts
// in join mode (empty config, learns membership from the leader); after the
// joint-consensus add commits it becomes a voter, catches up on the existing
// log, and participates in new commits.
func TestMembership_AddNode(t *testing.T) {
	// Allocate ports for all four nodes up front so node 4's advertised URL
	// is known before the cluster starts. Nodes 1-3 bootstrap as the initial
	// cluster; node 4 joins later.
	ports := pickFreePorts(4)
	allPeers := make([]raft.Peer, 4)
	for i := 0; i < 4; i++ {
		allPeers[i] = raft.Peer{ID: uint64(i + 1), Context: []byte(fmt.Sprintf("http://127.0.0.1:%d", ports[i]))}
	}
	bootstrapPeers := allPeers[:3]

	tick := multiNodeTickInterval
	rafts := make(Rafts, 0, len(bootstrapPeers))
	for _, p := range bootstrapPeers {
		rafts = append(rafts, createRaft(p.ID, bootstrapPeers, NewMemoryStorage(), tick, nil))
	}
	// Close everything (including node 4, appended below) via a closure so the
	// deferred call sees the final slice, not its value at defer time.
	defer func() { rafts.Close() }()

	rafts.WaitForLeader(t)
	leader := rafts.LeaderRaft()
	proposeAndCheck(t, leader, "before-add")

	// Bring up node 4 in join mode knowing the full peer set, then add it.
	node4 := createJoiningRaft(4, allPeers, tick)
	rafts = append(rafts, node4)

	leader.submitConfChange(addNodeCC(4, string(allPeers[3].Context)))

	// The leader settles on the four-node config (not joint).
	waitForMembership(t, leader, map[uint64]bool{1: true, 2: true, 3: true, 4: true})

	// Node 4 catches up on the entry committed before it joined, then on a
	// fresh one committed after — proving it is a full participant.
	waitForUserEntry(t, node4, []byte("before-add"))
	proposeAndCheck(t, leader, "after-add")
	waitForUserEntry(t, node4, []byte("after-add"))
}

// TestMembership_AddNodePersistsPeerURL is the durability half of the
// restart-wedge fix, exercised through real consensus: when an add-node conf
// change commits, every existing node persists the new member's raft URL to
// durable membership (the memberPeerURLBucket a restart reconciles the transport
// from), and a subsequent remove deletes it. Without the persistence the restored
// ConfState would carry the member id with no address, and a restart would seed
// the transport only from the stale static COMMITTED_PEERS set.
func TestMembership_AddNodePersistsPeerURL(t *testing.T) {
	ports := pickFreePorts(4)
	allPeers := make([]raft.Peer, 4)
	for i := 0; i < 4; i++ {
		allPeers[i] = raft.Peer{ID: uint64(i + 1), Context: []byte(fmt.Sprintf("http://127.0.0.1:%d", ports[i]))}
	}
	bootstrapPeers := allPeers[:3]

	tick := multiNodeTickInterval
	rafts := make(Rafts, 0, 4)
	for _, p := range bootstrapPeers {
		rafts = append(rafts, createRaft(p.ID, bootstrapPeers, NewMemoryStorage(), tick, nil))
	}
	defer func() { rafts.Close() }()

	rafts.WaitForLeader(t)
	leader := rafts.LeaderRaft()

	node4 := createJoiningRaft(4, allPeers, tick)
	rafts = append(rafts, node4)
	node4URL := string(allPeers[3].Context)

	leader.submitConfChange(addNodeCC(4, node4URL))
	waitForMembership(t, leader, map[uint64]bool{1: true, 2: true, 3: true, 4: true})

	peerURL := func(r *Raft, id uint64) (string, bool) {
		u, ok := r.storage.(*MemoryStorage).MemberPeerURLs()[id]
		return u, ok
	}

	// Every original node persisted node 4's URL. Node 4 skips persisting its own
	// (applyConfChange skips self), so it is not asserted.
	for _, r := range rafts {
		if r.id == 4 {
			continue
		}
		require.Eventuallyf(t, func() bool {
			u, ok := peerURL(r, 4)
			return ok && u == node4URL
		}, membershipSettleTimeout, 20*time.Millisecond, "node %d must persist the added peer's URL", r.id)
	}

	// Removing node 4 deletes its durable URL everywhere, so a later restart's
	// reconcile can't re-add a member that is gone.
	leader.submitConfChange(removeNodeCC(4))
	waitForMembership(t, leader, map[uint64]bool{1: true, 2: true, 3: true})

	for _, r := range rafts {
		if r.id == 4 {
			continue
		}
		require.Eventuallyf(t, func() bool {
			_, ok := peerURL(r, 4)
			return !ok
		}, membershipSettleTimeout, 20*time.Millisecond, "node %d must delete the removed peer's URL", r.id)
	}
}

// startClusterWithJoiner brings up a 3-node bootstrap cluster (ids 1-3) plus a
// 4th node in join mode that is reachable but not yet a member. It returns the
// full rafts slice (close it via defer), the leader, node 4, and node 4's
// advertised peer URL — the shared setup for the learner add/promote tests.
func startClusterWithJoiner(t *testing.T) (rafts Rafts, leader, node4 *Raft, node4URL string) {
	t.Helper()
	ports := pickFreePorts(4)
	allPeers := make([]raft.Peer, 4)
	for i := 0; i < 4; i++ {
		allPeers[i] = raft.Peer{ID: uint64(i + 1), Context: []byte(fmt.Sprintf("http://127.0.0.1:%d", ports[i]))}
	}
	bootstrapPeers := allPeers[:3]

	tick := multiNodeTickInterval
	rafts = make(Rafts, 0, 4)
	for _, p := range bootstrapPeers {
		rafts = append(rafts, createRaft(p.ID, bootstrapPeers, NewMemoryStorage(), tick, nil))
	}
	rafts.WaitForLeader(t)
	leader = rafts.LeaderRaft()

	node4 = createJoiningRaft(4, allPeers, tick)
	rafts = append(rafts, node4)
	return rafts, leader, node4, string(allPeers[3].Context)
}

// TestMembership_AddLearner adds a 4th node as a learner and verifies it is in
// the learner set (NOT the voter set, so it can't count toward quorum or be
// elected) and that it still replicates the log from the leader.
func TestMembership_AddLearner(t *testing.T) {
	rafts, leader, node4, url := startClusterWithJoiner(t)
	defer rafts.Close()

	proposeAndCheck(t, leader, "before-learner")

	leader.submitConfChange(addLearnerCC(4, url))
	waitForLearner(t, leader, 4)

	voters, learners, joint := leader.memberRoles()
	require.False(t, joint)
	require.ElementsMatch(t, []uint64{1, 2, 3}, memberKeys(voters),
		"the learner must not be in the voter set — that is what keeps it out of quorum")
	require.ElementsMatch(t, []uint64{4}, memberKeys(learners))

	// The learner replicates the entry committed before it joined and a fresh
	// one committed after.
	waitForUserEntry(t, node4, []byte("before-learner"))
	proposeAndCheck(t, leader, "after-learner")
	waitForUserEntry(t, node4, []byte("after-learner"))
}

// TestMembership_PromoteLearner adds a learner, lets it catch up, then promotes
// it — verifying it moves from the learner set to the voter set and the cluster
// keeps committing with it as a full voter.
func TestMembership_PromoteLearner(t *testing.T) {
	rafts, leader, node4, url := startClusterWithJoiner(t)
	defer rafts.Close()

	proposeAndCheck(t, leader, "before-promote")

	leader.submitConfChange(addLearnerCC(4, url))
	waitForLearner(t, leader, 4)
	waitForUserEntry(t, node4, []byte("before-promote")) // caught up

	leader.submitConfChange(promoteCC(4))
	waitForVoter(t, leader, 4) // 4 in the voter set, not merely the union

	voters, learners, joint := leader.memberRoles()
	require.False(t, joint)
	require.ElementsMatch(t, []uint64{1, 2, 3, 4}, memberKeys(voters))
	require.Empty(t, memberKeys(learners), "no learners remain after promotion")

	// The cluster still commits with node 4 now a voter.
	proposeAndCheck(t, leader, "after-promote")
	waitForUserEntry(t, node4, []byte("after-promote"))
}
