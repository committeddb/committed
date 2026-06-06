//go:build integration

package db_test

import (
	"fmt"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/philborlin/committed/internal/cluster/db"
)

// createJoiningRaft builds a node that joins an existing cluster: it knows the
// full peer set (so its transport can reach the existing members and bind its
// own listener) but uses db.WithJoin so it starts with no configuration and
// learns its membership from the leader, rather than bootstrapping a competing
// cluster from the static peer set. Mirrors createRaft otherwise.
func createJoiningRaft(id uint64, peers []raft.Peer, tick time.Duration) *Raft {
	proposeC := make(chan []byte)
	confChangeC := make(chan raftpb.ConfChangeV2)
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
