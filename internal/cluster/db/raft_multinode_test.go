//go:build integration

// Multi-node raft tests. Tagged `integration` (rather than living in
// raft_test.go) because they bind real loopback TCP listeners via
// httptransport, depend on the OS network stack, and are timing-sensitive
// in a way pure unit tests shouldn't be. Each test pays ~100-300ms of
// election-timeout latency, which is fine for an integration sweep but
// not for `make test`.
//
// Helpers (createRafts, Rafts.WaitForLeader, Rafts.StartDrainers,
// proposeAndCheck, waitForUserEntry, etc.) live in raft_test.go without
// a build tag so they're available to both single-node tests and to this
// file when the integration tag is set.

package db_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// TestRaftPropose_Cluster3 is the multi-node sibling of TestRaftPropose's
// single-node cases. Builds a 3-node cluster over loopback HTTP, waits
// for a leader, proposes two inputs, and verifies every node's storage
// reflects them.
//
// Lives here (not as a subtest in TestRaftPropose) because the table-
// driven shape would force the whole TestRaftPropose function under the
// integration tag, regressing unit-test coverage of the single-node path.
func TestRaftPropose_Cluster3(t *testing.T) {
	inputs := []string{"a/b/c", "foo"}

	rafts := createRafts(3)
	defer rafts.Close()
	stopDrainers := rafts.StartDrainers()
	defer stopDrainers()

	rafts.WaitForLeader(t)

	for _, input := range inputs {
		proposeAndCheck(t, rafts[0], input)
	}

	// Walk each node's user-entry suffix and assert the inputs landed in
	// order. Storage indices vary by bootstrap shape, so a "find by
	// content" approach is more robust than hardcoding offsets.
	for _, r := range rafts {
		userEnts, err := r.ents()
		if err != nil {
			t.Fatal(err)
		}
		if len(userEnts) < len(inputs) {
			t.Fatalf("node %d: expected at least %d user entries, got %d",
				r.id, len(inputs), len(userEnts))
		}
		start := len(userEnts) - len(inputs)
		for i, input := range inputs {
			diff := cmp.Diff([]byte(input), userEnts[start+i].Data)
			if diff != "" {
				t.Fatalf("node %d, entry %d: %s", r.id, i, diff)
			}
		}
	}

	// Phase-1 storage invariant: P_local == R_local on every node. The
	// Ready loop also checks this (fatal-exiting on violation) but the
	// test makes it explicit so a regression of either value surfaces
	// as a test failure rather than a cluster-wide fatal log line.
	for _, r := range rafts {
		p, rl := r.storage.EventIndex(), r.storage.AppliedIndex()
		if p != rl {
			t.Fatalf("node %d: storage invariant violation: EventIndex=%d AppliedIndex=%d", r.id, p, rl)
		}
	}
}

// TestRaftRestart_Cluster3 is the multi-node sibling of TestRaftRestart's
// single-node case. Restart only restarts node 0; the other two replicas
// stay up, the cluster keeps quorum, and node 0 catches back up via raft
// log replication when it returns.
func TestRaftRestart_Cluster3(t *testing.T) {
	inputs1 := []string{"foo"}
	inputs2 := []string{"bar"}

	rafts := createRafts(3)
	defer rafts.Close()

	// Per-node drainers so we can swap the one belonging to the node that
	// gets restarted. A test-wide StartDrainers would hold a stale commitC
	// reference for the restarted node and silently stop draining.
	drainers := make([]func(), len(rafts))
	for i, r := range rafts {
		drainers[i] = r.startDrainer()
	}
	defer func() {
		for _, d := range drainers {
			if d != nil {
				d()
			}
		}
	}()

	rafts.WaitForLeader(t)

	for _, input := range inputs1 {
		proposeAndCheck(t, rafts[0], input)
	}

	// Stop drainer for node 0 BEFORE Restart so the goroutine exits via
	// its stop channel before db.Raft.Close closes commitC out from under
	// it.
	drainers[0]()
	drainers[0] = nil

	if err := rafts[0].Restart(); err != nil {
		t.Fatalf("restart node 0: %v", err)
	}

	// Fresh drainer for the new commitC. Without this the new Ready loop
	// blocks on the first replayed committed entry and the next propose
	// hangs.
	drainers[0] = rafts[0].startDrainer()

	rafts.WaitForLeader(t)

	for _, input := range inputs2 {
		proposeAndCheck(t, rafts[0], input)
	}

	for _, r := range rafts {
		es, err := r.ents()
		if err != nil {
			t.Fatal(err)
		}

		want := append([]string(nil), inputs1...)
		want = append(want, inputs2...)

		if len(es) < len(want) {
			t.Fatalf("node %d: expected at least %d user entries, got %d",
				r.id, len(want), len(es))
		}
		start := len(es) - len(want)
		for i, w := range want {
			diff := cmp.Diff(w, string(es[start+i].Data))
			if diff != "" {
				t.Fatalf("node %d, entry %d: %s", r.id, i, diff)
			}
		}
	}
}

// TestRaftPropose_FromFollower verifies that proposing on a non-leader
// node is correctly forwarded to the leader by raft and ends up applied
// on every node in the cluster. This is the path the production HTTP API
// relies on when a write request lands on a follower: callers shouldn't
// need to know which node is leader, raft handles forwarding transparently.
//
// Without this test, follower-side proposes are exercised only by accident
// in TestRaftPropose_Cluster3 (where we propose on rafts[0], which happens
// to win elections most of the time). Forcing the propose onto a known
// follower is the only way to actually test the forwarding path.
func TestRaftPropose_FromFollower(t *testing.T) {
	rafts := createRafts(3)
	defer rafts.Close()

	stopDrainers := rafts.StartDrainers()
	defer stopDrainers()

	rafts.WaitForLeader(t)

	follower := rafts.FollowerRaft()
	if follower == nil {
		t.Fatal("FollowerRaft returned nil after WaitForLeader; cluster has no follower?")
	}

	// Sanity check: the follower really is a follower (not the leader).
	leader := rafts.LeaderRaft()
	if follower.id == leader.id {
		t.Fatalf("FollowerRaft returned the leader (id=%d)", follower.id)
	}

	const input = "from-follower"
	follower.proposeC <- []byte(input)

	// Verify the propose lands on every node, not just the follower or
	// the leader. The raft contract is that a successfully-committed
	// entry is replicated to a quorum and eventually to all live nodes.
	for _, r := range rafts {
		waitForUserEntry(t, r, []byte(input))
	}
}

// TestRaftPropose_LeaderKillReelectsAndAccepts kills the current leader
// of a 3-node cluster, waits for the surviving 2 nodes to elect a new
// leader (2-out-of-3 still meets quorum), then proposes a new entry on
// one of the survivors and asserts that the proposal commits on both
// surviving nodes.
//
// This is the most basic adversarial test of raft's failover semantics:
// the cluster must keep accepting writes after a leader loss. If election
// timeouts, vote messages, or post-election propose forwarding break in
// any way under leader-kill conditions, this test catches it.
func TestRaftPropose_LeaderKillReelectsAndAccepts(t *testing.T) {
	rafts := createRafts(3)
	defer rafts.Close()

	drainers := make([]func(), len(rafts))
	for i, r := range rafts {
		drainers[i] = r.startDrainer()
	}
	defer func() {
		for _, d := range drainers {
			if d != nil {
				d()
			}
		}
	}()

	rafts.WaitForLeader(t)

	// Establish a baseline by proposing one entry while the original
	// leader is still alive. This both confirms the cluster is healthy
	// and gives us a known prefix to assert on after the kill.
	proposeAndCheck(t, rafts[0], "before-kill")

	// Identify and kill the leader.
	leaderID := rafts.LeaderRaft().id
	killIdx := -1
	for i, r := range rafts {
		if r.id == leaderID {
			killIdx = i
			break
		}
	}
	if killIdx < 0 {
		t.Fatalf("could not find leader id %d in rafts slice", leaderID)
	}

	// Stop the leader's drainer first so the goroutine exits via its
	// stop channel before db.Raft.Close closes commitC. Order matters
	// here for the same reason as in TestRaftRestart_Cluster3.
	drainers[killIdx]()
	drainers[killIdx] = nil
	if err := rafts[killIdx].Close(); err != nil {
		t.Fatalf("close leader: %v", err)
	}

	// Build a "remaining" slice without the dead node so WaitForLeader
	// doesn't try to read its (now-stopped) raft.Node Status. The
	// stableLeader helper additionally requires the elected leader ID
	// to be one of the slice members, which prevents a transient state
	// where the survivors still report the dead leader's ID from being
	// interpreted as a stable cluster.
	var remaining Rafts
	for i, r := range rafts {
		if i != killIdx {
			remaining = append(remaining, r)
		}
	}

	// Re-election happens after the surviving nodes' election timeouts
	// fire (~100ms with our 10ms tick * 10-tick election). 5s upper
	// bound is generous.
	newLeader := remaining.WaitForLeader(t)
	if newLeader == leaderID {
		t.Fatalf("expected new leader, still see old leader id %d", leaderID)
	}

	// Propose a new entry on one of the survivors and verify both
	// remaining nodes apply it. The propose may land on a follower or
	// the new leader; either way raft handles the routing.
	proposeAndCheck(t, remaining[0], "after-kill")
	for _, r := range remaining {
		waitForUserEntry(t, r, []byte("after-kill"))
	}
}

// TestPreVote_PartitionedFollowerDoesNotDisruptLeader is the regression
// test for the PreVote configuration in db.Raft. It exercises the exact
// scenario from Ongaro's thesis §9.6: a follower is partitioned from the
// cluster, sits long enough that its election timer fires repeatedly,
// then rejoins. With PreVote enabled, the follower's election attempts
// never increment its term (the PreVote round can't get a quorum from
// the empty peer set), so on rejoin its term still matches the leader's
// and the leader is NOT forced to step down. With PreVote disabled, the
// follower's term would have climbed during the partition and on rejoin
// would force the healthy leader to step down via a higher-term reply.
//
// "Partition" is simulated at the transport layer via the test-only
// PartitionPeerForTest helper. Both directions are removed: the follower
// can no longer reach its peers, and its peers can no longer reach it.
// One-sided partition (only removing peers from one side) wouldn't work
// — the receiving side ignores the partition and the test wouldn't
// exercise the Pre/Vote increment-term path at all.
func TestPreVote_PartitionedFollowerDoesNotDisruptLeader(t *testing.T) {
	rafts := createRafts(3)
	defer rafts.Close()
	stopDrainers := rafts.StartDrainers()
	defer stopDrainers()

	originalLeader := rafts.WaitForLeader(t)

	// Pick any follower to partition. The cluster is small enough that
	// the choice doesn't matter; we just need a non-leader.
	follower := rafts.FollowerRaft()
	if follower == nil {
		t.Fatal("FollowerRaft returned nil after WaitForLeader")
	}

	// Partition the follower from the cluster at the transport layer.
	// Both directions: drop the follower from every other node's peer
	// set, and drop every other node from the follower's peer set.
	for _, r := range rafts {
		if r.id == follower.id {
			for _, peer := range follower.peers {
				if peer.ID != follower.id {
					follower.raft.PartitionPeerForTest(peer.ID)
				}
			}
		} else {
			r.raft.PartitionPeerForTest(follower.id)
		}
	}

	// Sit in the partition long enough for many election cycles to
	// elapse. Election timeout is 10 ticks × multiNodeTickInterval
	// (10ms) = 100ms, so 1s allows for ~10 cycles. With PreVote enabled
	// the follower's term should NOT advance during this window;
	// without PreVote it would advance ~10 times.
	time.Sleep(1 * time.Second)

	// Heal the partition by re-adding peers on both sides.
	for _, r := range rafts {
		if r.id == follower.id {
			for _, peer := range follower.peers {
				if peer.ID != follower.id {
					if err := follower.raft.UnpartitionPeerForTest(peer); err != nil {
						t.Fatalf("unpartition follower→%d: %v", peer.ID, err)
					}
				}
			}
		} else {
			for _, peer := range r.peers {
				if peer.ID == follower.id {
					if err := r.raft.UnpartitionPeerForTest(peer); err != nil {
						t.Fatalf("unpartition %d→follower: %v", r.id, err)
					}
				}
			}
		}
	}

	// Allow time for any in-flight RPCs from the rejoining follower to
	// land at the leader. This is the window where, without PreVote, a
	// stale-but-high-term RequestVote would force the leader down.
	time.Sleep(500 * time.Millisecond)

	// Cluster should still report the original leader. With PreVote off
	// this assertion would fail because the rejoining follower would
	// have a higher term than the leader.
	finalLeader := rafts.WaitForLeader(t)
	if finalLeader != originalLeader {
		t.Fatalf("leader changed across partition+heal: was %d, now %d (PreVote should prevent this)",
			originalLeader, finalLeader)
	}

	// Sanity check: cluster is still functional after the partition is
	// healed. Propose one entry and verify every node applies it,
	// including the formerly-partitioned follower (which catches up via
	// normal raft log replication once heartbeats resume).
	proposeAndCheck(t, rafts[0], "after-heal")
	for _, r := range rafts {
		waitForUserEntry(t, r, []byte("after-heal"))
	}
}
