//go:build adversarial

package db_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
)

// zoneCountSyncable is a per-node counting syncable so the test can see
// WHICH node's worker streams a zone-pinned syncable.
type zoneCountSyncable struct {
	mu    sync.Mutex
	count int
}

func (s *zoneCountSyncable) Sync(_ context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range a.Entities {
		if e.Type != nil && !cluster.IsInternal(e.Type.ID) {
			s.count++
		}
	}
	return true, nil
}
func (s *zoneCountSyncable) Close() error { return nil }
func (s *zoneCountSyncable) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

type zoneCountParser struct{ s *zoneCountSyncable }

func (p *zoneCountParser) Parse(_ *cluster.ParsedConfig, _ cluster.DatabaseStorage) (cluster.Syncable, error) {
	return p.s, nil
}

// announceZonesAndLevels deterministically announces, through raft, every
// node's feature level (3 — opens the zone-pinning gate) and its zone
// ("z-<id>"). Proposed directly rather than via the async startup
// goroutines, so the test owns the timing.
func announceZonesAndLevels(t *testing.T, h *multiDBHarness, proposer *db.DB) {
	t.Helper()
	for _, n := range h.nodes {
		ve, err := cluster.NewNodeVersionEntity(n.id, 3)
		require.NoError(t, err)
		ze, err := cluster.NewNodeZoneEntity(n.id, fmt.Sprintf("z-%d", n.id))
		require.NoError(t, err)
		proposeRetryingLost(t, func() error {
			return proposer.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{ve, ze}})
		})
	}
}

// -----------------------------------------------------------------------------
// Scenario: the ticket's core criteria on a real 3-node cluster. A syncable
// pinned to a FOLLOWER's zone is streamed by that follower (its checkpoint
// bumps travel the follower-origin proposal-forwarding path), no other node
// writes, a leader failover does NOT move it, and the owner-routing verb
// guard admits the owner while refusing everyone else.
// -----------------------------------------------------------------------------
func TestAdversarial_ZonePinnedServedByFollowerAcrossFailover(t *testing.T) {
	h, fc := newFaultyMultiDBHarness(t, 3, 50*time.Millisecond, time.Second)
	defer h.Close()

	instances := map[uint64]*zoneCountSyncable{}
	for _, n := range h.nodes {
		inst := &zoneCountSyncable{}
		instances[n.id] = inst
		n.db.AddSyncableParser("zonecount", &zoneCountParser{s: inst})
	}

	h.WaitForLeader(t)
	leaderID := h.stableLeader()
	leader := h.dbByID(leaderID)
	require.NotNil(t, leader)

	announceZonesAndLevels(t, h, leader)

	// Pin to a FOLLOWER's zone.
	var followerID uint64
	for _, n := range h.nodes {
		if n.id != leaderID {
			followerID = n.id
			break
		}
	}
	pinnedZone := fmt.Sprintf("z-%d", followerID)
	const id = "pinned-sync"
	require.Eventually(t, func() bool {
		return leader.ProposeSyncable(testCtx(t), &cluster.Configuration{
			ID:       id,
			MimeType: "text/toml",
			Data:     fmt.Appendf(nil, "[syncable]\ntype = \"zonecount\"\nname = %q\nzone = %q\n", id, pinnedZone),
		}) == nil
	}, 20*time.Second, 100*time.Millisecond, "the pinned config was never admitted (announcements not applied?)")

	seedUserProposals(t, leader, h.nodeByID(leaderID).storage, "evt", []string{"a", "b", "c", "d", "e"})

	// Only the pinned follower streams.
	require.Eventually(t, func() bool {
		return instances[followerID].Count() >= 5
	}, 30*time.Second, 20*time.Millisecond, "the pinned follower never served")
	for nid, inst := range instances {
		if nid != followerID {
			require.Zero(t, inst.Count(), "node %d wrote despite the pin to %s", nid, pinnedZone)
		}
	}
	require.Equal(t, followerID, leader.SyncableOwner(id))

	// The owner guard: the verb passes the routing check ON the owner (next
	// failure is the sink capability — zonecount can't rematerialize) and is
	// refused as not-owner elsewhere.
	require.ErrorIs(t, h.dbByID(followerID).RematerializeSyncable(testCtx(t), id), cluster.ErrNotRematerializable)
	require.ErrorIs(t, leader.RematerializeSyncable(testCtx(t), id), cluster.ErrNotSyncableOwner)

	// Fail the leader over (partition it away — the harness's SIGKILL). The
	// pinned follower must keep serving under the new leader, unmoved.
	beforeFailover := instances[followerID].Count()
	var survivorIDs []uint64
	for _, n := range h.nodes {
		if n.id != leaderID {
			survivorIDs = append(survivorIDs, n.id)
		}
	}
	fc.Partition([]uint64{leaderID}, survivorIDs)

	var newLeaderID uint64
	require.Eventually(t, func() bool {
		l := h.agreedLeaderAmong(survivorIDs)
		if l == 0 || l == leaderID {
			return false
		}
		newLeaderID = l
		return true
	}, 30*time.Second, 20*time.Millisecond, "survivors never elected a new leader")

	newLeader := h.dbByID(newLeaderID)
	seedUserProposals(t, newLeader, h.nodeByID(newLeaderID).storage, "evt2", []string{"f", "g", "h"})

	require.Eventually(t, func() bool {
		return instances[followerID].Count() >= beforeFailover+3
	}, 30*time.Second, 20*time.Millisecond, "the pin moved (or stalled) across the leader failover")
	for nid, inst := range instances {
		if nid != followerID && nid != leaderID {
			require.Zero(t, inst.Count(), "node %d started writing after the failover", nid)
		}
	}
	require.Equal(t, followerID, newLeader.SyncableOwner(id), "the failover must not move the pin")
}

// -----------------------------------------------------------------------------
// Scenario: strict-pin stall on pinned-node death, and complete catch-up on
// return. The pinned node is partitioned away (dead, still a member): NO
// other node may write a single entity — silent leader fallback is the
// failure mode this pins against — and when the node returns it catches up
// to the full head from the replicated checkpoint.
// -----------------------------------------------------------------------------
func TestAdversarial_ZonePinnedStrictStallOnPinnedNodeDeath(t *testing.T) {
	h, fc := newFaultyMultiDBHarness(t, 3, 50*time.Millisecond, time.Second)
	defer h.Close()

	instances := map[uint64]*zoneCountSyncable{}
	for _, n := range h.nodes {
		inst := &zoneCountSyncable{}
		instances[n.id] = inst
		n.db.AddSyncableParser("zonecount", &zoneCountParser{s: inst})
	}

	h.WaitForLeader(t)
	leaderID := h.stableLeader()
	leader := h.dbByID(leaderID)
	announceZonesAndLevels(t, h, leader)

	var followerID uint64
	for _, n := range h.nodes {
		if n.id != leaderID {
			followerID = n.id
			break
		}
	}
	pinnedZone := fmt.Sprintf("z-%d", followerID)
	const id = "stall-sync"
	require.Eventually(t, func() bool {
		return leader.ProposeSyncable(testCtx(t), &cluster.Configuration{
			ID:       id,
			MimeType: "text/toml",
			Data:     fmt.Appendf(nil, "[syncable]\ntype = \"zonecount\"\nname = %q\nzone = %q\n", id, pinnedZone),
		}) == nil
	}, 20*time.Second, 100*time.Millisecond)

	seedUserProposals(t, leader, h.nodeByID(leaderID).storage, "evt", []string{"a", "b"})
	require.Eventually(t, func() bool {
		return instances[followerID].Count() >= 2
	}, 30*time.Second, 20*time.Millisecond, "the pinned follower never served before the death")

	// Kill the pinned node (partition it away; it stays a MEMBER, so the pin
	// still resolves to it — the strict stall, not unsatisfiability).
	var survivorIDs []uint64
	for _, n := range h.nodes {
		if n.id != followerID {
			survivorIDs = append(survivorIDs, n.id)
		}
	}
	fc.Partition([]uint64{followerID}, survivorIDs)
	require.Eventually(t, func() bool {
		l := h.agreedLeaderAmong(survivorIDs)
		return l != 0 && l != followerID
	}, 30*time.Second, 20*time.Millisecond, "survivors lost their leader")

	// Seed while the pinned node is dead.
	postLeader := h.dbByID(h.agreedLeaderAmong(survivorIDs))
	seedUserProposals(t, postLeader, h.nodeByID(h.agreedLeaderAmong(survivorIDs)).storage, "evt2",
		[]string{"c", "d", "e", "f"})

	// STRICT: zero writes from any survivor while the pinned node is dead.
	require.Never(t, func() bool {
		for nid, inst := range instances {
			if nid != followerID && inst.Count() > 0 {
				return true
			}
		}
		return false
	}, 3*time.Second, 100*time.Millisecond,
		"a survivor wrote to the pinned syncable's sink — silent leader fallback, the exact failure strict pins forbid")
	require.Equal(t, followerID, postLeader.SyncableOwner(id), "a dead-but-member pinned node still owns")

	// Return the node: complete catch-up from the replicated checkpoint.
	fc.Heal()
	require.Eventually(t, func() bool {
		return instances[followerID].Count() >= 6
	}, 60*time.Second, 50*time.Millisecond, "the returned pinned node never caught up")
	for nid, inst := range instances {
		if nid != followerID {
			require.Zero(t, inst.Count(), "node %d wrote during or after the stall", nid)
		}
	}
}
