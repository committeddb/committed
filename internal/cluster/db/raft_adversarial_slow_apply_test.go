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
)

// -----------------------------------------------------------------------------
// Scenario: a slow apply must not stop raft's clock.
//
// Invariant protected: while a node is busy applying committed entries, it
// still ticks, so its election timer and leader lease run and it can elect.
// Apply runs off the raft goroutine; before, a Ready's apply ran inline, the
// node did not tick until it finished, and a follower kept refusing votes on
// a lease that never expired (found by the pipelined-ingest flap test: 961
// CDC entries in one Ready held two nodes in apply for seconds and stalled
// the election for 30 s).
//
// Setup: writes flow while both followers take 2 s per apply. Partition the
// leader away: the followers must elect a new leader within 3 s. Inline apply
// ticks at most once per apply, so its election would take about 20 s.
// -----------------------------------------------------------------------------
func TestAdversarial_SlowApplyStillElects(t *testing.T) {
	h, fc := newFaultyMultiDBHarness(t, 3, 50*time.Millisecond, time.Second)
	defer h.Close()

	h.WaitForLeader(t)
	leaderID := h.stableLeader()
	leader := h.dbByID(leaderID)
	seedUserProposals(t, leader, h.nodeByID(leaderID).storage, "slow-evt", nil)
	typ, err := h.nodeByID(leaderID).storage.ResolveType(cluster.LatestTypeRef("slow-evt"))
	require.NoError(t, err)

	var followers []uint64
	for _, n := range h.nodes {
		if n.id != leaderID {
			followers = append(followers, n.id)
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
			}
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			_ = leader.Propose(ctx, &cluster.Proposal{Entities: []*cluster.Entity{
				{Type: typ, Key: fmt.Appendf(nil, "k%d", i), Data: []byte("v")},
			}})
			cancel()
		}
	})
	defer func() {
		close(stop)
		wg.Wait()
	}()

	for _, id := range followers {
		h.nodeByID(id).storage.SetApplyDelayForTest(2 * time.Second)
	}
	defer func() {
		for _, n := range h.nodes {
			n.storage.SetApplyDelayForTest(0)
		}
	}()
	time.Sleep(time.Second) // both followers are now mid-apply

	fc.Partition([]uint64{leaderID}, followers)
	start := time.Now()
	require.Eventually(t, func() bool {
		l := h.agreedLeaderAmong(followers)
		return l != 0 && l != leaderID
	}, 3*time.Second, 10*time.Millisecond, "followers busy applying never elected a new leader")
	t.Logf("new leader elected %s after the partition, with both followers mid-apply", time.Since(start).Round(time.Millisecond))
	fc.Heal()
}
