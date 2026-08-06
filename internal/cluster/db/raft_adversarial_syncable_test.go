//go:build adversarial

package db_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// adoptionSyncable is a per-node counting syncable: shouldSnapshot=true so
// checkpoints flow, and the per-node instance lets the test see WHICH node's
// worker actually did work.
type adoptionSyncable struct {
	mu    sync.Mutex
	count int
}

func (s *adoptionSyncable) Sync(ctx context.Context, _ *cluster.Actual) (cluster.ShouldSnapshot, error) {
	// Slow enough that no worker can finish the seeded backlog inside the
	// crash window — the field shape was a large replay mid-flight.
	select {
	case <-time.After(5 * time.Millisecond):
	case <-ctx.Done():
		return false, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count++
	return true, nil
}
func (s *adoptionSyncable) Close() error { return nil }
func (s *adoptionSyncable) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

type adoptionParser struct{ s *adoptionSyncable }

func (p *adoptionParser) Parse(_ *cluster.ParsedConfig, _ cluster.DatabaseStorage) (cluster.Syncable, error) {
	return p.s, nil
}

// -----------------------------------------------------------------------------
// Scenario: crash-window syncable adoption (multinode field validation,
// 2026-08-06). A syncable created shortly before the leader dies was observed
// in the field as a PHANTOM on the new leader: status derived "running",
// checkpoint frozen at 0, no error, no dead letters — silent until an operator
// re-POSTed the config. This scenario pins the required behavior: the new
// leader must ADOPT the young syncable and make progress (replicated
// checkpoint advances past 0) without operator action.
// -----------------------------------------------------------------------------
func TestAdversarial_CrashWindowSyncableAdoptedByNewLeader(t *testing.T) {
	// 50ms tick → sub-second elections; modest grace so the old leader's
	// blocked bump waiters resolve promptly on the survivors' new term.
	h, fc := newFaultyMultiDBHarness(t, 3, 50*time.Millisecond, time.Second)
	defer h.Close()

	instances := map[uint64]*adoptionSyncable{}
	for _, n := range h.nodes {
		inst := &adoptionSyncable{}
		instances[n.id] = inst
		n.db.AddSyncableParser("adopt", &adoptionParser{s: inst})
	}

	h.WaitForLeader(t)
	leaderID := h.stableLeader()
	leader := h.dbByID(leaderID)
	require.NotNil(t, leader)

	// Work for the syncable to replay: a type and enough rows that the old
	// leader cannot possibly finish them in the crash window.
	payloads := make([]string, 400)
	for i := range payloads {
		payloads[i] = "p"
	}
	seedUserProposals(t, leader, h.nodeByID(leaderID).storage, "evt", payloads)

	// Create the syncable — and kill the leader as soon as the create returns
	// (the crash window: the config is committed, the old leader's worker may
	// have barely started).
	const id = "crash-window-sync"
	require.NoError(t, leader.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID:       id,
		MimeType: "text/toml",
		Data:     []byte("[syncable]\ntype = \"adopt\"\nname = \"" + id + "\""),
	}))
	// "Crash" the leader the only way an in-process harness safely can:
	// PARTITION it away. From the survivors' perspective this is the field's
	// SIGKILL — leadership moves while the young syncable's replay is
	// mid-flight on a now-unreachable owner. (Closing the leader's storage
	// under it panics the shared test process; a real SIGKILL kills one
	// process, not the cluster.)
	var survivorIDs []uint64
	for _, n := range h.nodes {
		if n.id != leaderID {
			survivorIDs = append(survivorIDs, n.id)
		}
	}
	fc.Partition([]uint64{leaderID}, survivorIDs)

	// A new leader among the survivors.
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

	// THE assertion: the young syncable makes progress on the new owner with
	// no operator action. Capture the checkpoint the old leader managed to
	// commit before dying, then require the NEW leader to advance it to the
	// full seeded head — and to have done that work with its OWN instance.
	cp0, _, _ := newLeader.SyncableProgress(id)
	require.Eventually(t, func() bool {
		cp, head, err := newLeader.SyncableProgress(id)
		return err == nil && cp > cp0 && head > 0 && cp >= head
	}, 60*time.Second, 50*time.Millisecond,
		"phantom adoption: the crash-window syncable never progressed on the new leader (checkpoint frozen)")
	require.Positive(t, instances[newLeaderID].Count(),
		"the new leader's own worker instance must have done the syncing")
}
