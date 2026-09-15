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
	"github.com/committeddb/committed/internal/cluster/syncable/loopback"
)

// keyRecorder is a per-node consumer of one topic that counts how often it
// saw each key — the observable for "every derived row arrived, and a row
// derived after the failover arrived exactly once".
type keyRecorder struct {
	mu    sync.Mutex
	topic string
	seen  map[string]int
}

func (r *keyRecorder) Sync(_ context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range a.Entities {
		if e.Type != nil && e.Type.ID == r.topic && e.Variant() == cluster.EntityVariantRow {
			if r.seen == nil {
				r.seen = map[string]int{}
			}
			r.seen[string(e.Key)]++
		}
	}
	return true, nil
}
func (r *keyRecorder) Close() error { return nil }
func (r *keyRecorder) counts() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]int, len(r.seen))
	for k, n := range r.seen {
		out[k] = n
	}
	return out
}

func allIDs(h *multiDBHarness) []uint64 {
	ids := make([]uint64, 0, len(h.nodes))
	for _, n := range h.nodes {
		ids = append(ids, n.id)
	}
	return ids
}

type keyRecorderParser struct{ r *keyRecorder }

func (p *keyRecorderParser) Parse(_ *cluster.ParsedConfig, _ cluster.DatabaseStorage) (cluster.Syncable, error) {
	return p.r, nil
}

// -----------------------------------------------------------------------------
// Scenario: a loopback derivation survives a leader change. The loopback
// (leader-owned) derives raw rows into a derived topic; a consumer of the
// derived topic runs on the leader too. The leader is partitioned away, the
// survivors elect a new one, and rows proposed afterwards are derived and
// consumed there — from the replicated checkpoints, so nothing proposed
// before the failover is lost and nothing proposed after it is derived twice.
// -----------------------------------------------------------------------------
func TestAdversarial_LoopbackDerivationSurvivesLeaderChange(t *testing.T) {
	h, fc := newFaultyMultiDBHarness(t, 3, 50*time.Millisecond, time.Second)
	defer h.Close()

	consumers := map[uint64]*keyRecorder{}
	audits := map[uint64]*keyRecorder{}
	for _, n := range h.nodes {
		n.db.AddSyncableParser("loopback", &loopback.SyncableParser{Proposer: n.db})
		consumers[n.id] = &keyRecorder{topic: "derived"}
		n.db.AddSyncableParser("consumer", &keyRecorderParser{r: consumers[n.id]})
		audits[n.id] = &keyRecorder{topic: "derived"}
		n.db.AddSyncableParser("audit", &keyRecorderParser{r: audits[n.id]})
	}

	h.WaitForLeader(t)
	leaderID := h.stableLeader()
	leader := h.dbByID(leaderID)
	require.NotNil(t, leader)

	proposeType := func(d *db.DB, id string) {
		require.Eventually(t, func() bool {
			return d.ProposeType(testCtx(t), &cluster.Configuration{
				ID: id, MimeType: "text/toml",
				Data: fmt.Appendf(nil, "[type]\nname = %q\nentityKind = \"snapshot\"\n", id),
			}) == nil
		}, 20*time.Second, 100*time.Millisecond, "type %s never admitted", id)
	}
	proposeType(leader, "raw")
	proposeType(leader, "derived")
	require.Eventually(t, func() bool {
		return proposeLoopback(t, leader, "canonizer", "raw", "derived", "") == nil
	}, 20*time.Second, 100*time.Millisecond, "the loopback was never admitted")
	require.Eventually(t, func() bool {
		return leader.ProposeSyncable(testCtx(t), &cluster.Configuration{
			ID: "derived-consumer", MimeType: "text/toml",
			Data: []byte("[syncable]\nname = \"derived-consumer\"\ntype = \"consumer\"\n"),
		}) == nil
	}, 20*time.Second, 100*time.Millisecond, "the consumer was never admitted")

	seed := func(d *db.DB, keys []string) {
		rawType, err := h.nodeByID(h.agreedLeaderAmong(allIDs(h))).storage.ResolveType(cluster.LatestTypeRef("raw"))
		require.NoError(t, err)
		for _, k := range keys {
			proposeRetryingLost(t, func() error {
				return d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{
					cluster.NewUpsertEntity(rawType, []byte(k), []byte(fmt.Sprintf(`{"id":%q}`, k))),
				}})
			})
		}
	}
	before := []string{"k1", "k2", "k3", "k4", "k5"}
	seed(leader, before)
	require.Eventually(t, func() bool {
		return len(consumers[leaderID].counts()) >= len(before)
	}, 30*time.Second, 20*time.Millisecond, "derived rows never reached the leader's consumer")

	// Partition the leader away; the survivors elect a new one, which takes
	// over both workers from the replicated checkpoints.
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

	after := []string{"k6", "k7", "k8", "k9", "k10"}
	seedAfter := func() {
		rawType, err := h.nodeByID(newLeaderID).storage.ResolveType(cluster.LatestTypeRef("raw"))
		require.NoError(t, err)
		for _, k := range after {
			proposeRetryingLost(t, func() error {
				return newLeader.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{
					cluster.NewUpsertEntity(rawType, []byte(k), []byte(fmt.Sprintf(`{"id":%q}`, k))),
				}})
			})
		}
	}
	seedAfter()
	require.Eventually(t, func() bool {
		c := consumers[newLeaderID].counts()
		for _, k := range after {
			if c[k] == 0 {
				return false
			}
		}
		return true
	}, 30*time.Second, 20*time.Millisecond, "rows proposed after the failover never reached the new leader's consumer: %v", consumers[newLeaderID].counts())

	// Heal, then audit the derived topic from index 0 with a fresh consumer:
	// every key was derived, and the post-failover keys exactly once.
	fc.Heal()
	require.Eventually(t, func() bool { return h.agreedLeaderAmong(allIDs(h)) != 0 },
		30*time.Second, 20*time.Millisecond, "the cluster never re-agreed on a leader after healing")
	auditLeaderID := h.agreedLeaderAmong(allIDs(h))
	require.Eventually(t, func() bool {
		return h.dbByID(auditLeaderID).ProposeSyncable(testCtx(t), &cluster.Configuration{
			ID: "derived-audit", MimeType: "text/toml",
			Data: []byte("[syncable]\nname = \"derived-audit\"\ntype = \"audit\"\n"),
		}) == nil
	}, 20*time.Second, 100*time.Millisecond, "the audit consumer was never admitted")
	require.Eventually(t, func() bool {
		return len(audits[h.agreedLeaderAmong(allIDs(h))].counts()) >= len(before)+len(after)
	}, 30*time.Second, 20*time.Millisecond, "the audit never saw every derived key: %v", audits[auditLeaderID].counts())
	counts := audits[h.agreedLeaderAmong(allIDs(h))].counts()
	for _, k := range append(append([]string{}, before...), after...) {
		require.GreaterOrEqual(t, counts[k], 1, "key %s was never derived", k)
	}
	for _, k := range after {
		require.Equal(t, 1, counts[k], "key %s, proposed after the failover, must be derived exactly once", k)
	}
}
