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

// keyRecorder models a shared destination for one logical consumer across nodes.
// Replaying an Actual after a checkpoint/leadership change is idempotent; the
// same key in distinct Actuals still counts twice, exposing duplicate derivation.
type keyRecorder struct {
	mu      sync.Mutex
	topic   string
	seen    map[string]int
	actuals map[uint64]struct{}
}

func (r *keyRecorder) Sync(_ context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.actuals[a.Index]; ok {
		return true, nil
	}
	if r.actuals == nil {
		r.actuals = make(map[uint64]struct{})
	}
	r.actuals[a.Index] = struct{}{}
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

// The destination follows worker ownership across nodes. A checkpoint replay
// must not look like a second derivation, but duplicate log rows must remain visible.
func TestKeyRecorderOwnershipHandoff(t *testing.T) {
	destination := &keyRecorder{topic: "derived"}
	first, err := (&keyRecorderParser{r: destination}).Parse(nil, nil)
	require.NoError(t, err)
	second, err := (&keyRecorderParser{r: destination}).Parse(nil, nil)
	require.NoError(t, err)
	row := func(key string) *cluster.Entity {
		return cluster.NewUpsertEntity(&cluster.Type{ID: "derived"}, []byte(key), []byte(`{}`))
	}
	deliver := func(worker cluster.Syncable, index uint64, rows ...*cluster.Entity) {
		t.Helper()
		_, err := worker.Sync(context.Background(), &cluster.Actual{Index: index, Entities: rows})
		require.NoError(t, err)
	}
	deliver(first, 10, row("before"))
	require.NoError(t, first.Close())
	deliver(second, 10, row("before")) // replay after an uncheckpointed delivery
	deliver(second, 20, row("after"))
	require.Equal(t, map[string]int{"before": 1, "after": 1}, destination.counts())
	deliver(second, 30, row("after")) // a second derivation has a distinct index
	deliver(second, 40, row("batch"), row("batch"))
	require.Equal(t, map[string]int{"before": 1, "after": 2, "batch": 2}, destination.counts())
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

	consumer := &keyRecorder{topic: "derived"}
	audit := &keyRecorder{topic: "derived"}
	for _, n := range h.nodes {
		n.db.AddSyncableParser("loopback", &loopback.SyncableParser{Proposer: n.db})
		n.db.AddSyncableParser("consumer", &keyRecorderParser{r: consumer})
		n.db.AddSyncableParser("audit", &keyRecorderParser{r: audit})
	}

	h.WaitForLeader(t)
	var leaderID uint64
	require.Eventually(t, func() bool {
		leaderID = h.stableLeader()
		return leaderID != 0
	}, 30*time.Second, 20*time.Millisecond)
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
		rawType, err := h.nodeByID(d.ID()).storage.ResolveType(cluster.LatestTypeRef("raw"))
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
		return len(consumer.counts()) >= len(before)
	}, 30*time.Second, 20*time.Millisecond, "derived rows never reached the shared consumer destination")

	// Partition the leader away; the survivors elect a new one, which takes
	// over both workers from the replicated checkpoints.
	require.Eventually(t, func() bool {
		leaderID = h.agreedLeaderAmong(allIDs(h))
		return leaderID != 0
	}, 30*time.Second, 20*time.Millisecond, "no agreed leader before partition")
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
		c := consumer.counts()
		for _, k := range after {
			if c[k] == 0 {
				return false
			}
		}
		return true
	}, 30*time.Second, 20*time.Millisecond, "rows proposed after the failover never reached the new leader's consumer: %v", consumer.counts())

	// Heal, then audit the derived topic from index 0 with a fresh consumer:
	// every key was derived, and the post-failover keys exactly once.
	fc.Heal()
	var auditLeaderID uint64
	require.Eventually(t, func() bool {
		auditLeaderID = h.agreedLeaderAmong(allIDs(h))
		return auditLeaderID != 0
	},
		30*time.Second, 20*time.Millisecond, "the cluster never re-agreed on a leader after healing")
	require.Eventually(t, func() bool {
		return h.dbByID(auditLeaderID).ProposeSyncable(testCtx(t), &cluster.Configuration{
			ID: "derived-audit", MimeType: "text/toml",
			Data: []byte("[syncable]\nname = \"derived-audit\"\ntype = \"audit\"\n"),
		}) == nil
	}, 20*time.Second, 100*time.Millisecond, "the audit consumer was never admitted")
	require.Eventually(t, func() bool {
		return len(audit.counts()) >= len(before)+len(after)
	}, 30*time.Second, 20*time.Millisecond, "the audit never saw every derived key: %v", audit.counts())
	counts := audit.counts()
	for _, k := range append(append([]string{}, before...), after...) {
		require.GreaterOrEqual(t, counts[k], 1, "key %s was never derived", k)
	}
	for _, k := range after {
		require.Equal(t, 1, counts[k], "key %s, proposed after the failover, must be derived exactly once", k)
	}
}
