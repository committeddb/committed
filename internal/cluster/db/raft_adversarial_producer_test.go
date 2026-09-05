//go:build adversarial

package db_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
)

// TestAdversarial_RacedIngestableCreatesYieldOneProducer protects the
// single-epoch-stamping-producer invariant under a genuine cross-node race:
// several concurrent creates claim the SAME topic from rotated nodes. The
// leader's admission check catches the sequential-ish ones (400, topic
// taken); any that race past it and commit are decided by the deterministic
// log-index-order replay at build time. Whatever mix of the two paths a run
// produces, the invariant is the same: every node registers exactly ONE
// racer worker — the same winner everywhere (the inner Ingest additionally
// runs on the owner only) — every other committed racer is loudly degraded
// on every node, and every node stores the same committed set. Without the apply-time backstop, two producers would
// interleave refresh-epoch spaces and one's reconciling sweep would silently
// erase the other's rows on every keyed sink downstream.
func TestAdversarial_RacedIngestableCreatesYieldOneProducer(t *testing.T) {
	const replicas = 3
	const attempts = 6

	h := newMultiDBHarness(t, replicas)
	defer h.Close()

	// The topic-reporting fake ingest kind, registered identically on every
	// node (the producer guard reads topics from the config document alone).
	for _, d := range h.dbs {
		d.AddIngestableParser("fake", &topicIngestParser{})
	}
	h.WaitForLeader(t)

	racer := func(i int) string { return fmt.Sprintf("racer-%d", i) }
	var wg sync.WaitGroup
	errs := make([]error, attempts)
	for i := 0; i < attempts; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			cfg := &cluster.Configuration{
				ID: racer(i), MimeType: "text/toml",
				Data: fmt.Appendf(nil, "[ingestable]\nname = %q\ntype = \"fake\"\n[fake]\ntopic = \"hot\"\n", racer(i)),
			}
			// Same 30s budget + leader-churn retry discipline as
			// TestAdversarial_ConcurrentConfigChanges. An admission refusal
			// ("already has a producer") is a FINAL, correct outcome here —
			// only the documented retry-me signals loop.
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			for attempt := 0; attempt < 5; attempt++ {
				errs[i] = h.dbs[i%replicas].ProposeIngestable(ctx, cfg)
				if !errors.Is(errs[i], db.ErrProposalUnknown) &&
					!errors.Is(errs[i], db.ErrProposalLost) {
					return
				}
			}
		}()
	}
	wg.Wait()

	// Every attempt either succeeded or was refused by the producer guard —
	// nothing else is an acceptable outcome once the retry loop drained the
	// leader-churn signals.
	accepted := 0
	for i, err := range errs {
		if err == nil {
			accepted++
			continue
		}
		require.ErrorContains(t, err, "already has a producer",
			"racer %d failed for a reason other than the producer guard", i)
	}
	require.GreaterOrEqual(t, accepted, 1, "at least one racer must win")

	// The invariant, per node and cluster-wide: every node registers exactly
	// ONE racer worker (the registry entry exists on every node — idle on
	// non-owners — so registration IS the "would produce" state), the SAME
	// winner everywhere; every other committed racer is degraded by the
	// guard on every node; and all nodes store the same committed set
	// (consensus did its half). Poll to convergence — build listeners lag
	// apply by design.
	//
	// lastUnmet names the condition still failing at timeout so a starvation
	// flake and an invariant violation read differently. (Convergence needs a
	// functioning leader — followers learn the commit index from it — and a
	// starved CI runner has been seen to lose quorum for ~20s straight after
	// the proposals resolved: a CheckQuorum step-down and a dozen failed
	// elections at a 200ms election timeout, 2026-09-05, shard 1/4. The
	// budget stays at 30s on purpose: widening it would hide the next flake
	// that is actually fixable.)
	var lastUnmet string
	converged := func() bool {
		committed := map[string]bool{}
		var want int
		for n, d := range h.dbs {
			cfgs, err := d.Ingestables()
			if err != nil {
				lastUnmet = fmt.Sprintf("node %d: Ingestables(): %v", n+1, err)
				return false
			}
			if n == 0 {
				want = len(cfgs)
				for _, c := range cfgs {
					committed[c.ID] = true
				}
			} else if len(cfgs) != want {
				lastUnmet = fmt.Sprintf("node %d stores %d ingestables, node 1 stores %d (replicas still converging on the committed set)", n+1, len(cfgs), want)
				return false
			}
		}
		winner := ""
		for n, d := range h.dbs {
			registered := ""
			for i := 0; i < attempts; i++ {
				if !d.HasIngestWorkerForTest(racer(i)) {
					continue
				}
				if registered != "" {
					lastUnmet = fmt.Sprintf("node %d registered two producers: %s and %s", n+1, registered, racer(i))
					return false
				}
				registered = racer(i)
			}
			if registered == "" {
				lastUnmet = fmt.Sprintf("node %d has not built the winner yet", n+1)
				return false
			}
			if winner == "" {
				winner = registered
			} else if winner != registered {
				lastUnmet = fmt.Sprintf("nodes disagree on the winner: node 1 registered %s, node %d registered %s", winner, n+1, registered)
				return false
			}
			refused := map[string]bool{}
			for _, ce := range d.ConfigBuildErrors() {
				if ce.Kind == "ingestable" {
					refused[ce.ID] = true
				}
			}
			if refused[registered] {
				lastUnmet = fmt.Sprintf("node %d registered %s but also reports it refused", n+1, registered)
				return false
			}
			for id := range committed {
				if id != registered && !refused[id] {
					lastUnmet = fmt.Sprintf("node %d: committed loser %s not yet loudly degraded", n+1, id)
					return false
				}
			}
		}
		if !committed[winner] {
			lastUnmet = fmt.Sprintf("winner %s is not in node 1's committed set", winner)
			return false
		}
		return true
	}
	require.Truef(t, assert.Eventually(t, converged, 30*time.Second, 50*time.Millisecond),
		"the cluster must converge on exactly one active producer, with every committed loser loudly degraded — last unmet: %s", lastUnmet)
}
