//go:build adversarial

package db_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

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
	require.Eventually(t, func() bool {
		committed := map[string]bool{}
		var want int
		for n, d := range h.dbs {
			cfgs, err := d.Ingestables()
			if err != nil {
				return false
			}
			if n == 0 {
				want = len(cfgs)
				for _, c := range cfgs {
					committed[c.ID] = true
				}
			} else if len(cfgs) != want {
				return false // replicas still converging on the committed set
			}
		}

		winner := ""
		for _, d := range h.dbs {
			registered := ""
			for i := 0; i < attempts; i++ {
				if !d.HasIngestWorkerForTest(racer(i)) {
					continue
				}
				if registered != "" {
					return false // two producers registered on one node
				}
				registered = racer(i)
			}
			if registered == "" {
				return false // this node hasn't built the winner yet
			}
			if winner == "" {
				winner = registered
			} else if winner != registered {
				return false // nodes disagree on the winner — never acceptable
			}

			refused := map[string]bool{}
			for _, ce := range d.ConfigBuildErrors() {
				if ce.Kind == "ingestable" {
					refused[ce.ID] = true
				}
			}
			if refused[registered] {
				return false
			}
			for id := range committed {
				if id != registered && !refused[id] {
					return false // a committed loser not yet loudly degraded here
				}
			}
		}
		return committed[winner]
	}, 30*time.Second, 50*time.Millisecond,
		"the cluster must converge on exactly one active producer, with every committed loser loudly degraded")
}
