// Package ingesttest holds the first-class test primitives for driving a
// cluster.Ingestable's output channels. Dialect-level tests previously
// hand-rolled the same select loop at a dozen sites; when the checkpoint
// contract evolved (a commit checkpoint now rides the transaction's final
// proposal — see the cluster.Ingestable contract), every copy broke and had
// to be patched separately. This package is that loop, once.
package ingesttest

import (
	"testing"
	"time"

	"github.com/committeddb/committed/internal/cluster"
)

// Result is what Await observed: every proposal received (in order), the
// set of entity keys seen across them, and the checkpoint that satisfied
// the wait.
type Result struct {
	Proposals []*cluster.Proposal
	Seen      map[string]bool
	Position  cluster.Position
}

// Await drives an Ingestable's two output channels until every key in
// wantKeys has been seen on a proposal AND wantPos accepts a checkpoint,
// then returns what it observed. Per the cluster.Ingestable contract, a
// commit checkpoint travels WITH the data it covers — the transaction's
// final proposal carries it (Proposal.Position) — and the position channel
// carries only checkpoints with no proposal to ride (an empty-flush commit,
// snapshot progress); Await therefore accepts the checkpoint from EITHER
// source.
//
// Checkpoints are considered only once every wantKey has been seen, so the
// returned Position is never a stale pre-data checkpoint: the first
// acceptable one is the bundle on the completing proposal itself, a bundle
// on a later proposal, or a later channel checkpoint. "Seen" means received
// here: with buffered channels a checkpoint can be received before the
// proposal the dialect emitted ahead of it and be discarded as pre-key —
// safe (never a stale Position), and a live stream's next checkpoint
// completes the wait. A nil wantPos means "no checkpoint required": the
// wait ends when the keys are seen, and Position best-effort carries the
// last checkpoint observed after that point (possibly nil).
//
// On timeout it fails the test naming exactly what is still missing.
func Await(t *testing.T, pr <-chan *cluster.Proposal, po <-chan cluster.Position,
	timeout time.Duration, wantPos func(cluster.Position) bool, wantKeys ...string,
) Result {
	t.Helper()

	res := Result{Seen: map[string]bool{}}
	keysDone := func() bool {
		for _, k := range wantKeys {
			if !res.Seen[k] {
				return false
			}
		}
		return true
	}
	posDone := false
	consider := func(pos cluster.Position) {
		if len(pos) == 0 || !keysDone() {
			return
		}
		if wantPos == nil {
			res.Position = pos
			return
		}
		if !posDone && wantPos(pos) {
			res.Position = pos
			posDone = true
		}
	}

	deadline := time.After(timeout)
	for !keysDone() || (wantPos != nil && !posDone) {
		select {
		case p := <-pr:
			res.Proposals = append(res.Proposals, p)
			for _, e := range p.Entities {
				res.Seen[string(e.Key)] = true
			}
			consider(p.Position)
		case pos := <-po:
			consider(pos)
		case <-deadline:
			missing := []string{}
			for _, k := range wantKeys {
				if !res.Seen[k] {
					missing = append(missing, k)
				}
			}
			t.Fatalf("ingesttest.Await timed out after %s: missing keys %v, checkpoint satisfied=%v (proposals seen: %d)",
				timeout, missing, posDone || wantPos == nil, len(res.Proposals))
		}
	}
	return res
}
