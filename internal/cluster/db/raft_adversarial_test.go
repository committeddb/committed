//go:build adversarial

// Multi-node adversarial raft tests. Tagged `adversarial` (not `integration`)
// so they don't run in either `make test` or `make test-all` — see
// `make test/adversarial`. These are phase 1 of the adversarial suite:
// partition + flap + concurrent config changes. Phase 2 (latency,
// directional drop, drop-rate) extends FaultyTransport in place.
//
// Every scenario's test comment calls out the invariant being protected.
// If a scenario starts failing, the comment is the load-bearing part of the
// fix discussion — it tells you what regressed, not just that something
// broke.

package db_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	"github.com/philborlin/committed/internal/cluster/db"
	"github.com/philborlin/committed/internal/cluster/db/parser"
	"github.com/philborlin/committed/internal/cluster/db/wal"
	tidwallwal "github.com/tidwall/wal"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// adversarialSettleTime is how long we give raft to process a state change
// (partition, heal, kill, restart) before asserting on the post-state. Two
// election timeouts (~200ms with our 10ms tick * 10-tick election) is the
// minimum; we use 400ms so transient thrash windows don't leak into the
// assertion phase.
const adversarialSettleTime = 400 * time.Millisecond

// -----------------------------------------------------------------------------
// Scenario (a): symmetric partition
//
// Invariants protected:
//
//   - Safety during partition: no node in the minority may advance its
//     commit index while the partition is in effect. Without quorum, a
//     minority-side leader (if any) cannot commit; a minority-side
//     follower forwards proposes to the (unreachable) leader, which
//     never commits them either.
//
//   - Safety across partition: no two nodes ever have different
//     committed entries at the same log index. Post-heal, this
//     invariant manifests as "all 5 nodes' committed log prefixes are
//     byte-identical through some common commit index."
//
//   - Liveness during partition: the majority (3 of 5) keeps accepting
//     and committing proposes, because it retains quorum.
//
//   - Liveness after heal: the minority catches up to the majority's
//     committed log via routine raft replication, and the full cluster
//     eventually agrees on a single committed log.
//
// Note what we do NOT assert: we do not claim the minority-side
// proposal ("minority-stuck") fails to commit post-heal. The partition
// blocks message DELIVERY between groups; once healed, any MsgProp that
// a minority follower queued for its leader (via raft forwarding)
// finally reaches the leader and can be committed by the now-reunited
// majority. That's not a safety violation — it's a delayed proposal,
// and raft makes no promises about dropping it. The test only asserts
// that WHATEVER the post-heal committed log looks like, it is the same
// on all 5 nodes.
// -----------------------------------------------------------------------------
func TestAdversarial_SymmetricPartition(t *testing.T) {
	// Seeded RNG. Unused in this scenario today, but reserved so adding
	// randomness later (e.g., random propose order) doesn't introduce a
	// reproducibility regression. Same pattern used in scenarios (c) and
	// (g) below so failure triage is uniform across the suite.
	_ = rand.New(rand.NewSource(1))

	rafts, cluster := createFaultyRafts(5)

	// Drainer-then-Close defer ordering: Go runs defers LIFO, so we
	// register drainer-stop FIRST (runs LAST) and rafts.Close SECOND
	// (runs FIRST). rafts.Close closes each commitC, which makes the
	// drainers exit via the `!ok` path; stopping drainers before Close
	// would leave serveChannels blocked on a commitC send with no reader
	// and deadlock Close waiting on serveChannelsDoneC.
	stopDrainers := rafts.StartDrainers()
	defer stopDrainers()
	defer rafts.Close()

	rafts.WaitForLeader(t)

	// Pre-partition baseline: every node has the same log and the same
	// committed index.
	prePartitionEntries := []string{"pre-1", "pre-2", "pre-3"}
	for _, e := range prePartitionEntries {
		proposeAndCheck(t, rafts[0], e)
	}
	for _, r := range rafts {
		waitForUserEntry(t, r, []byte("pre-3"))
	}

	// Split the cluster 2/3. Node ids are 1..5 by construction in
	// createRafts; minority=[1,2], majority=[3,4,5]. The majority retains
	// quorum (3/5) and keeps accepting writes; the minority cannot form
	// quorum on its own.
	minorityIDs := []uint64{1, 2}
	majorityIDs := []uint64{3, 4, 5}

	// Split the Rafts slice by id for per-side assertions. Same ids so
	// the two slices are disjoint and their union is the whole cluster.
	minorityRafts := raftsByIDs(rafts, minorityIDs)
	majorityRafts := raftsByIDs(rafts, majorityIDs)

	// Record per-minority-node commit indexes immediately before the
	// partition. Post-partition, no new commits may advance these on the
	// minority side — that's the safety invariant.
	minorityCommitBefore := map[uint64]uint64{}
	for _, r := range minorityRafts {
		minorityCommitBefore[r.id] = r.raft.CommitIndexForTest()
	}

	cluster.Partition(minorityIDs, majorityIDs)

	// Give the partition time to settle: the majority may need to
	// re-elect if the old leader ended up in the minority, and the
	// minority needs to observe its peer drops.
	time.Sleep(adversarialSettleTime)

	// Majority must have a stable leader that's in the majority slice.
	// `WaitForLeader` uses stableLeader, which requires the elected
	// leader id to be one of the slice members — so this correctly
	// fails if the majority is still pointing at an old minority
	// leader that they can no longer reach.
	majorityRafts.WaitForLeader(t)

	// Liveness on the majority side: a fresh propose commits on all
	// three majority nodes.
	proposeAndCheck(t, majorityRafts[0], "majority-during")
	for _, r := range majorityRafts {
		waitForUserEntry(t, r, []byte("majority-during"))
	}

	// Minority-side propose: send one, bounded so the test surfaces a
	// hang as a real failure instead of the Go test timeout.
	//
	// A minority follower's Propose is forwarded as MsgProp toward the
	// (partitioned-away) leader via the outgoing transport; with the
	// peer removed, the send is dropped and the message never reaches
	// the leader while the partition is in effect. Under some
	// interleavings the MsgProp sits in a send queue and gets delivered
	// on Heal, committing after the fact — that's fine for safety, so
	// we don't assert on the fate of this specific payload.
	select {
	case minorityRafts[0].proposeC <- []byte("minority-stuck"):
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("minority proposeC blocked — raft should accept MsgProp into its state machine even without quorum")
	}

	// Let the minority-stuck proposal sit long enough that if it were
	// going to commit on the minority side (it can't — no quorum), it
	// would have by now.
	time.Sleep(adversarialSettleTime)

	// Safety invariant: commit index on the minority side has NOT
	// advanced. etcd raft only advances Commit when an AppendEntries
	// reply from a quorum of peers reports the entry persisted; with
	// only 2 of 5 nodes reachable on the minority side, no entry can
	// achieve quorum.
	for _, r := range minorityRafts {
		got := r.raft.CommitIndexForTest()
		before := minorityCommitBefore[r.id]
		if got != before {
			t.Fatalf("minority node %d: commit index advanced during partition (before=%d, now=%d) — "+
				"safety violated, minority achieved quorum-less commit?",
				r.id, before, got)
		}
	}

	// Liveness invariant on the majority: commit index has advanced
	// past the pre-partition baseline. With "majority-during" committed
	// on all three majority nodes (verified above via waitForUserEntry),
	// the majority's Commit must be > the minority's frozen baseline.
	majorityBaseline := minorityCommitBefore[minorityIDs[0]]
	for _, r := range majorityRafts {
		got := r.raft.CommitIndexForTest()
		if got <= majorityBaseline {
			t.Fatalf("majority node %d: commit index %d did not advance past pre-partition baseline %d — "+
				"liveness violated, majority did not commit during partition",
				r.id, got, majorityBaseline)
		}
	}

	cluster.Heal()

	// Post-heal convergence: every node's commit index catches up to
	// the highest majority-side commit index. Once all 5 are at or
	// above the target commit, the committed-log-prefix identity check
	// below can run meaningfully (at a stable snapshot).
	targetCommit := uint64(0)
	for _, r := range majorityRafts {
		if c := r.raft.CommitIndexForTest(); c > targetCommit {
			targetCommit = c
		}
	}
	convergeDeadline := time.Now().Add(10 * time.Second)
	for _, r := range rafts {
		for r.raft.CommitIndexForTest() < targetCommit {
			if time.Now().After(convergeDeadline) {
				t.Fatalf("node %d: commit index %d never caught up to majority %d — "+
					"liveness violated: post-heal replication did not converge",
					r.id, r.raft.CommitIndexForTest(), targetCommit)
			}
			time.Sleep(25 * time.Millisecond)
		}
	}

	// Cross-node safety: every node's committed log prefix is
	// byte-identical through the lowest commit index observed across
	// the cluster. If any two nodes have different entries at the same
	// committed index, raft safety has been violated.
	//
	// We use the minimum commit index across all nodes as the common
	// prefix length — a node can have extra committed entries beyond
	// that (if heartbeats haven't caught it up quite yet for a later
	// entry), but the shared prefix must match byte-for-byte.
	minCommit := uint64(^uint64(0))
	for _, r := range rafts {
		if c := r.raft.CommitIndexForTest(); c < minCommit {
			minCommit = c
		}
	}

	// Collect each node's committed user entries (index ≤ minCommit) so
	// we can compare. r.ents() filters EntryNormal with non-nil Data,
	// which is the user-visible log; empty-leader-entry artifacts from
	// term starts don't carry data and are correctly skipped.
	committedPerNode := map[uint64][]string{}
	for _, r := range rafts {
		es, err := r.ents()
		if err != nil {
			t.Fatal(err)
		}
		var committed []string
		for _, e := range es {
			if e.Index <= minCommit {
				committed = append(committed, string(e.Data))
			}
		}
		committedPerNode[r.id] = committed
	}

	// Pick the first node as reference and assert every other node's
	// committed-user-entry list equals it exactly.
	referenceID := rafts[0].id
	reference := committedPerNode[referenceID]
	for id, got := range committedPerNode {
		if id == referenceID {
			continue
		}
		if len(got) != len(reference) {
			t.Fatalf("node %d has %d committed user entries, node %d has %d — "+
				"cross-node safety violated (node %d: %v, node %d: %v)",
				id, len(got), referenceID, len(reference), id, got, referenceID, reference)
		}
		for i := range got {
			if got[i] != reference[i] {
				t.Fatalf("node %d and node %d disagree at committed index offset %d: "+
					"%q vs %q — cross-node safety violated",
					id, referenceID, i, got[i], reference[i])
			}
		}
	}

	// Post-heal sanity: the converged committed log includes every
	// pre-partition entry AND the majority-during entry. The minority-
	// stuck entry may or may not appear (its MsgProp might have been
	// delivered to the leader post-heal and committed, or it might have
	// been dropped entirely) — either outcome is safe.
	committedSet := map[string]bool{}
	for _, s := range reference {
		committedSet[s] = true
	}
	for _, want := range append(prePartitionEntries, "majority-during") {
		if !committedSet[want] {
			t.Fatalf("post-heal committed log missing %q — liveness violated (log: %v)",
				want, reference)
		}
	}
}


// raftsByIDs returns the subset of rafts whose ids are in the given list,
// preserving rafts' original order. Used by the partition scenario to
// address the minority / majority groups without pre-sorting by id.
func raftsByIDs(rafts Rafts, ids []uint64) Rafts {
	want := make(map[uint64]bool, len(ids))
	for _, id := range ids {
		want[id] = true
	}
	var out Rafts
	for _, r := range rafts {
		if want[r.id] {
			out = append(out, r)
		}
	}
	return out
}

// -----------------------------------------------------------------------------
// Scenario (c): leader flapping
//
// Invariant protected: exactly-once apply across leader transitions.
//
// A continuous-proposer goroutine sends unique payloads while the test
// kills and restarts the leader repeatedly. Every payload whose propose
// "acked" (was accepted by serveChannels AND subsequently observed in
// storage within its deadline) must appear in every node's log EXACTLY
// ONCE — not duplicated across leader transitions, not missing from any
// node, not reordered into a different index on different nodes.
// -----------------------------------------------------------------------------
func TestAdversarial_LeaderFlapping(t *testing.T) {
	// Seeded RNG used to pick which surviving node we propose against
	// on each iteration. Constant seed → reproducible if something
	// flakes. Parallel test runs don't interfere (rng is test-local).
	rng := rand.New(rand.NewSource(42))

	rafts := createRafts(3)

	// Per-node drainers — we swap them across Restart, same pattern as
	// TestRaftRestart. A test-wide StartDrainers holds a stale commitC
	// for the restarted node and silently stops draining, which
	// deadlocks the new Ready loop on its first commit.
	//
	// Defer order matters: rafts.Close must run BEFORE the drainer stop
	// so that db.Raft.Close's commitC-close naturally drains any
	// in-flight processCommittedEntry send. Stopping drainers first
	// would leave serveChannels blocked on commitC with no reader, and
	// Close would deadlock waiting on serveChannelsDoneC. Register the
	// drainer-stop defer FIRST so Go's LIFO defer order runs it LAST.
	drainers := make([]func(), len(rafts))
	for i, r := range rafts {
		drainers[i] = r.startDrainer()
	}
	defer func() {
		// Runs last. By this point rafts.Close has already closed each
		// commitC, so each drainer has already exited via !ok. Calling
		// the stop functions here is a no-op but keeps the cleanup
		// shape uniform with TestRaftRestart / the flap loop.
		for _, d := range drainers {
			if d != nil {
				d()
			}
		}
	}()
	defer rafts.Close()

	rafts.WaitForLeader(t)

	// Shared state between the proposer goroutine and the main (flap)
	// goroutine. acked is the slice of payloads whose propose returned
	// without error AND whose commit was observed on the proposing
	// node's storage within the deadline.
	var (
		mu       sync.Mutex
		acked    []string
		proposed atomic.Uint64

		// stopC signals the proposer to exit. Closed by the main
		// goroutine after the flap window completes.
		stopC = make(chan struct{})
	)

	// aliveNodes returns the Rafts that the flap loop hasn't killed
	// for the current iteration. The proposer reads this each loop to
	// avoid sending on a dead node's proposeC (which would block
	// forever — the serveChannels reader is gone).
	var aliveMu sync.Mutex
	alive := make(map[uint64]*Raft, len(rafts))
	for _, r := range rafts {
		alive[r.id] = r
	}
	getAlive := func() []*Raft {
		aliveMu.Lock()
		defer aliveMu.Unlock()
		out := make([]*Raft, 0, len(alive))
		for _, r := range alive {
			out = append(out, r)
		}
		return out
	}
	markDead := func(id uint64) {
		aliveMu.Lock()
		defer aliveMu.Unlock()
		delete(alive, id)
	}
	markAlive := func(r *Raft) {
		aliveMu.Lock()
		defer aliveMu.Unlock()
		alive[r.id] = r
	}

	// Proposer goroutine. Picks a random alive node, sends a unique
	// payload via proposeC (bounded), then polls for that payload on
	// the same node's storage (bounded). Both bounds must succeed for
	// the payload to count as "acked".
	var proposerWG sync.WaitGroup
	proposerWG.Add(1)
	go func() {
		defer proposerWG.Done()
		for {
			select {
			case <-stopC:
				return
			default:
			}

			nodes := getAlive()
			if len(nodes) == 0 {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			pick := nodes[rng.Intn(len(nodes))]
			seq := proposed.Add(1)
			payload := fmt.Sprintf("flap-%d", seq)

			ok := tryProposeAndVerify(pick, []byte(payload), 500*time.Millisecond, stopC)
			if ok {
				mu.Lock()
				acked = append(acked, payload)
				mu.Unlock()
			}
			// If not ok, we drop this payload — it either blocked on
			// a mid-kill proposeC, or never reached commit before
			// its deadline. The payload isn't in `acked`, so the
			// post-flap invariant check won't require it.
		}
	}()

	// Flap loop. Fixed iteration count (instead of wall-clock) so CI
	// runtime is predictable under -count=20. The ticket suggests 30s,
	// which is too long for -count=20; 3 iterations exercises multiple
	// re-elections and a full kill-restart cycle per iteration, which
	// is enough to catch regressions in the invariants while keeping
	// total test time reasonable.
	//
	// Between iterations we sleep interFlapRest to give the proposer
	// goroutine a stable window to land successful proposes — without
	// the rest period the cluster is in constant re-election churn
	// (~100ms per election) and the proposer never accumulates an
	// acked set. With the rest, each flap cycle includes one stable
	// leader window of ~interFlapRest during which commits complete.
	const flapIterations = 3
	const interFlapRest = 600 * time.Millisecond
	for i := 0; i < flapIterations; i++ {
		// Identify current leader. If re-election is still in flight,
		// WaitForLeader blocks until it settles.
		rafts.WaitForLeader(t)
		leader := rafts.LeaderRaft()
		if leader == nil {
			t.Fatalf("flap iteration %d: no stable leader after WaitForLeader", i)
		}

		// Find the leader's index in the rafts slice — we need this to
		// swap its drainer across the restart cycle.
		leaderIdx := -1
		for idx, r := range rafts {
			if r.id == leader.id {
				leaderIdx = idx
				break
			}
		}
		if leaderIdx < 0 {
			t.Fatalf("flap iteration %d: leader id %d not in rafts slice", i, leader.id)
		}

		// DO NOT stop the drainer before Close — if we do, and
		// serveChannels is mid-Ready with a pending processCommittedEntry
		// send on commitC, the send blocks with no reader and Close
		// deadlocks waiting on serveChannelsDoneC. Under the flap test's
		// continuous-propose workload there's almost always a commit in
		// flight when Close is called. Instead, leave the drainer running
		// throughout Close; db.Raft.Close closes commitC after
		// serveChannels exits, which signals the drainer to return via
		// the `!ok` path of its select. We mark the drainers[] slot as
		// already-exited (nil) so the deferred cleanup doesn't double-
		// stop a goroutine that's already gone.
		oldDrainerStop := drainers[leaderIdx]
		drainers[leaderIdx] = nil

		markDead(leader.id)
		if err := leader.Close(); err != nil {
			t.Fatalf("flap iteration %d: close leader %d: %v", i, leader.id, err)
		}
		// Close commitC happens inside leader.Close; the drainer goroutine
		// has already returned by here (it exited via !ok). Calling the
		// old stop function now is a no-op but also safe — close(stop)
		// on an unclosed channel with no listener does nothing, and
		// wg.Wait returns immediately because Done already fired.
		oldDrainerStop()

		// Wait for the survivors to re-elect. Using a subset slice
		// with only the survivors (not the full rafts) so stableLeader's
		// "leader must be in slice" check behaves correctly during the
		// window where the dead leader is still the reported leader
		// id on survivors.
		var survivors Rafts
		for _, r := range rafts {
			if r.id != leader.id {
				survivors = append(survivors, r)
			}
		}
		survivors.WaitForLeader(t)

		// Restart the dead leader. It comes back as a follower; the
		// new leader's AppendEntries catch it up via raft replication.
		if err := leader.Restart(); err != nil {
			t.Fatalf("flap iteration %d: restart node %d: %v", i, leader.id, err)
		}
		drainers[leaderIdx] = leader.startDrainer()
		markAlive(leader)

		// Wait for all 3 to re-agree on a leader before the next
		// iteration. Without this, the proposer keeps hitting a node
		// that's still applying replayed entries and half its
		// payloads time out, shrinking the acked set to the point
		// where the invariant check becomes trivial.
		rafts.WaitForLeader(t)

		// Stable window. Lets the proposer actually commit some
		// payloads before the next flap starts.
		time.Sleep(interFlapRest)
	}

	close(stopC)
	proposerWG.Wait()

	// Snapshot the acked set. mu is no longer contended (proposer
	// exited), but we take the lock for paranoia.
	mu.Lock()
	ackedSnapshot := make([]string, len(acked))
	copy(ackedSnapshot, acked)
	mu.Unlock()

	if len(ackedSnapshot) == 0 {
		// The proposer should have gotten SOME payloads through
		// between flaps. Zero suggests the test infrastructure is
		// broken (e.g., WaitForLeader races), not that the invariant
		// is preserved.
		t.Fatalf("no acked proposals across %d flap iterations — test is not exercising its invariants",
			flapIterations)
	}

	// Invariant: every acked payload appears exactly once in every
	// node's storage. We poll per-node for convergence because the
	// last flap's AppendEntries catch-up may still be in flight.
	for _, r := range rafts {
		// Poll until the acked set fully converges on this node, or
		// the deadline expires.
		deadline := time.Now().Add(10 * time.Second)
		for {
			es, err := r.ents()
			if err != nil {
				t.Fatal(err)
			}
			have := map[string]int{}
			for _, e := range es {
				have[string(e.Data)]++
			}
			missing := 0
			for _, want := range ackedSnapshot {
				if have[want] == 0 {
					missing++
				}
			}
			if missing == 0 {
				// All acked payloads present. Now check "exactly
				// once" across every acked payload.
				for _, want := range ackedSnapshot {
					if have[want] != 1 {
						t.Fatalf("node %d: payload %q appears %d times, expected exactly once — "+
							"exactly-once apply violated across leader transitions",
							r.id, want, have[want])
					}
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %d: %d of %d acked payloads never converged — "+
					"durability violated across leader transitions",
					r.id, missing, len(ackedSnapshot))
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
}

// tryProposeAndVerify sends payload on r's proposeC with a bounded send,
// then waits for the payload to appear in r's storage tail (also bounded).
// Returns true only if BOTH steps succeed within deadline. A send that
// blocks past deadline, or a commit that never arrives, returns false.
//
// Uses r.proposeChan() and r.ents() — both snapshot the underlying fields
// under r's RWMutex, so a concurrent Restart swapping the internal raft
// cannot tear the send/read.
//
// stopC is an optional early-exit signal (the flap-test proposer uses it
// so the last iteration's deadline doesn't delay shutdown). Pass nil for
// "no early exit".
func tryProposeAndVerify(r *Raft, payload []byte, deadline time.Duration, stopC <-chan struct{}) bool {
	// proposeC send with bounded timeout. If the node's serveChannels
	// reader is alive and not blocked, this succeeds immediately. If
	// the node is mid-Close, it returns false.
	ch := r.proposeChan()
	sendDeadline := time.NewTimer(deadline)
	defer sendDeadline.Stop()
	select {
	case ch <- payload:
	case <-sendDeadline.C:
		return false
	case <-stopC:
		return false
	}

	// Storage poll: wait for the payload to land at any position in
	// r's user-entry tail. We don't require it at exactly the tail —
	// concurrent proposes from other goroutines may interleave —
	// just that it appears somewhere. The per-payload deadline
	// bounds the total wait.
	poll := time.NewTimer(deadline)
	defer poll.Stop()
	for {
		es, err := r.ents()
		if err != nil {
			return false
		}
		for _, e := range es {
			if bytes.Equal(e.Data, payload) {
				return true
			}
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-poll.C:
			return false
		case <-stopC:
			return false
		}
	}
}

// -----------------------------------------------------------------------------
// Scenario (g): concurrent config changes
//
// Invariant protected: config-change idempotency under concurrent writers
// and raft forwarding.
//
// 10 goroutines concurrently propose distinct ingestable configurations
// via db.ProposeIngestable. Each goroutine targets a rotated db.DB so
// some proposes land on a follower and get forwarded to the leader via
// raft, exercising the forwarding path. After all proposes return:
//
//   - every node's storage reports exactly 10 ingestables (no lost
//     configs, no duplicates).
//   - every node's ingest worker registry holds exactly 10 entries
//     (regression test for the PR3 concurrent-replace fix at the
//     multi-node level — no orphaned workers from a concurrent-register
//     race).
// -----------------------------------------------------------------------------
func TestAdversarial_ConcurrentConfigChanges(t *testing.T) {
	_ = rand.New(rand.NewSource(7))

	const replicas = 3
	const ingestables = 10

	h := newMultiDBHarness(t, replicas)
	defer h.Close()

	// Register the same fake IngestableParser on every node, keyed by
	// "fake" — ProposeIngestable routes on Configuration.MimeType's
	// type field (see parser.ParseIngestable), so the parser name must
	// match what the test configuration uses below.
	for _, d := range h.dbs {
		fakeParser := &clusterfakes.FakeIngestableParser{}
		fakeIngestable := &clusterfakes.FakeIngestable{}
		fakeParser.ParseReturns(fakeIngestable, nil)
		d.AddIngestableParser("fake", fakeParser)
	}

	// Wait for all 3 DB instances to agree on a leader before the
	// concurrent proposers start. Without this barrier, early proposes
	// race the initial election and return ErrClosed / context-cancel
	// before raft ever gets them.
	h.WaitForLeader(t)

	// 10 concurrent ProposeIngestable calls, each targeting a rotated
	// DB so the propose distribution is approximately:
	//   - ~1/3 on the leader (fast path)
	//   - ~2/3 on a follower (forwarded via raft to the leader)
	//
	// Whoever the leader happens to be, at least a third of the
	// proposes exercise the forwarding path.
	var wg sync.WaitGroup
	errs := make([]error, ingestables)
	for i := 0; i < ingestables; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			cfg := createTestIngestableConfig(fmt.Sprintf("adv-%d", i))
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			errs[i] = h.dbs[i%replicas].ProposeIngestable(ctx, cfg)
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("ProposeIngestable %d: %v", i, err)
		}
	}

	// Every ingestable config landed on every node. storage.Ingestables
	// reads from bbolt, which the wal.Storage apply path has already
	// populated by the time ProposeIngestable returns (Propose blocks
	// until Apply — see DB.Propose's ack/waiter dance). Poll as a
	// safety net because the listenForIngestables → db.Ingest path
	// runs in its own goroutine and may briefly lag apply.
	for _, node := range h.nodes {
		deadline := time.Now().Add(5 * time.Second)
		for {
			cfgs, err := node.storage.Ingestables()
			if err != nil {
				t.Fatal(err)
			}
			if len(cfgs) == ingestables {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %d: storage.Ingestables() has %d configs, expected %d",
					node.id, len(cfgs), ingestables)
			}
			time.Sleep(25 * time.Millisecond)
		}
	}

	// Every ingest worker registered exactly once. The PR3 fix
	// prevents a concurrent-replace race from leaving a worker in
	// the registry without a corresponding map entry; this assertion
	// catches a regression that would land the workers under
	// mismatched ids or duplicate the same id.
	for _, d := range h.dbs {
		deadline := time.Now().Add(5 * time.Second)
		for {
			ids := d.IngestWorkerIDsForTest()
			if len(ids) == ingestables {
				// Every id appears exactly once (map keys are unique
				// by construction; the cardinality check is enough).
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("db.IngestWorkerIDsForTest returned %d ids, expected %d — "+
					"worker registry has orphaned or missing workers",
					len(ids), ingestables)
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
}

// multiDBHarness wires up N db.DB instances over real loopback HTTP for
// the config-change scenario. It owns the per-node wal.Storage, the
// per-node IngestableWithID channel, and the temporary directories that
// back the wal — all cleaned up in Close.
//
// The ingestable scenario needs the full apply chain (entry → wal apply
// → ingest channel → db.listenForIngestables → db.Ingest → registry),
// which only wal.Storage provides. The in-memory MemoryStorage used by
// the other adversarial scenarios is no-op on ApplyCommitted, so it
// would never register workers.
type multiDBHarness struct {
	t     *testing.T
	nodes []*multiDBNode
	dbs   []*db.DB
}

type multiDBNode struct {
	id      uint64
	dir     string
	storage *wal.Storage
	db      *db.DB
	parser  *parser.Parser
	sync    chan *db.SyncableWithID
	ingest  chan *db.IngestableWithID
}

func newMultiDBHarness(t *testing.T, replicas int) *multiDBHarness {
	t.Helper()

	ports := pickFreePorts(replicas)
	peers := db.Peers{}
	for i := 0; i < replicas; i++ {
		peers[uint64(i+1)] = fmt.Sprintf("http://127.0.0.1:%d", ports[i])
	}

	h := &multiDBHarness{t: t}
	for i := 0; i < replicas; i++ {
		id := uint64(i + 1)
		dir := t.TempDir()
		p := parser.New()
		syncCh := make(chan *db.SyncableWithID, 32)
		ingestCh := make(chan *db.IngestableWithID, 32)

		storage, err := wal.Open(dir, p, syncCh, ingestCh)
		if err != nil {
			t.Fatalf("wal.Open for node %d: %v", id, err)
		}

		d := db.New(id, peers, storage, p, syncCh, ingestCh,
			db.WithTickInterval(multiNodeTickInterval),
		)

		h.nodes = append(h.nodes, &multiDBNode{
			id:      id,
			dir:     dir,
			storage: storage,
			db:      d,
			parser:  p,
			sync:    syncCh,
			ingest:  ingestCh,
		})
		h.dbs = append(h.dbs, d)
	}

	return h
}

// WaitForLeader blocks until all DBs agree on the same non-zero leader
// id. Mirrors Rafts.WaitForLeader for the DB-level harness.
func (h *multiDBHarness) WaitForLeader(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(multiNodeStartupTimeout)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("multiDBHarness.WaitForLeader: no stable leader within %v", multiNodeStartupTimeout)
		}
		if h.stableLeader() != 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (h *multiDBHarness) stableLeader() uint64 {
	if len(h.dbs) == 0 {
		return 0
	}
	first := h.dbs[0].Leader()
	if first == 0 {
		return 0
	}
	for _, d := range h.dbs[1:] {
		if d.Leader() != first {
			return 0
		}
	}
	return first
}

// Close tears down every DB and wal.Storage. Safe to call even if a
// partial initialization failed — nil-guards each step.
func (h *multiDBHarness) Close() error {
	for _, n := range h.nodes {
		if n.db != nil {
			_ = n.db.Close()
		}
		if n.storage != nil {
			_ = n.storage.Close()
		}
	}
	return nil
}

// createTestIngestableConfig builds a minimal cluster.Configuration that
// routes through the "fake" IngestableParser registered in the scenario.
// The TOML body names `fake` as the parser type; the parser's ParseReturns
// stub ignores the rest of the body, so we don't need realistic fields.
func createTestIngestableConfig(id string) *cluster.Configuration {
	data := []byte(fmt.Sprintf("[ingestable]\nname = \"%s\"\ntype = \"fake\"\n", id))
	return &cluster.Configuration{
		ID:       id,
		MimeType: "text/toml",
		Data:     data,
	}
}

// -----------------------------------------------------------------------------
// Scenario (b): asymmetric (one-way) partition
//
// Invariants protected:
//
//   - PreVote correctness under a DIRECTIONAL partition. Symmetric
//     partition (scenario a) can't express this because both sides lose
//     contact — no side has the "I can hear you but you can't hear me"
//     shape that produces stale-term disruption in pre-PreVote raft.
//     Here, leader → follower is dropped while follower → leader still
//     works: the follower's heartbeat arrival stops (so its election
//     timer fires), but the follower can still send PreVote messages to
//     everyone else.
//
//   - Leader liveness when one follower stops hearing heartbeats.
//     Without PreVote, the isolated-in-one-direction follower would
//     repeatedly bump its term via campaign messages, and the leader
//     (which eventually hears the higher term via follower → leader
//     traffic) would step down even though it still has quorum with the
//     OTHER follower. With PreVote, the follower asks for votes before
//     incrementing term; the non-isolated followers reject the PreVote
//     (they just heard from the current leader), and the leader stays.
//
//   - Post-heal convergence. Dropping the DirectionalDrop and then
//     proposing one more entry must succeed on all three nodes, proving
//     the previously-isolated follower catches back up via routine
//     AppendEntries replication.
// -----------------------------------------------------------------------------
func TestAdversarial_AsymmetricPartition(t *testing.T) {
	// Seeded RNG unused in this scenario today — same reservation pattern
	// as scenarios (a), (c), (g). Uniform across the suite for triage.
	_ = rand.New(rand.NewSource(2))

	rafts, cluster := createFaultyRafts(3)

	// Defer ordering: drainer-stop defers FIRST so it runs LAST (after
	// rafts.Close), matching the pattern in scenario (a).
	stopDrainers := rafts.StartDrainers()
	defer stopDrainers()
	defer rafts.Close()

	leaderID := rafts.WaitForLeader(t)
	leader := rafts.LeaderRaft()
	if leader == nil {
		t.Fatalf("LeaderRaft returned nil after WaitForLeader")
	}

	// Pick any follower to be the isolated-in-one-direction side. Any
	// non-leader id works — the scenario's invariants don't depend on
	// which follower is chosen.
	var isolatedID uint64
	for _, r := range rafts {
		if r.id != leaderID {
			isolatedID = r.id
			break
		}
	}
	if isolatedID == 0 {
		t.Fatalf("could not find a follower id distinct from leader %d", leaderID)
	}

	// Establish a baseline so the post-heal catch-up assertion has something
	// to check against.
	proposeAndCheck(t, leader, "pre-asym")
	for _, r := range rafts {
		waitForUserEntry(t, r, []byte("pre-asym"))
	}

	// One-way drop: leader can no longer reach the isolated follower.
	// The reverse direction (isolated → leader) still works, so the
	// isolated follower's PreVotes can still reach the leader and be
	// rejected — that's the whole point of the test.
	cluster.DirectionalDrop(leaderID, isolatedID, true)

	// Sit for 1s — long enough that the isolated follower's 100ms
	// election timer would fire ~10 times if PreVote weren't masking it.
	// Each election attempt issues a PreVote, and each PreVote that
	// DIDN'T get masked would result in the leader stepping down on a
	// higher term. 10 attempts is a generous margin above the ~1-2
	// attempts that a flaky PreVote implementation would need to trigger
	// the bug.
	const isolationWindow = 1 * time.Second
	deadline := time.Now().Add(isolationWindow)
	for time.Now().Before(deadline) {
		// The leader must not step down. Any change in leader id
		// during the isolation window is an invariant violation.
		if got := rafts[0].Leader(); got != leaderID {
			// rafts[0] may BE the isolated follower — in that case
			// it's expected to lose track of the leader because
			// heartbeats are dropped. Check leader id from the OTHER
			// non-isolated follower instead.
			if rafts[0].id == isolatedID {
				// Use a non-isolated node's view.
				for _, r := range rafts {
					if r.id == leaderID || r.id == isolatedID {
						continue
					}
					if r.Leader() != leaderID {
						t.Fatalf("non-isolated follower %d lost leader: expected %d, got %d — "+
							"leader stepped down under directional partition; PreVote did not mask isolated-follower election attempts",
							r.id, leaderID, r.Leader())
					}
				}
			} else {
				t.Fatalf("non-isolated node %d: leader changed from %d to %d during directional partition — "+
					"PreVote did not mask isolated-follower election attempts",
					rafts[0].id, leaderID, got)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Heal: the leader can reach the isolated follower again.
	cluster.DirectionalDrop(leaderID, isolatedID, false)

	// Post-heal sanity: the cluster is still functional. The original
	// leader is still leader (no step-down during isolation), the
	// isolated follower catches up via AppendEntries, a new propose
	// commits on all three.
	rafts.WaitForLeader(t)
	if got := rafts.LeaderRaft(); got == nil || got.id != leaderID {
		gotID := uint64(0)
		if got != nil {
			gotID = got.id
		}
		t.Fatalf("post-heal leader changed: expected %d, got %d — "+
			"leader stepped down at some point (possibly during heal) despite PreVote",
			leaderID, gotID)
	}
	proposeAndCheck(t, leader, "post-asym")
	for _, r := range rafts {
		waitForUserEntry(t, r, []byte("post-asym"))
	}
}

// -----------------------------------------------------------------------------
// Scenario (d): slow follower
//
// Invariants protected:
//
//   - Heartbeat / election-timeout tolerance of a slow replica. With 500ms
//     one-way latency from leader to follower3, the follower experiences
//     delayed heartbeats but NOT missing ones — its first heartbeat arrives
//     500ms late, then subsequent heartbeats arrive at the normal 10ms
//     cadence (just shifted in time). PreVote ensures that any election
//     timer fires from the delayed startup window don't disrupt the leader.
//
//   - Catch-up-path correctness under latency. All 1000 proposes must
//     eventually apply on follower3 — the entries arrive in one or more
//     AppendEntries batches, each 500ms late, and the storage state must
//     converge regardless of the per-message delay.
//
//   - Exactly-once apply under latency. Delayed delivery must not cause
//     any entry to be applied twice (e.g., via a retransmit whose earlier
//     copy was not fully processed). The per-node "every acked entry
//     appears exactly once" check is the same invariant scenario (c)
//     protects under leader flapping.
// -----------------------------------------------------------------------------
func TestAdversarial_SlowFollower(t *testing.T) {
	_ = rand.New(rand.NewSource(4))

	rafts, cluster := createFaultyRafts(3)

	stopDrainers := rafts.StartDrainers()
	defer stopDrainers()
	defer rafts.Close()

	leaderID := rafts.WaitForLeader(t)
	leader := rafts.LeaderRaft()
	if leader == nil {
		t.Fatalf("LeaderRaft returned nil after WaitForLeader")
	}

	// Pick a single follower to slow down. We choose the first non-leader
	// node; which one it is doesn't matter to the invariants being tested
	// (the leader keeps quorum via itself + the other follower).
	var slowID uint64
	for _, r := range rafts {
		if r.id != leaderID {
			slowID = r.id
			break
		}
	}
	if slowID == 0 {
		t.Fatalf("could not find a follower id distinct from leader %d", leaderID)
	}

	// 500ms latency per the ticket. Applied BEFORE proposes start so every
	// AppendEntries carrying user data suffers the delay. The reverse
	// direction (follower → leader) is unaffected, so replies come back
	// immediately and the leader's view of the follower's matchIndex
	// advances as the follower catches up.
	const slowLatency = 500 * time.Millisecond
	cluster.AddLatency(leaderID, slowID, slowLatency)

	// 1000 entries back-to-back per the ticket. We fire them at the
	// leader's proposeC without per-propose commit confirmation: the
	// ticket's explicit invariant is "all 1000 entries eventually apply
	// on all 3 nodes", not "each propose commits within a bounded time".
	// Using a bounded sender with a fallback timeout guards against the
	// proposeC send itself hanging (which would indicate serveChannels
	// is wedged — a real failure mode).
	const numEntries = 1000
	ch := leader.proposeChan()
	for i := 0; i < numEntries; i++ {
		payload := []byte(fmt.Sprintf("slow-%d", i))
		select {
		case ch <- payload:
		case <-time.After(5 * time.Second):
			t.Fatalf("propose %d: proposeC send blocked for 5s — "+
				"leader's serveChannels appears wedged under slow-follower load",
				i)
		}
	}

	// Liveness check: the leader did not step down mid-run. A step-down
	// would indicate the slow follower somehow disrupted the election
	// despite PreVote (or the other follower lost contact with the leader,
	// which would be a different and equally damning bug).
	if got := rafts.LeaderRaft(); got == nil || got.id != leaderID {
		gotID := uint64(0)
		if got != nil {
			gotID = got.id
		}
		t.Fatalf("leader changed during slow-follower run: expected %d, got %d — "+
			"slow follower should not cause re-election",
			leaderID, gotID)
	}

	// Catch-up check: every proposed entry appears exactly once on every
	// node, including the slow one. Poll per-node because the slow
	// follower's last batch of AppendEntries is at least 500ms behind
	// the leader's commit point.
	expected := make([]string, numEntries)
	for i := 0; i < numEntries; i++ {
		expected[i] = fmt.Sprintf("slow-%d", i)
	}
	catchUpDeadline := 30 * time.Second
	for _, r := range rafts {
		nodeDeadline := time.Now().Add(catchUpDeadline)
		for {
			es, err := r.ents()
			if err != nil {
				t.Fatal(err)
			}
			have := map[string]int{}
			for _, e := range es {
				have[string(e.Data)]++
			}
			missing := 0
			for _, want := range expected {
				if have[want] == 0 {
					missing++
				}
			}
			if missing == 0 {
				for _, want := range expected {
					if have[want] != 1 {
						t.Fatalf("node %d: payload %q appears %d times, expected exactly once — "+
							"exactly-once apply violated under slow-follower latency",
							r.id, want, have[want])
					}
				}
				break
			}
			if time.Now().After(nodeDeadline) {
				t.Fatalf("node %d: %d of %d entries still missing after %v — "+
					"slow follower did not catch up via AppendEntries",
					r.id, missing, numEntries, catchUpDeadline)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// -----------------------------------------------------------------------------
// Scenario (f): disk full (ENOSPC)
//
// Invariants protected:
//
//   - Graceful degradation on single-node storage failure. When one node's
//     Save path starts returning ENOSPC, the raft.go Ready loop returns
//     from serveChannels rather than continuing past the failed Save —
//     that preserves the raft invariant that every Ready entry must be
//     durably persisted before Advance is called. The node becomes raft-
//     silent (no more Send or Advance), which matches the ticket's
//     "fatals / steps down cleanly" acceptance criteria.
//
//   - No data loss on the surviving pair. Any propose that returns
//     successfully (commit observed on the proposing node) must appear on
//     BOTH surviving nodes, not just one. The failed node is allowed to
//     have missing data — that's what ENOSPC models.
//
//   - Liveness of the surviving pair. Even after node 1 freezes, nodes 2
//     and 3 must elect a leader (if not already) and continue committing
//     proposes. Two out of three is quorum, so this is a pure failover
//     liveness check.
// -----------------------------------------------------------------------------
func TestAdversarial_DiskFull(t *testing.T) {
	_ = rand.New(rand.NewSource(6))

	// Threshold 50 per the ticket. Bootstrap consumes ~4 entries (one
	// conf-change per peer + empty leader entry on term start), so the
	// first ~46 user entries land normally on node 1 before Save starts
	// failing. The cluster-level assertion only requires that SOME
	// proposals commit before ENOSPC and that ALL acked proposals post-
	// ENOSPC land on the surviving pair — the exact split is irrelevant.
	const threshold = 50

	// faultyStore holds the FaultyStorage pointer for node 1 so the test
	// can assert it actually tripped (a test where Save never failed
	// would be a false pass).
	var faultyStore *FaultyStorage
	rafts, _ := createFaultyRaftsWithStorageWrapper(3, func(id uint64, s db.Storage) db.Storage {
		if id != 1 {
			return s
		}
		faultyStore = NewFaultyStorage(s, threshold)
		return faultyStore
	})

	stopDrainers := rafts.StartDrainers()
	defer stopDrainers()
	defer rafts.Close()

	rafts.WaitForLeader(t)

	// Propose via node 2 (never the faulty node 1). If node 2 is a
	// follower its propose is forwarded to whoever is leader; if node 1
	// is the leader AND its Ready loop has frozen on the ENOSPC error
	// send, the propose will time out until nodes 2 and 3 re-elect among
	// themselves.
	const numEntries = 100
	const proposeDeadline = 5 * time.Second
	target := rafts[1]

	// Track which entries successfully committed on the target node.
	// Those are the "acked" proposals whose presence on nodes 2 and 3
	// is the load-bearing invariant.
	var acked []string
	for i := 0; i < numEntries; i++ {
		payload := fmt.Sprintf("disk-%d", i)
		if tryProposeAndVerify(target, []byte(payload), proposeDeadline, nil) {
			acked = append(acked, payload)
		}
		// Unacked proposes are not required to have landed (the
		// failing node may have been leader mid-flight). We don't
		// fail the test on them; the surviving pair's
		// no-data-loss invariant only applies to the acked set.
	}

	if len(acked) < threshold/2 {
		t.Fatalf("only %d of %d proposals acked on the surviving pair — "+
			"surviving pair did not maintain liveness under single-node ENOSPC",
			len(acked), numEntries)
	}

	// The FaultyStorage must actually have tripped. If threshold was set
	// so high that Save never failed, this test didn't exercise its
	// invariants and a silent false-pass would be worse than a noisy
	// failure.
	if !faultyStore.Tripped() {
		t.Fatalf("FaultyStorage on node 1 never tripped — test did not exercise ENOSPC path "+
			"(threshold=%d may be too high for the entry volume)",
			threshold)
	}

	// Every acked proposal is present on BOTH surviving nodes. Poll for
	// convergence because the last few proposes may still be in flight to
	// the non-leader follower when the loop exits.
	survivors := []*Raft{rafts[1], rafts[2]}
	for _, r := range survivors {
		nodeDeadline := time.Now().Add(15 * time.Second)
		for {
			es, err := r.ents()
			if err != nil {
				t.Fatal(err)
			}
			have := map[string]bool{}
			for _, e := range es {
				have[string(e.Data)] = true
			}
			missing := 0
			for _, want := range acked {
				if !have[want] {
					missing++
				}
			}
			if missing == 0 {
				break
			}
			if time.Now().After(nodeDeadline) {
				t.Fatalf("surviving node %d: %d of %d acked proposals missing — "+
					"data loss on the surviving pair under single-node ENOSPC",
					r.id, missing, len(acked))
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// -----------------------------------------------------------------------------
// Scenario (e): severe-lag follower rebuild
//
// A follower is taken offline, the rest of the cluster advances past it,
// and then the follower is rebuilt via the operator-facing "rsync from a
// healthy peer" procedure documented at docs/operations/rebuild.md. The
// cluster must converge with every node holding byte-identical permanent
// event logs — the determinism guarantee that makes the rsync-based
// rebuild safe in the first place.
//
// This scenario only became buildable after permanent-event-log.md
// landed: before that, there was no separate event store to rsync, no
// metadata snapshot to install, and no raft-log compaction to push a
// follower outside the recovery window. See the `permanent-event-log.md`
// prerequisite note in the severe-lag-rebuild ticket.
//
// Invariants protected:
//
//   - Storage invariant (P_local == R_local) holds on every node after
//     the rebuilt follower rejoins, per docs/event-log-architecture.md
//     § "The storage invariant". If the invariant breaks on any node,
//     checkStorageInvariant fatal-exits the Ready loop.
//
//   - Determinism invariant — every node's permanent event log is byte-
//     identical for the full applied prefix after convergence, per
//     docs/event-log-architecture.md § "Determinism requirement". The
//     rebuilt follower's events/ directory must hash to the same value
//     as the two peers that stayed online.
//
//   - Recoverability. Operator can in fact rebuild a severely-lagged
//     follower by copying a healthy peer's data directory; post-rebuild
//     a further propose reaches all three nodes, proving the rebuilt
//     node is a full participant again (not just a silent replica).
//
//   - Raft-log compaction on the leader actually fires past the stopped
//     follower's last-applied index, the canonical production trigger
//     for severe lag. Without this, the test is vulnerable to plain
//     AppendEntries catch-up resolving the gap without needing a
//     rebuild — which wouldn't exercise the scenario the ticket cares
//     about.
//
// The fatal-exit path IS exercised in-process. Each node's logger is
// wired with zap.WithFatalHook so logger.Fatal (called by
// processSnapshot when wal.Storage.RestoreSnapshot rejects a too-far-
// ahead snapshot) routes through a test hook that records the event
// and calls runtime.Goexit instead of os.Exit. The test observes the
// fatal via a channel, then proceeds with the rsync rebuild — so
// both halves of the v1 severe-lag flow are under test in a single
// scenario, mirroring the production sequence: follower receives
// InstallSnapshot → P_local invariant violated → fatal exit →
// operator runs rebuild → cluster converges.
// -----------------------------------------------------------------------------
func TestAdversarial_SevereLagFollowerRebuild(t *testing.T) {
	// Seeded RNG reserved for future test randomness — same pattern as
	// scenarios (a), (b), (c), (d), (f), (g). Keeps triage uniform.
	_ = rand.New(rand.NewSource(8))

	const (
		replicas        = 3
		baselineEntries = 30
		duringEntries   = 30
	)

	// Aggressive compaction so maybeCompact fires on the surviving pair
	// during Phase 3 and advances past follower 3's stale applied index.
	// 512 bytes is well below what a 30-entry burst produces with even
	// tiny payloads, so the size limb is reached within a few Saves —
	// fast enough that every iteration reliably pushes compaction
	// past the stopped follower's position.
	nodeOpts := []db.Option{
		db.WithCompactMaxSize(512),
		db.WithCompactMaxAge(0),
	}

	rafts, cluster, dirs, fatalC := newSevereLagCluster(t, replicas, nodeOpts)

	// Per-node drainers so we can stop/restart individual nodes without
	// leaking commitC readers. Matches the scenario (c) pattern: defer
	// drainer-stop FIRST so Go's LIFO ordering runs it LAST, after the
	// rafts have already closed their commitCs.
	drainers := make([]func(), replicas)
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

	// Track whether each node is currently open so the deferred cleanup
	// doesn't double-close a storage we already closed mid-test. Indexed
	// by position in the rafts slice (parallel to dirs / drainers).
	alive := make([]bool, replicas)
	for i := range alive {
		alive[i] = true
	}
	defer func() {
		// Close rafts first so Save/Apply goroutines exit before we
		// close the underlying storage files out from under them.
		for i, r := range rafts {
			if alive[i] {
				_ = r.Close()
			}
		}
		for i, r := range rafts {
			if alive[i] {
				r.mu.RLock()
				s := r.storage
				r.mu.RUnlock()
				_ = s.Close()
			}
		}
	}()

	rafts.WaitForLeader(t)

	// Phase 1: baseline. Propose baselineEntries entries and wait for
	// every node — including follower 3 — to apply each one. After this
	// phase all three nodes have identical applied state.
	for i := 0; i < baselineEntries; i++ {
		payload := fmt.Sprintf("base-%d", i)
		proposeAndCheck(t, rafts.LeaderRaft(), payload)
	}
	for _, r := range rafts {
		waitForUserEntry(t, r, []byte(fmt.Sprintf("base-%d", baselineEntries-1)))
	}

	// Record follower 3's applied index before we stop it; the post-
	// rebuild assertion uses this to prove the node's own state really
	// was stale relative to the cluster's advance during Phase 3. Without
	// that gap, the rsync in Phase 4 would be a no-op and the test
	// wouldn't exercise the rebuild path.
	follower3 := rafts[2]
	follower3Dir := dirs[2]
	follower3AppliedBefore := follower3.storage.AppliedIndex()

	// Phase 2: stop follower 3 cleanly. Close its raft and its wal.Storage
	// so no goroutine is writing to the on-disk state when we come back
	// to overwrite it in Phase 4.
	if err := follower3.Close(); err != nil {
		t.Fatalf("close follower 3: %v", err)
	}
	drainers[2]()
	drainers[2] = nil
	if err := follower3.storage.Close(); err != nil {
		t.Fatalf("close follower 3 storage: %v", err)
	}
	alive[2] = false

	// Phase 3: propose duringEntries more entries while follower 3 is
	// down. Every propose is committed by the surviving pair (nodes 1 and
	// 2), both of which apply + bump EventIndex/AppliedIndex.
	//
	// Proposes must hit a LIVE node; rafts.LeaderRaft() returns nil if
	// the leader was node 3 (killed). The surviving pair contains the
	// new leader either way.
	survivors := Rafts{rafts[0], rafts[1]}
	survivors.WaitForLeader(t)
	for i := 0; i < duringEntries; i++ {
		payload := fmt.Sprintf("during-%d", i)
		proposeAndCheck(t, survivors.LeaderRaft(), payload)
	}
	for _, r := range survivors {
		waitForUserEntry(t, r, []byte(fmt.Sprintf("during-%d", duringEntries-1)))
	}

	// Prove the surviving pair actually advanced past the stopped
	// follower's last-applied point. If it didn't, the rebuild in
	// Phase 4 is a no-op and the test is degenerate.
	var survivorsApplied uint64
	for _, r := range survivors {
		if a := r.storage.AppliedIndex(); a > survivorsApplied {
			survivorsApplied = a
		}
	}
	if survivorsApplied <= follower3AppliedBefore {
		t.Fatalf("surviving pair did not advance past stopped follower "+
			"(survivors applied=%d, follower 3 applied at stop=%d) — "+
			"test did not exercise rebuild path",
			survivorsApplied, follower3AppliedBefore)
	}

	// Prove that raft-log compaction actually fired on the surviving
	// pair past follower 3's stale position. Without this, follower 3
	// could in principle catch up via plain AppendEntries on restart
	// without needing an rsync rebuild — which would make the rebuild
	// exercise in Phase 4+ vacuously "succeed". The canonical severe-
	// lag trigger in production IS compaction; requiring it here keeps
	// the test honest.
	var leaderCompacted uint64
	for _, r := range survivors {
		if c := r.raft.LastCompactedIndexForTest(); c > leaderCompacted {
			leaderCompacted = c
		}
	}
	if leaderCompacted == 0 {
		t.Fatalf("no compaction observed on surviving pair — compactMaxSize=%d " +
			"was not reached during the %d-entry burst (consider smaller limit " +
			"or more proposes)", 512, duringEntries)
	}
	if leaderCompacted <= follower3AppliedBefore {
		t.Fatalf("compaction on surviving pair reached index %d but stopped "+
			"follower was already at %d — compaction did not advance past "+
			"follower 3's stale applied index; AppendEntries could still "+
			"resolve the gap without rebuild",
			leaderCompacted, follower3AppliedBefore)
	}

	// Quiesce: wait until both survivors' AppliedIndex matches and
	// EventIndex == AppliedIndex on each. waitForUserEntry in the
	// propose loop only blocks on user-visible entries; raft can have
	// in-flight empty-leader entries or heartbeat-driven commit bumps
	// that cause one survivor's events/ to briefly lead the other's.
	// Closing node 1 mid-flight in Phase 4 would then hand node 3
	// (via rsync) an events/ prefix that doesn't match what node 2
	// ends up with post-rebuild — surfacing as a spurious determinism
	// failure at the final hash comparison. Waiting here makes the
	// close-point deterministic.
	waitForSurvivorConvergence(t, survivors, 10*time.Second)

	// Phase 4: restart follower 3 WITHOUT rsync to trigger the
	// production severe-lag fatal path. Follower 3's stored state is
	// at raft index ~follower3AppliedBefore; the leader's firstIndex
	// is now past the compacted boundary from Phase 3. When follower 3
	// rejoins, the leader's progress tracker sees it's behind
	// firstIndex and sends InstallSnapshot at an index > follower 3's
	// EventIndex. wal.Storage.RestoreSnapshot rejects with
	//   "restore snapshot: snap.Metadata.Index=X exceeds EventIndex=Y; run rebuild procedure"
	// processSnapshot in raft.go converts that to logger.Fatal, which
	// under the test's fatal hook posts to fatalC and calls
	// runtime.Goexit (terminating serveChannels, not the test process).
	//
	// Without the fatal hook, this step would os.Exit and take the
	// test binary with it — which is why the previous version of this
	// test skipped the fatal-exit path and jumped straight to rsync.
	rebootWalNode(t, rafts[2], follower3Dir, nodeOpts, fatalC)
	drainers[2] = rafts[2].startDrainer()
	alive[2] = true

	// Wait for the fatal event. The snapshot+reject round is fast —
	// a couple of election intervals for the leader to detect follower
	// 3's position, ship the snapshot, have it rejected, and fatal.
	// 10 s is generous headroom for loaded test machines.
	select {
	case ev := <-fatalC:
		if ev.nodeID != 3 {
			t.Fatalf("fatal came from node %d, expected 3 (stopped-then-restarted follower): %q",
				ev.nodeID, ev.message)
		}
		msg := strings.ToLower(ev.message)
		// processSnapshot's fatal is "restore snapshot failed";
		// checkStorageInvariant's is "storage invariant violation".
		// Either one is a valid signal that the severe-lag v1
		// fail-fast behaviour kicked in — that's the invariant this
		// phase protects.
		if !strings.Contains(msg, "snapshot") && !strings.Contains(msg, "invariant") {
			t.Fatalf("fatal on node 3 did not match severe-lag path: %q", ev.message)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("follower 3 did not fatal-exit within 10s after restart without rsync — " +
			"expected processSnapshot → restore-snapshot-failed fatal from the leader's " +
			"InstallSnapshot round")
	}

	// Close the fatal'd node cleanly. serveChannels has already
	// Goexit'd (its deferred close of serveChannelsDoneC fired), so
	// raft.Close's wait on serveChannelsDoneC returns immediately.
	// stopTransport and node.Stop still run to tear down the HTTP
	// listener and etcd raft's internal goroutine; without that, the
	// test would leak goroutines across -count=20 iterations.
	if err := rafts[2].Close(); err != nil {
		t.Fatalf("close fatal'd follower 3: %v", err)
	}
	drainers[2]()
	drainers[2] = nil
	if err := rafts[2].storage.Close(); err != nil {
		t.Fatalf("close fatal'd follower 3 storage: %v", err)
	}
	alive[2] = false

	// Phase 5: rsync node 1's data directory onto follower 3's. We MUST
	// take node 1 offline before the copy: tidwall/wal segments and
	// bbolt are being actively written to by node 1's raft loop, and a
	// mid-write copy would capture a torn state. This is equivalent to
	// the "stop the source, snapshot, restart" pattern that real rsync
	// rebuilds use (with filesystem snapshots or a brief quiesce).
	//
	// Closing node 1 leaves only node 2 alive — below quorum — so no
	// proposes can commit during this window. That's fine; the test
	// isn't proposing during the rsync. Proposes resume in Phase 7.
	healthy := rafts[0]
	healthyDir := dirs[0]
	if err := healthy.Close(); err != nil {
		t.Fatalf("close healthy peer for rsync: %v", err)
	}
	drainers[0]()
	drainers[0] = nil
	if err := healthy.storage.Close(); err != nil {
		t.Fatalf("close healthy peer storage: %v", err)
	}
	alive[0] = false

	// Blow away follower 3's stale dir, then copy node 1's in. Mirrors
	// the "rm -rf /var/lib/committed/*; rsync healthy:/var/lib/committed/
	// /var/lib/committed/" steps in docs/operations/rebuild.md.
	if err := os.RemoveAll(follower3Dir); err != nil {
		t.Fatalf("rm failed follower dir: %v", err)
	}
	if err := os.MkdirAll(follower3Dir, 0o755); err != nil {
		t.Fatalf("mkdir follower dir: %v", err)
	}
	copyTree(t, healthyDir, follower3Dir)

	// Phase 6: restart node 1 and node 3 against their (possibly-copied)
	// data dirs. Both come up as raft followers restarting from durable
	// state; whoever node 2 believes is leader (itself, after the brief
	// quorum loss during the rsync window) will replicate any small gap
	// via AppendEntries. Node 3's copied state is at the same index as
	// node 1's was at Phase 5, so the gap is zero or near-zero.
	rebootWalNode(t, rafts[0], healthyDir, nodeOpts, fatalC)
	drainers[0] = rafts[0].startDrainer()
	alive[0] = true

	rebootWalNode(t, rafts[2], follower3Dir, nodeOpts, fatalC)
	drainers[2] = rafts[2].startDrainer()
	alive[2] = true

	// Give the freshly-rebooted HTTP transports a moment to come up and
	// re-establish peer connections before forcing an election. Without
	// this, a tight race between reboot and the first PreVote round can
	// leave one node unreachable long enough that convergence exceeds
	// even generous timeouts. adversarialSettleTime (400ms) covers two
	// full election timeouts, which is enough for both transports to
	// bind, announce, and accept the first heartbeat.
	time.Sleep(adversarialSettleTime)

	// Phase 7: convergence. A full leader election must complete across
	// all three nodes before we can propose; the rsync+restart window
	// may have forced a re-election on node 2 (it went solo), and nodes
	// 1 and 3 are coming back with potentially stale term state plus
	// fresh HTTP listeners. The default WaitForLeader timeout (5s)
	// occasionally clips the worst-case 3-way reconvergence, so we use
	// a larger bound here.
	waitForLeaderExtended(t, rafts, 15*time.Second)

	const postRebuildPayload = "post-rebuild"
	proposeAndCheck(t, rafts.LeaderRaft(), postRebuildPayload)
	for _, r := range rafts {
		waitForUserEntry(t, r, []byte(postRebuildPayload))
	}

	// Let the final apply + invariant check settle on every node. Without
	// this, a race between the post-rebuild propose's last AppendEntries
	// reply and the byte-for-byte hash comparison below can flag a
	// spurious "node 3 events log shorter than node 1" failure.
	deadline := time.Now().Add(5 * time.Second)
	for {
		allConverged := true
		for _, r := range rafts {
			p := r.storage.EventIndex()
			a := r.storage.AppliedIndex()
			if p != a {
				allConverged = false
				break
			}
		}
		if allConverged {
			// All three are at P_local == R_local AND all three have
			// applied the post-rebuild entry. Check the indices match
			// across nodes too.
			ref := rafts[0].storage.AppliedIndex()
			stable := true
			for _, r := range rafts[1:] {
				if r.storage.AppliedIndex() != ref {
					stable = false
					break
				}
			}
			if stable {
				break
			}
		}
		if time.Now().After(deadline) {
			for _, r := range rafts {
				t.Logf("node %d: EventIndex=%d AppliedIndex=%d",
					r.id, r.storage.EventIndex(), r.storage.AppliedIndex())
			}
			t.Fatal("cluster did not converge to matching P_local == R_local across all nodes")
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Invariant 1: storage invariant (P_local == R_local) holds on every
	// node post-rebuild. This is the single most important post-condition
	// of the scenario — if it fails the next Ready iteration would
	// fatal-exit the node.
	for _, r := range rafts {
		p := r.storage.EventIndex()
		a := r.storage.AppliedIndex()
		if p != a {
			t.Fatalf("node %d: storage invariant violated post-rebuild "+
				"(EventIndex=%d, AppliedIndex=%d, gap=%d)",
				r.id, p, a, int64(a)-int64(p))
		}
	}

	// Invariant 2: AppliedIndex matches across all three nodes. Follower
	// 3 must catch up to exactly the same index as the peers — a stuck
	// rebuilt follower is as bad as a lost one.
	refApplied := rafts[0].storage.AppliedIndex()
	for _, r := range rafts[1:] {
		if got := r.storage.AppliedIndex(); got != refApplied {
			t.Fatalf("node %d AppliedIndex=%d, node %d AppliedIndex=%d — "+
				"rebuilt cluster diverged after convergence",
				r.id, got, rafts[0].id, refApplied)
		}
	}

	// Before closing, fully partition every node from every other so
	// the teardown sequence can't cause any further applies. Without
	// this, closing rafts[0] first leaves nodes 2+3 with quorum (2 of
	// 3), so they re-elect and the new leader writes an empty leader
	// entry that both survivors apply — but rafts[0]'s events/ is
	// already frozen. That one-entry gap shows up as a byte-for-byte
	// mismatch in the determinism check below.
	//
	// Partitioning at the transport layer is the cheapest way to freeze
	// the log: Send() drops all outbound traffic, so no AppendEntries,
	// no heartbeats, no votes. Each node's Ready loop still ticks, but
	// nothing can commit (no peer replies).
	cluster.Partition([]uint64{1}, []uint64{2, 3})
	cluster.Partition([]uint64{2}, []uint64{3})

	// Close every raft + storage now so the on-disk event log files are
	// in a stable state for the byte-for-byte comparison below. Walking
	// tidwall/wal segments while the underlying wal.Log is still open
	// risks catching a segment mid-rotation.
	for i, r := range rafts {
		if alive[i] {
			_ = r.Close()
		}
	}
	for i, r := range rafts {
		if alive[i] {
			_ = r.storage.Close()
			alive[i] = false
		}
	}
	// Stop drainers now that every commitC has been closed. (The
	// deferred drainer cleanup above is then a no-op.)
	for i, d := range drainers {
		if d != nil {
			d()
			drainers[i] = nil
		}
	}

	// Invariant 3: determinism. ApplyCommitted mirrors every committed
	// raft entry into events/ keyed by raft index, and raft's log-
	// matching property guarantees identical (term, data) tuples at
	// identical indices across nodes. So for the common prefix of the
	// event log — every raft index that every node has durably written
	// — the raw bytes at each corresponding wal sequence must be
	// byte-for-byte identical. This assertion locks in determinism
	// against future regressions in apply / event-log write ordering.
	//
	// A node may have ONE extra trailing event vs. its peers (a
	// straggler apply after the partition freeze but before close). We
	// deliberately do NOT assert on the length tail — that part is
	// inherently racy at close time. The shared prefix is the load-
	// bearing invariant; any regression in determinism shows up here
	// just as loudly as in a full-directory hash.
	assertEventLogPrefixMatches(t, rafts, dirs)
}

// assertEventLogPrefixMatches reopens each node's events/ dir as a
// standalone tidwall/wal log, and asserts every node has byte-identical
// raw bytes at every wal sequence from 1 through the minimum lastSeq
// observed across all nodes. Runs with every raft + storage already
// closed so the on-disk state is stable.
func assertEventLogPrefixMatches(t *testing.T, rafts Rafts, dirs []string) {
	t.Helper()
	// Reopen each events/ dir as a raw wal.Log so we can read raw bytes
	// by sequence. wal.Open at the storage-layer level would re-run
	// migrations + bbolt and want sync/ingest channels — too heavy for
	// a post-mortem byte comparison.
	logs := make([]*wallog, len(rafts))
	for i, d := range dirs {
		lg, err := openRawWalLog(filepath.Join(d, "events"))
		if err != nil {
			t.Fatalf("reopen events dir for node %d: %v", rafts[i].id, err)
		}
		logs[i] = lg
		defer lg.Close()
	}

	// Determine the common prefix: the minimum lastSeq any node has.
	// Since wal seq 1:1 corresponds to raft index on a fresh-start
	// cluster (before any compaction), this is also the highest raft
	// index every node has durably written to its event log.
	minLastSeq := uint64(^uint64(0))
	for i, lg := range logs {
		last, err := lg.LastSeq()
		if err != nil {
			t.Fatalf("node %d lastSeq: %v", rafts[i].id, err)
		}
		if last < minLastSeq {
			minLastSeq = last
		}
	}
	if minLastSeq == 0 {
		t.Fatal("every node's events/ is empty — test did not exercise the rebuild path")
	}

	// Compare entry-by-entry across nodes, from seq 1 through
	// minLastSeq. Any mismatch is a determinism violation: the same
	// raft index should map to the same pb.Entry bytes on every node
	// (log matching property + ApplyCommitted writing every committed
	// entry into events/ unconditionally).
	for seq := uint64(1); seq <= minLastSeq; seq++ {
		ref, err := logs[0].Read(seq)
		if err != nil {
			t.Fatalf("node %d read seq %d: %v", rafts[0].id, seq, err)
		}
		for i := 1; i < len(logs); i++ {
			got, err := logs[i].Read(seq)
			if err != nil {
				t.Fatalf("node %d read seq %d: %v", rafts[i].id, seq, err)
			}
			if !bytes.Equal(ref, got) {
				t.Fatalf("event log diverged at wal seq %d: node %d (%d bytes) != node %d (%d bytes) — "+
					"determinism invariant violated after rebuild",
					seq, rafts[i].id, len(got), rafts[0].id, len(ref))
			}
		}
	}
}

// newSevereLagCluster builds a `replicas`-node raft cluster backed by
// real wal.Storage on disjoint temp directories, with every node sharing
// a single FaultyCluster so future scenarios can layer fault injection
// on top. The wal-backed storage is load-bearing for this scenario:
// raft_test's MemoryStorage hard-codes AppliedIndex/EventIndex to 0,
// which makes maybeCompact a no-op and the storage invariant trivially
// satisfied — neither of which exercises the rebuild path.
//
// Returns the Rafts slice, the shared FaultyCluster (so the test can
// partition all nodes before close to freeze the committed log), the
// per-node data directories (owned by t.TempDir() and auto-removed on
// test end), and a fatalCapture channel. Every node's logger is wired
// with zap.WithFatalHook so any logger.Fatal call — checkStorageInvariant
// in raft.go, processSnapshot's restore-failed fatal, etc — routes
// through the hook: the event is delivered on fatalC and runtime.Goexit
// terminates only the calling goroutine (typically serveChannels),
// leaving the test process alive to run the rebuild phase.
func newSevereLagCluster(t *testing.T, replicas int, opts []db.Option) (Rafts, *FaultyCluster, []string, chan fatalEvent) {
	t.Helper()

	ports := pickFreePorts(replicas)
	peers := make([]raft.Peer, replicas)
	for i := 0; i < replicas; i++ {
		id := uint64(i + 1)
		ctx := fmt.Sprintf("http://127.0.0.1:%d", ports[i])
		peers[i] = raft.Peer{ID: id, Context: []byte(ctx)}
	}

	fc := NewFaultyCluster(peers)
	fatalC := make(chan fatalEvent, replicas*4)

	dirs := make([]string, replicas)
	rafts := make(Rafts, replicas)
	for i := 0; i < replicas; i++ {
		dirs[i] = t.TempDir()
		rafts[i] = openWalRaft(t, peers[i].ID, peers, dirs[i], fc, opts, fatalC)
	}
	return rafts, fc, dirs, fatalC
}

// openWalRaft opens a fresh wal.Storage at `dir` (reading any pre-existing
// data if the directory was rsync'd into before this call) and constructs
// a db.Raft against it, wrapped in the shared FaultyCluster's transport.
// Returns the *Raft wrapper used by the rest of the adversarial suite.
//
// The sync/ingest channels are buffered but never drained here: the
// severe-lag scenario proposes raw user payloads (not syncable/ingestable
// configs), so ApplyCommitted's undecodable-proposal branch runs and the
// channels stay empty. A 64-entry buffer is ample headroom if a future
// caller wants to propose configs against this harness.
func openWalRaft(t *testing.T, id uint64, peers []raft.Peer, dir string, fc *FaultyCluster, opts []db.Option, fatalC chan<- fatalEvent) *Raft {
	t.Helper()

	p := parser.New()
	syncCh := make(chan *db.SyncableWithID, 64)
	ingestCh := make(chan *db.IngestableWithID, 64)
	s, err := wal.Open(dir, p, syncCh, ingestCh, wal.WithoutFsync(), wal.WithInMemoryTimeSeries())
	if err != nil {
		t.Fatalf("wal.Open(%s): %v", dir, err)
	}

	// Per-node logger with a fatal hook that posts to fatalC and calls
	// runtime.Goexit instead of the default os.Exit. Lets the test
	// observe the severe-lag fatal path (processSnapshot /
	// checkStorageInvariant) without terminating the test process.
	logger := newFatalCapturingLogger(id, fatalC)

	proposeC := make(chan []byte)
	confChangeC := make(chan raftpb.ConfChange)

	nodeOpts := []db.Option{
		db.WithTickInterval(multiNodeTickInterval),
		db.WithTransportWrapperForTest(fc.Wrap(id)),
		db.WithLogger(logger),
	}
	nodeOpts = append(nodeOpts, opts...)

	commitC, errorC, r := db.NewRaft(id, peers, s, proposeC, confChangeC, nodeOpts...)

	return &Raft{
		storage:       s,
		peers:         peers,
		commitC:       commitC,
		errorC:        errorC,
		raft:          r,
		proposeC:      proposeC,
		confChangeC:   confChangeC,
		id:            id,
		tickInterval:  multiNodeTickInterval,
		faultyCluster: fc,
	}
}

// rebootWalNode closes the old raft (if still open) and opens a new
// wal.Storage-backed raft at `dir`, reusing the *Raft wrapper so existing
// slice references stay valid. The caller is responsible for having
// already closed the old storage (which may have happened before an
// rsync overwrote the dir).
//
// Analogous to raft_test.go's (*Raft).Restart but rewires storage too,
// which Restart doesn't — Restart is built for the MemoryStorage case
// where the storage object survives across the restart.
func rebootWalNode(t *testing.T, rs *Raft, dir string, opts []db.Option, fatalC chan<- fatalEvent) {
	t.Helper()

	rs.mu.RLock()
	id := rs.id
	peers := rs.peers
	fc := rs.faultyCluster
	rs.mu.RUnlock()

	rebuilt := openWalRaft(t, id, peers, dir, fc, opts, fatalC)

	rs.mu.Lock()
	rs.storage = rebuilt.storage
	rs.commitC = rebuilt.commitC
	rs.errorC = rebuilt.errorC
	rs.raft = rebuilt.raft
	rs.proposeC = rebuilt.proposeC
	rs.confChangeC = rebuilt.confChangeC
	rs.mu.Unlock()
}

// copyTree replicates src's directory structure and every file under
// it into dst. Mirrors the copyDir helper in wal/rebuild_test.go; kept
// local rather than exported so the wal test-only helper stays
// package-private.
func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, info.Mode())
	})
	if err != nil {
		t.Fatalf("copyTree %s -> %s: %v", src, dst, err)
	}
}

// waitForLeaderExtended is like Rafts.WaitForLeader but with a caller-
// controlled timeout. The post-rebuild phase of the severe-lag scenario
// restarts two of three nodes concurrently; the full convergence cost
// (TCP listener rebind + raft election + AppendEntries catchup across
// every surviving pair interaction) occasionally clips the default 5s
// WaitForLeader budget. Extending the window here is cheaper than
// plumbing an Option through the whole test harness.
func waitForLeaderExtended(t *testing.T, rs Rafts, deadline time.Duration) {
	t.Helper()
	stop := time.Now().Add(deadline)
	for {
		if _, ok := rs.stableLeader(); ok {
			return
		}
		if time.Now().After(stop) {
			for _, r := range rs {
				t.Logf("node %d: Leader()=%d", r.id, r.Leader())
			}
			t.Fatalf("waitForLeaderExtended: no stable leader within %v", deadline)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitForSurvivorConvergence blocks until every node in `nodes` reports
// the same AppliedIndex AND every node's EventIndex == AppliedIndex.
// This is the cluster-is-quiesced barrier the severe-lag scenario needs
// before closing a node for rsync: without it, in-flight empty-leader
// entries or commit-bump deltas can leave one survivor's events/
// transiently ahead of the other, which surfaces as a spurious
// determinism failure after rebuild.
func waitForSurvivorConvergence(t *testing.T, nodes Rafts, deadline time.Duration) {
	t.Helper()
	stop := time.Now().Add(deadline)
	for {
		ref := nodes[0].storage.AppliedIndex()
		stable := true
		for _, r := range nodes {
			a := r.storage.AppliedIndex()
			p := r.storage.EventIndex()
			if a != ref || p != a {
				stable = false
				break
			}
		}
		if stable {
			return
		}
		if time.Now().After(stop) {
			for _, r := range nodes {
				t.Logf("node %d: EventIndex=%d AppliedIndex=%d",
					r.id, r.storage.EventIndex(), r.storage.AppliedIndex())
			}
			t.Fatal("survivors did not converge to matching AppliedIndex within deadline")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// fatalEvent records a single zap.Fatal call captured via the
// test-only fatal hook. The test waits on a channel of fatalEvents
// to confirm that the severe-lag fatal-exit path was actually
// reached — without the hook the logger.Fatal call would os.Exit
// and take the whole test binary with it.
type fatalEvent struct {
	nodeID  uint64
	message string
}

// fatalHook implements zapcore.CheckWriteHook. It posts a fatalEvent
// to its channel (non-blocking; the channel is buffered) and then
// calls runtime.Goexit to terminate the calling goroutine. Only the
// goroutine that invoked Fatal exits — the rest of the test process,
// including Close()-time cleanup for the dying node, keeps running.
type fatalHook struct {
	nodeID uint64
	c      chan<- fatalEvent
}

func (h *fatalHook) OnWrite(ce *zapcore.CheckedEntry, _ []zapcore.Field) {
	select {
	case h.c <- fatalEvent{nodeID: h.nodeID, message: ce.Message}:
	default:
	}
	runtime.Goexit()
}

// newFatalCapturingLogger returns a zap.Logger whose Fatal calls route
// through fatalHook instead of os.Exit. The underlying core is a
// NopCore so log output never surfaces in test runs; only the fatal
// hook is observable. The node ID is baked into the hook so the test
// can assert WHICH node fatal-exited.
func newFatalCapturingLogger(nodeID uint64, c chan<- fatalEvent) *zap.Logger {
	return zap.New(zapcore.NewNopCore(), zap.WithFatalHook(&fatalHook{nodeID: nodeID, c: c}))
}

// wallog is a thin shim around tidwall/wal.Log used only by
// assertEventLogPrefixMatches so the test can read raw event log bytes
// by sequence number without going through wal.Storage (which would
// want a parser and sync/ingest channels and run bbolt migrations).
type wallog struct {
	l *tidwallwal.Log
}

func openRawWalLog(dir string) (*wallog, error) {
	l, err := tidwallwal.Open(dir, nil)
	if err != nil {
		return nil, err
	}
	return &wallog{l: l}, nil
}

func (w *wallog) Close() error          { return w.l.Close() }
func (w *wallog) LastSeq() (uint64, error) {
	return w.l.LastIndex()
}
func (w *wallog) Read(seq uint64) ([]byte, error) {
	return w.l.Read(seq)
}
