package db_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/philborlin/committed/internal/cluster/db"
	parser "github.com/philborlin/committed/internal/cluster/db/parser"
	"github.com/philborlin/committed/internal/cluster/metrics"
)

// failFastGrace is short enough to keep these tests fast but long enough
// that go test's scheduler jitter on a loaded machine can't push the
// grace timer's fire before the transition's at-risk sweep runs.
const failFastGrace = 50 * time.Millisecond

// newFailFastDB builds a single-node DB tuned for the fail-fast tests:
// short grace period, short tick for a fast self-election.
//
// Tests that drive SetLeaderIDForTest must first wait for the Ready
// loop's initial 0→1 transition to settle (via waitForStableLeader).
// Otherwise the race between the test's manual SetLeaderID and raft's
// own post-election Ready iterations can synthesize an intermediate
// "transition through 0" that marks any waiter stamped >0 at risk and
// confuses the assertions.
func newFailFastDB(t *testing.T, opts ...db.Option) *db.DB {
	t.Helper()
	id := uint64(1)
	peers := make(db.Peers)
	peers[id] = ""
	p := parser.New()
	s := NewMemoryStorage()

	combined := append([]db.Option{
		db.WithTickInterval(testTickInterval),
		db.WithLeaderChangeGracePeriod(failFastGrace),
	}, opts...)

	d := db.New(id, peers, s, p, nil, nil, combined...)
	t.Cleanup(func() { d.Close() })
	return d
}

// waitForStableLeader blocks until the leader-ID recorded inside the
// DB's LeaderState is 1. After this returns, subsequent Ready
// iterations call SetLeaderID(1) which is a no-op (same value → no
// transition), so tests can drive SetLeaderIDForTest without racing
// raft's own leader-ID updates.
//
// We deliberately read ObservedLeaderForTest rather than Leader().
// Leader() reads raft.Node.Status().Lead, which can report the elected
// leader one Ready iteration before LeaderState catches up. If a test
// drives SetLeaderIDForTest during that gap, it synthesizes a 0→N
// transition that the "initial election" skip short-circuits, which
// silently breaks the at-risk sweep the test is trying to exercise.
func waitForStableLeader(t *testing.T, d *db.DB) {
	t.Helper()
	require.Eventually(t,
		func() bool { return d.ObservedLeaderForTest() == 1 },
		2*time.Second,
		2*time.Millisecond,
		"single-node raft never elected itself leader (observed from LeaderState)",
	)
}

// TestPropose_FailFast_TransitionSignalsWaiter drives the canonical
// path: a waiter stamped under the current leader observes a
// transition to a different leader, and after the grace period the
// watcher signals it with ErrProposalUnknown.
func TestPropose_FailFast_TransitionSignalsWaiter(t *testing.T) {
	d := newFailFastDB(t)
	waitForStableLeader(t, d)

	// Use a RequestID well above any the real Propose path would
	// assign during this test so nothing else can race to satisfy the
	// waiter. nextRequestID starts at 0 and increments per real
	// Propose; 999 leaves plenty of headroom.
	const rid uint64 = 999
	ack := d.RegisterWaiterForTest(rid, 1)
	t.Cleanup(func() { d.UnregisterWaiterForTest(rid) })

	// Transition to a different leader ID. Waiter stamped under 1 is
	// at risk; grace timer starts. raft's subsequent SetLeaderID(1)
	// from its own Ready loop transitions back to 1, but that doesn't
	// mark the stamped-1 waiter at risk (stamp matches new), so no
	// duplicate grace fires.
	d.SetLeaderIDForTest(2)

	select {
	case err := <-ack:
		require.ErrorIs(t, err, db.ErrProposalUnknown)
	case <-time.After(failFastGrace * 5):
		t.Fatal("waiter never received ErrProposalUnknown")
	}
}

// TestPropose_FailFast_AppliesInGraceReturnsNil covers the
// "new leader inherited and committed the entry fast" case: a
// transition is observed, but the apply path signals the waiter
// before the grace timer fires. The caller sees nil (not
// ErrProposalUnknown).
func TestPropose_FailFast_AppliesInGraceReturnsNil(t *testing.T) {
	// Use a longer grace so we can deterministically simulate apply
	// happening well within the window regardless of scheduler noise.
	d := newFailFastDB(t, db.WithLeaderChangeGracePeriod(500*time.Millisecond))
	waitForStableLeader(t, d)

	const rid uint64 = 1001
	ack := d.RegisterWaiterForTest(rid, 1)

	// Transition → watcher snapshots atRisk, arms 500ms grace timer.
	d.SetLeaderIDForTest(2)

	// Simulate apply firing well inside the grace window. The
	// buffered ack chan gets nil; the watcher's later send hits the
	// default branch and is dropped.
	time.Sleep(20 * time.Millisecond)
	d.SignalWaiterForTest(rid, nil)

	select {
	case err := <-ack:
		require.NoError(t, err, "apply beat the grace timer — expected nil")
	case <-time.After(time.Second):
		t.Fatal("waiter never received any signal")
	}

	// Mirror Propose's defer: the real Propose removes the waiter
	// from the map immediately after receiving the first signal, so
	// the watcher's grace goroutine finds it missing and skips.
	// RegisterWaiterForTest doesn't emulate that defer for us — do
	// it manually so the next assertion reflects production shape.
	d.UnregisterWaiterForTest(rid)

	// Sleep past the grace period to confirm the watcher does NOT
	// subsequently fire a spurious ErrProposalUnknown on the
	// now-unregistered waiter.
	time.Sleep(600 * time.Millisecond)
	select {
	case extra := <-ack:
		t.Fatalf("waiter received unexpected extra signal: %v", extra)
	default:
	}
}

// TestPropose_FailFast_NoTransitionPathUnaffected sanity-checks the
// default case: without any test-driven transition, a normal Propose
// still returns nil. Guards against a regression where fail-fast
// wiring accidentally poisons the steady-state path.
func TestPropose_FailFast_NoTransitionPathUnaffected(t *testing.T) {
	d := newFailFastDB(t)

	ps := createProposals([][]string{{"x"}, {"y"}})
	for _, p := range ps {
		err := d.Propose(testCtx(t), p)
		require.NoError(t, err)
	}
}

// TestPropose_FailFast_StableLeaderDoesNotSignal confirms that a
// waiter stamped under the current leader is NOT signaled when no
// transition away from that leader occurs. After single-node election
// stabilizes, raft keeps calling SetLeaderID(1) which is a no-op —
// the watcher sees no transitions, so the waiter stays quiet.
func TestPropose_FailFast_StableLeaderDoesNotSignal(t *testing.T) {
	d := newFailFastDB(t)
	waitForStableLeader(t, d)

	const rid uint64 = 2001
	ack := d.RegisterWaiterForTest(rid, 1)
	t.Cleanup(func() { d.UnregisterWaiterForTest(rid) })

	// No SetLeaderIDForTest. Wait comfortably past the grace period.
	// If the waiter is signaled here, the watcher is spuriously
	// firing on no-op SetLeaderID(1) calls from the Ready loop.
	select {
	case err := <-ack:
		t.Fatalf("unexpected signal on stable-leader waiter: %v", err)
	case <-time.After(failFastGrace * 4):
	}
}

// TestPropose_FailFast_MetricsIncrement verifies both counters fire
// through the full transition+grace path: the transitions counter
// ticks up (at least once for our manual transition; the initial
// election also contributes), and the fail-fast counter ticks up
// exactly once for the single waiter we signaled.
func TestPropose_FailFast_MetricsIncrement(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	m := metrics.New(provider.Meter("test"))
	d := newFailFastDB(t, db.WithMetrics(m))
	waitForStableLeader(t, d)

	const rid uint64 = 3001
	ack := d.RegisterWaiterForTest(rid, 1)
	t.Cleanup(func() { d.UnregisterWaiterForTest(rid) })

	d.SetLeaderIDForTest(2)

	select {
	case err := <-ack:
		require.ErrorIs(t, err, db.ErrProposalUnknown)
	case <-time.After(failFastGrace * 5):
		t.Fatal("waiter never received ErrProposalUnknown")
	}

	// Give raft's subsequent Ready iteration time to re-assert
	// leaderID=1. That 2→1 transition doesn't re-mark our waiter
	// (stamp=1=new), so the fail-fast counter stays at 1.
	time.Sleep(failFastGrace * 2)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	transitions := findMetric(rm, "committed.leader.transitions.observed")
	require.NotNil(t, transitions, "transitions counter missing")
	transSum, ok := transitions.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	// ≥1: initial election (0→1) + our manual transition (1→2) +
	// raft's snap-back (2→1). We assert lower-bound only because the
	// exact count depends on scheduling.
	require.GreaterOrEqual(t, sumInt64(transSum), int64(1))

	failFast := findMetric(rm, "committed.propose.fail_fast.unknown")
	require.NotNil(t, failFast, "fail-fast counter missing")
	ffSum, ok := failFast.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Equal(t, int64(1), sumInt64(ffSum))
}

func sumInt64(s metricdata.Sum[int64]) int64 {
	var total int64
	for _, dp := range s.DataPoints {
		total += dp.Value
	}
	return total
}

// TestLeaderState_SubscribeObservesTransitions exercises the new
// SetLeaderID / Subscribe plumbing in isolation. No DB needed.
func TestLeaderState_SubscribeObservesTransitions(t *testing.T) {
	l := &db.LeaderState{}

	ch := l.Subscribe(4)

	l.SetLeaderID(1)
	l.SetLeaderID(1) // duplicate — must NOT dispatch
	l.SetLeaderID(2)
	l.SetLeaderID(0) // transition back to "unknown" — DOES dispatch

	got := drainTransitions(t, ch, 3, 200*time.Millisecond)
	require.Equal(t, []db.LeaderTransition{
		{Old: 0, New: 1},
		{Old: 1, New: 2},
		{Old: 2, New: 0},
	}, got)

	require.Equal(t, uint64(0), l.Leader())
}

// TestLeaderState_SubscribeSlowConsumerDropsTransitions confirms a
// slow subscriber does not block SetLeaderID (which is called from
// the raft Ready loop and must not stall). Excess transitions are
// dropped rather than queued indefinitely.
func TestLeaderState_SubscribeSlowConsumerDropsTransitions(t *testing.T) {
	l := &db.LeaderState{}
	ch := l.Subscribe(2) // small buffer to force overflow

	// Fire more distinct transitions than the buffer holds.
	for i := uint64(1); i <= 10; i++ {
		l.SetLeaderID(i)
	}

	// Buffer holds at most 2 → we can drain at most 2 without
	// blocking. Assert no further transitions arrive once the buffer
	// empties (confirming the sender's default branch dropped them
	// rather than blocking).
	drained := 0
	for range 2 {
		select {
		case <-ch:
			drained++
		case <-time.After(50 * time.Millisecond):
		}
	}
	require.LessOrEqual(t, drained, 2)

	select {
	case tr := <-ch:
		t.Fatalf("drained more than buffer: extra transition %+v", tr)
	case <-time.After(50 * time.Millisecond):
	}
}

func drainTransitions(t *testing.T, ch <-chan db.LeaderTransition, expected int, timeout time.Duration) []db.LeaderTransition {
	t.Helper()
	out := make([]db.LeaderTransition, 0, expected)
	deadline := time.Now().Add(timeout)
	for len(out) < expected {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("timed out waiting for %d transitions, got %d: %+v", expected, len(out), out)
		}
		select {
		case tr := <-ch:
			out = append(out, tr)
		case <-time.After(remaining):
			t.Fatalf("timed out waiting for %d transitions, got %d: %+v", expected, len(out), out)
		}
	}
	return out
}
