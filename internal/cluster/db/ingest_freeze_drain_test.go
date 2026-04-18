package db_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
	parser "github.com/philborlin/committed/internal/cluster/db/parser"
	"github.com/philborlin/committed/internal/cluster/metrics"
)

// TestIngestFreezeDrain_AppliedPath covers the happy drain branch:
// an in-flight position bump whose waiter is signaled nil (applied)
// before the drain deadline. The drain consumes the ack, unregisters
// the waiter, and does not emit the timeout metric.
func TestIngestFreezeDrain_AppliedPath(t *testing.T) {
	d, reader := newDrainTestDB(t)

	const rid uint64 = 9001
	ack := d.RegisterWaiterForTest(rid, 1)
	d.SignalWaiterForTest(rid, nil) // apply-path win

	d.DrainInflightBumpsForTest(
		map[uint64]<-chan error{rid: ack},
		200*time.Millisecond,
		"applied-id",
	)

	require.Empty(t, d.WaitersForTest(), "drain must unregister the waiter")
	require.Zero(t, drainTimeoutCount(t, reader, "applied-id"),
		"applied path must not record a drain timeout")
}

// TestIngestFreezeDrain_LostPath covers the alternate resolution:
// the waiter was signaled ErrProposalUnknown by the leader-change
// watcher (bump may have been truncated). Drain consumes the ack
// just as for the applied path — both outcomes leave the worker's
// subsequent post-freeze position consistent from the supervisor's
// point of view.
func TestIngestFreezeDrain_LostPath(t *testing.T) {
	d, reader := newDrainTestDB(t)

	const rid uint64 = 9002
	ack := d.RegisterWaiterForTest(rid, 1)
	d.SignalWaiterForTest(rid, db.ErrProposalUnknown)

	d.DrainInflightBumpsForTest(
		map[uint64]<-chan error{rid: ack},
		200*time.Millisecond,
		"lost-id",
	)

	require.Empty(t, d.WaitersForTest())
	require.Zero(t, drainTimeoutCount(t, reader, "lost-id"),
		"lost path must not record a drain timeout (ack was consumed within deadline)")
}

// TestIngestFreezeDrain_TimeoutPath covers the pathological case: a
// bump whose waiter never resolves within the drain budget. The
// drain gives up, unregisters the waiter anyway (leak prevention),
// and emits the IngestFreezeDrainTimeout metric so operators can
// see that the supervisor may subsequently read a stale position.
func TestIngestFreezeDrain_TimeoutPath(t *testing.T) {
	d, reader := newDrainTestDB(t)

	const rid uint64 = 9003
	ack := d.RegisterWaiterForTest(rid, 1)
	// Intentionally NOT signaled — drain must hit the deadline.

	start := time.Now()
	d.DrainInflightBumpsForTest(
		map[uint64]<-chan error{rid: ack},
		30*time.Millisecond,
		"timeout-id",
	)
	elapsed := time.Since(start)

	require.GreaterOrEqual(t, elapsed, 30*time.Millisecond,
		"drain must wait out the full deadline on timeout")
	require.Empty(t, d.WaitersForTest(),
		"drain must unregister the waiter even on timeout (leak prevention)")

	require.Eventually(t, func() bool {
		return drainTimeoutCount(t, reader, "timeout-id") >= 1
	}, 2*time.Second, 10*time.Millisecond,
		"timeout metric must fire when drain gives up on unresolved acks")
}

// TestIngestFreezeDrain_Empty is a degenerate but worth-covering
// case: no in-flight bumps. Drain must be a cheap no-op without
// starting the deadline timer (we should not pay timer setup cost
// on every freeze in the common "nothing in flight" case).
func TestIngestFreezeDrain_Empty(t *testing.T) {
	d, reader := newDrainTestDB(t)

	start := time.Now()
	d.DrainInflightBumpsForTest(
		map[uint64]<-chan error{},
		5*time.Second, // would dominate test runtime if drain started the timer
		"empty-id",
	)
	elapsed := time.Since(start)

	require.Less(t, elapsed, 100*time.Millisecond,
		"empty drain must return immediately, not wait on any deadline")
	require.Empty(t, d.WaitersForTest())
	require.Zero(t, drainTimeoutCount(t, reader, "empty-id"))
}

// TestIngest_FreezeNoWaiterLeak verifies the end-to-end cleanup
// invariant through the worker's actual freeze path: after inducing
// a freeze (and subsequent supervisor restart, which is effectively
// disabled via a very long backoff), no waiter entries remain in
// db.waiters. This catches regressions where the drain-on-exit
// defer in db.ingest fails to unregister tracked bumps, or where
// Propose's own defer skips cleanup on the ErrProposalUnknown
// return path.
func TestIngest_FreezeNoWaiterLeak(t *testing.T) {
	d, s := newIngestFailFastDB(t)

	require.Eventually(t,
		func() bool { return d.ObservedLeaderForTest() == 1 },
		2*time.Second, 2*time.Millisecond,
	)

	proposal := &cluster.Proposal{Entities: []*cluster.Entity{{
		Type: &cluster.Type{ID: "string"}, Key: []byte("k"), Data: []byte("v"),
	}}}
	ing := newFreezeRecordingIngestable(proposal, cluster.Position([]byte("pos")))

	require.NoError(t, d.Ingest(context.Background(), "leak-id", ing))
	rid := d.WaitForAnyWaiterForTest(2 * time.Second)
	require.NotZero(t, rid)

	d.SignalWaiterForTest(rid, db.ErrProposalUnknown)

	// Give the worker a moment to run through the freeze branch
	// (drain is a no-op here because no position bump was ever
	// submitted — worker blocked in Propose(pr1) for the whole test)
	// and return ingestExitFreeze. The supervisor backoff is 1h per
	// newIngestFailFastDB, so it won't restart during teardown.
	require.Eventually(t, func() bool {
		for _, w := range d.WaitersForTest() {
			if w == rid {
				return false
			}
		}
		return true
	}, 2*time.Second, 10*time.Millisecond,
		"Propose's defer never unregistered the frozen waiter")

	s.Unblock()
	require.NoError(t, d.Close())

	require.Empty(t, d.WaitersForTest(),
		"waiters map must be empty after Close")
}

// newDrainTestDB returns a minimal single-node DB + a metric reader,
// sized for drain-unit tests that don't exercise any ingest or sync
// plumbing. The supervisor is effectively disabled (long backoffs)
// and apply runs on a plain MemoryStorage with no stall — the DB
// exists only so RegisterWaiterForTest / unregisterWaiter have a
// working waiter map to mutate.
func newDrainTestDB(t *testing.T) (*db.DB, *sdkmetric.ManualReader) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	m := metrics.New(provider.Meter("test"))

	id := uint64(1)
	peers := make(db.Peers)
	peers[id] = ""
	p := parser.New()
	s := NewMemoryStorage()
	s.SetNode(id)

	d := db.New(id, peers, s, p, nil, nil,
		db.WithTickInterval(testTickInterval),
		db.WithMetrics(m),
	)
	t.Cleanup(func() { _ = d.Close() })
	return d, reader
}

func drainTimeoutCount(t *testing.T, reader *sdkmetric.ManualReader, id string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	return findSupervisorCounterForID(rm, "committed.ingest.freeze_drain_timeout_total", id)
}
