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
	"github.com/philborlin/committed/internal/cluster/metrics"
)

// TestIngest_SupervisorRestartsAfterFreeze exercises the happy-path
// auto-recovery: a worker observes ErrProposalUnknown, exits with
// ingestExitFreeze, and the supervisor re-registers the ingestable
// via db.Ingest. We detect the restart by counting how many times
// cluster.Ingestable.Ingest was invoked on the same instance — the
// initial Ingest call sets it to 1, a supervisor-driven restart
// raises it to 2.
//
// The metric side-effects (frozen → 1 on freeze, frozen → 0 on
// successful restart, restart_total incremented) are collected via
// an OTel ManualReader so we can assert the operator-visible
// observability story too.
func TestIngest_SupervisorRestartsAfterFreeze(t *testing.T) {
	id := "supervised-ingest"

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { provider.Shutdown(context.Background()) })
	m := metrics.New(provider.Meter("test"))

	d, s := newIngestFailFastDBWith(t,
		db.WithMetrics(m),
		db.WithIngestSupervisorInitialBackoff(1*time.Millisecond),
		db.WithIngestSupervisorMaxBackoff(5*time.Millisecond),
	)

	require.Eventually(t,
		func() bool { return d.ObservedLeaderForTest() == 1 },
		2*time.Second, 2*time.Millisecond,
	)

	proposal := &cluster.Proposal{
		Entities: []*cluster.Entity{{
			Type: &cluster.Type{ID: "string"},
			Key:  []byte("k"),
			Data: []byte("v"),
		}},
	}
	ing := newFreezeRecordingIngestable(proposal, cluster.Position([]byte("pos-1")))

	require.NoError(t, d.Ingest(context.Background(), id, ing))

	// Initial registration calls Ingest exactly once.
	require.Eventually(t, func() bool { return ing.IngestCalls() >= 1 },
		2*time.Second, 5*time.Millisecond,
		"initial ingestable.Ingest never invoked")

	rid := d.WaitForAnyWaiterForTest(2 * time.Second)
	require.NotZero(t, rid, "ingest worker never registered a Propose waiter")

	// Inject freeze.
	d.SignalWaiterForTest(rid, db.ErrProposalUnknown)

	// Supervisor should restart the ingestable. Second Ingest call is
	// our signal — the ingestable's state (posMu.ingestCalls) is the
	// most direct evidence that a fresh worker started.
	require.Eventually(t, func() bool { return ing.IngestCalls() >= 2 },
		5*time.Second, 10*time.Millisecond,
		"supervisor did not restart the ingestable within the deadline",
	)

	// Collect metrics and verify the frozen gauge flipped to 0 (i.e.,
	// the supervisor's successful-restart path ran) and that the
	// restart counter recorded the event.
	require.Eventually(t, func() bool {
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			return false
		}
		frozen := findSupervisorGaugeForID(rm, "committed.ingest.frozen", id)
		restart := findSupervisorCounterForID(rm, "committed.ingest.restart_total", id)
		return frozen == 0.0 && restart >= 1
	}, 5*time.Second, 25*time.Millisecond,
		"expected frozen=0 and restart_total>=1 for id=%s", id,
	)

	// Teardown: unblock raft's apply path so the restart worker's
	// in-flight Propose can complete, then Close cleanly.
	s.Unblock()
	require.NoError(t, d.Close())
}

// TestIngest_SupervisorBailsWhenUserReplaces covers the race between a
// supervisor-driven restart and a user-initiated replace. With a long
// supervisor backoff (200ms), the test signals ErrProposalUnknown to
// drive worker A into ingestExitFreeze, then calls db.Ingest(id, B)
// before the supervisor's backoff expires. When the supervisor
// eventually wakes, its preflight sees B in the registry (not A's
// frozen handle) and must bail — otherwise its db.Ingest call would
// tear down B and reinstall A.
//
// This exercises the lock-held check+install fix. Prior to it, the
// supervisor dropped the lock between preflight and install, opening
// a window where the supervisor could overwrite a user replacement.
func TestIngest_SupervisorBailsWhenUserReplaces(t *testing.T) {
	id := "race-ingest"

	d, s := newIngestFailFastDBWith(t,
		db.WithIngestSupervisorInitialBackoff(200*time.Millisecond),
		db.WithIngestSupervisorMaxBackoff(500*time.Millisecond),
	)

	require.Eventually(t,
		func() bool { return d.ObservedLeaderForTest() == 1 },
		2*time.Second, 2*time.Millisecond,
	)

	proposalA := &cluster.Proposal{Entities: []*cluster.Entity{{
		Type: &cluster.Type{ID: "string"}, Key: []byte("k"), Data: []byte("va"),
	}}}
	proposalB := &cluster.Proposal{Entities: []*cluster.Entity{{
		Type: &cluster.Type{ID: "string"}, Key: []byte("k"), Data: []byte("vb"),
	}}}

	ingA := newFreezeRecordingIngestable(proposalA, cluster.Position([]byte("pos-a")))
	ingB := newFreezeRecordingIngestable(proposalB, cluster.Position([]byte("pos-b")))

	require.NoError(t, d.Ingest(context.Background(), id, ingA))
	rid := d.WaitForAnyWaiterForTest(2 * time.Second)
	require.NotZero(t, rid, "ingest worker never registered a Propose waiter")

	// Induce freeze. Worker A returns ingestExitFreeze; supervisor
	// spawns with 200ms backoff. The outer goroutine has closed
	// handle_A.done before spawning the supervisor, so the next
	// db.Ingest(id, ...) can make progress through its replace loop.
	d.SignalWaiterForTest(rid, db.ErrProposalUnknown)

	// Replace A with B while the supervisor's backoff is still in
	// flight. db.Ingest's replace loop waits on handle_A.done (already
	// closed), then installs B in the registry.
	require.NoError(t, d.Ingest(context.Background(), id, ingB))

	// Wait past the supervisor's backoff plus margin. With the
	// lock-held fix, the supervisor's preflight observes B's handle
	// (≠ frozen A) and bails. Without the fix, the supervisor's
	// db.Ingest call would cancel B and reinstall A — A.IngestCalls
	// would rise to 2.
	time.Sleep(400 * time.Millisecond)

	require.Equal(t, 1, ingA.IngestCalls(),
		"supervisor re-invoked A's Ingest despite user replace to B",
	)
	require.GreaterOrEqual(t, ingB.IngestCalls(), 1,
		"B's Ingest was never invoked after user replace",
	)

	// Teardown: unblock apply so Close's serveChannels drain can
	// complete, then Close.
	s.Unblock()
	require.NoError(t, d.Close())
}

// TestIngestSupervisor_BackoffAndGiveup exercises the supervisor's
// consecutive-freeze decision function directly. A full-stack
// freeze-repeatedly test is awkward to synchronize because once the
// slow-apply harness stalls raft's Ready loop, subsequent Propose
// calls are stuck on the proposeC send (before the ack channel) and
// ErrProposalUnknown injection can't complete them. Unit-testing the
// decision function gives us precise coverage of the backoff ladder
// and the giveup threshold without that coupling.
func TestIngestSupervisor_BackoffAndGiveup(t *testing.T) {
	const maxAttempts = 3

	d, _ := newIngestFailFastDBWith(t,
		db.WithIngestSupervisorInitialBackoff(5*time.Millisecond),
		db.WithIngestSupervisorMaxBackoff(40*time.Millisecond),
		db.WithIngestSupervisorMaxAttempts(maxAttempts),
		db.WithIngestSupervisorHealthyWindow(1*time.Hour),
	)
	t.Cleanup(func() { _ = d.Close() })

	id := "bookkeeping-id"

	// First maxAttempts freezes: supervisor schedules a restart, not a
	// giveup. Backoff doubles each time until it hits the max cap.
	expectedBackoffs := []time.Duration{
		5 * time.Millisecond,  // 1st
		10 * time.Millisecond, // 2nd
		20 * time.Millisecond, // 3rd
	}
	for i, want := range expectedBackoffs {
		backoff, consecutive, giveup := d.RecordFreezeAndNextBackoffForTest(id)
		require.Falsef(t, giveup, "unexpected giveup at consecutive=%d", i+1)
		require.Equal(t, i+1, consecutive)
		require.Equal(t, want, backoff, "backoff ladder mismatch at consecutive=%d", i+1)
	}

	// (maxAttempts+1)th observation triggers giveup. Backoff value is
	// irrelevant on giveup, but consecutive keeps climbing so operators
	// can see in debug logs how deep the flap ran.
	_, consecutive, giveup := d.RecordFreezeAndNextBackoffForTest(id)
	require.True(t, giveup, "expected giveup at consecutive=%d", consecutive)
	require.Equal(t, maxAttempts+1, consecutive)
}

// TestIngestSupervisor_HealthyWindowResetsCounter verifies that a
// freeze observed after HealthyWindow has elapsed since the last one
// resets the consecutive count to 1. This prevents a long-running
// ingestable that occasionally encounters an ErrProposalUnknown (a
// routine leader hand-off a day later) from eventually tripping the
// giveup cap on cumulative, non-consecutive flaps.
func TestIngestSupervisor_HealthyWindowResetsCounter(t *testing.T) {
	d, _ := newIngestFailFastDBWith(t,
		db.WithIngestSupervisorInitialBackoff(5*time.Millisecond),
		db.WithIngestSupervisorMaxBackoff(40*time.Millisecond),
		db.WithIngestSupervisorMaxAttempts(10),
		db.WithIngestSupervisorHealthyWindow(20*time.Millisecond),
	)
	t.Cleanup(func() { _ = d.Close() })

	id := "reset-id"

	// Two back-to-back freezes → consecutive=1, then 2. Backoff grows.
	_, c1, _ := d.RecordFreezeAndNextBackoffForTest(id)
	require.Equal(t, 1, c1)
	b2, c2, _ := d.RecordFreezeAndNextBackoffForTest(id)
	require.Equal(t, 2, c2)
	require.Equal(t, 10*time.Millisecond, b2)

	// Wait past HealthyWindow so the next observation is "fresh".
	time.Sleep(30 * time.Millisecond)

	b3, c3, giveup := d.RecordFreezeAndNextBackoffForTest(id)
	require.False(t, giveup)
	require.Equal(t, 1, c3, "expected consecutive counter to reset after healthy window")
	require.Equal(t, 5*time.Millisecond, b3, "expected backoff to reset to initial value")
}

// findSupervisorGaugeForID looks up a gauge metric by name and returns
// the data point whose "id" attribute matches id. Returns math.NaN-
// like sentinel (-1) when not found so the caller's Eventually loop
// treats "not yet recorded" as "keep polling".
func findSupervisorGaugeForID(rm metricdata.ResourceMetrics, name, id string) float64 {
	met := findMetric(rm, name)
	if met == nil {
		return -1
	}
	g, ok := met.Data.(metricdata.Gauge[float64])
	if !ok {
		return -1
	}
	for _, dp := range g.DataPoints {
		for _, a := range dp.Attributes.ToSlice() {
			if string(a.Key) == "id" && a.Value.AsString() == id {
				return dp.Value
			}
		}
	}
	return -1
}

// findSupervisorCounterForID looks up an Int64 counter by name and
// returns the value whose "id" attribute matches id. Returns 0 when
// no matching data point exists so callers can use >= comparisons
// directly.
func findSupervisorCounterForID(rm metricdata.ResourceMetrics, name, id string) int64 {
	met := findMetric(rm, name)
	if met == nil {
		return 0
	}
	sum, ok := met.Data.(metricdata.Sum[int64])
	if !ok {
		return 0
	}
	for _, dp := range sum.DataPoints {
		for _, a := range dp.Attributes.ToSlice() {
			if string(a.Key) == "id" && a.Value.AsString() == id {
				return dp.Value
			}
		}
	}
	return 0
}

