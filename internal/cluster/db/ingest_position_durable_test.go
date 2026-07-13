package db_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
	"github.com/committeddb/committed/internal/cluster/metrics"
)

// positionProbeIngestable emits interleaved (proposal, position) pairs
// and, right after each proposal hand-off, records what storage.Position
// currently reports. The unbuffered proposalChan means a `pr <- proposal`
// send only returns once the worker has come back around to its select
// and received it — which, with blocking position bumps, happens only
// AFTER the previous position's bump has durably applied. So the value
// recorded at proposal k must be the position emitted at k-1. With the
// old fire-and-forget bump the worker looped back immediately and the
// recorded value would still be the older (or nil) position.
type positionProbeIngestable struct {
	s         *wal.Storage
	id        string
	proposals []*cluster.Proposal
	positions []cluster.Position

	mu       sync.Mutex
	observed []string
}

func (p *positionProbeIngestable) Ingest(ctx context.Context, _ cluster.Position, pr chan<- *cluster.Proposal, po chan<- cluster.Position) error {
	for i := range p.proposals {
		select {
		case pr <- p.proposals[i]:
		case <-ctx.Done():
			return nil
		}
		// The send above returned, so the worker has received proposal
		// i — meaning it already finished applying position i-1's bump.
		if i > 0 {
			p.mu.Lock()
			p.observed = append(p.observed, string(p.s.Position(p.id)))
			p.mu.Unlock()
		}
		select {
		case po <- p.positions[i]:
		case <-ctx.Done():
			return nil
		}
	}
	<-ctx.Done()
	return nil
}

func (p *positionProbeIngestable) Close() error { return nil }

func (p *positionProbeIngestable) recorded() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.observed))
	copy(out, p.observed)
	return out
}

// seedIngestableConfig persists a minimal ingestable config for id so a test
// that injects a fake worker via db.Ingest also establishes the production
// invariant the position guard depends on: an IngestablePosition bump only
// persists while its config exists in bbolt (see saveIngestablePosition). The
// build degrades under newWalDB's no-ingestable parser, which is fine —
// saveIngestable persists the config bytes before the local build.
func seedIngestableConfig(t *testing.T, d *db.DB, id string) {
	t.Helper()
	e, err := cluster.NewUpsertIngestableEntity(&cluster.Configuration{
		ID: id, MimeType: "application/json", Data: []byte("{}"),
	})
	require.NoError(t, err)
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{e}}))
}

// TestIngestPosition_DurableBeforeNextIteration proves the core
// guarantee: a position bump is durably applied before the worker
// processes the next upstream event. The probe records storage.Position
// after each proposal hand-off; with the blocking bump, the value seen
// at proposal k is exactly the position checkpointed after proposal k-1.
func TestIngestPosition_DurableBeforeNextIteration(t *testing.T) {
	d, s := newWalDB(t)
	id := "durable-ingest"
	seedIngestableConfig(t, d, id)

	const n = 4
	proposeTypeTOML(t, d, "evt", "evt", "", "")
	typ, err := s.ResolveType(cluster.LatestTypeRef("evt"))
	require.NoError(t, err)

	proposals := make([]*cluster.Proposal, n)
	positions := make([]cluster.Position, n)
	for i := range proposals {
		proposals[i] = &cluster.Proposal{Entities: []*cluster.Entity{{
			Type: typ,
			Key:  fmt.Appendf(nil, "k%d", i),
			Data: fmt.Appendf(nil, "v%d", i),
		}}}
		positions[i] = cluster.Position(fmt.Appendf(nil, "cp-%d", i))
	}

	probe := &positionProbeIngestable{s: s, id: id, proposals: proposals, positions: positions}

	// d.Ingest ignores the passed ctx (the worker runs on db.ctx); the
	// probe is torn down when newWalDB's cleanup closes the DB.
	require.NoError(t, d.Ingest(context.Background(), id, probe))

	// n proposals produce n-1 recorded observations (none before the
	// first proposal).
	require.Eventually(t, func() bool {
		return len(probe.recorded()) >= n-1
	}, 10*time.Second, 5*time.Millisecond, "probe never observed all checkpoints")

	got := probe.recorded()
	for j := range n - 1 {
		require.Equal(t, fmt.Sprintf("cp-%d", j), got[j],
			"position checkpointed after proposal %d must be durable before proposal %d is processed", j, j+1)
	}
}

// TestIngestPosition_EmitsDurationMetric verifies the new
// committed.ingest.position.bump.duration histogram records one
// observation per durable position bump. MemoryStorage is fine — the
// metric is recorded regardless of backend, and its ResolveType stub
// lets us reuse createProposals without registering a type.
func TestIngestPosition_EmitsDurationMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { provider.Shutdown(context.Background()) })

	m := metrics.New(provider.Meter("test"))
	s := NewMemoryStorage()
	id := uint64(1)
	peers := db.Peers{id: ""}

	d := db.New(id, peers, s, parser.New(), nil, nil,
		db.WithTickInterval(testTickInterval),
		db.WithMetrics(m),
	)
	defer d.Close()

	ps := createProposals([][]string{{"a"}, {"b"}})
	positions := []cluster.Position{cluster.Position([]byte("cp"))}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ingestable := NewIngestable(ps, positions, cancel)
	require.NoError(t, d.Ingest(ctx, "metric-ingest", ingestable))

	require.Eventually(t, func() bool {
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			return false
		}
		bump := findMetric(rm, "committed.ingest.position.bump.duration")
		if bump == nil {
			return false
		}
		hist, ok := bump.Data.(metricdata.Histogram[float64])
		if !ok || len(hist.DataPoints) != 1 {
			return false
		}
		return hist.DataPoints[0].Count == 1
	}, 5*time.Second, 5*time.Millisecond, "expected one ingest position bump observation")
}

// positionOnlyIngestable emits a single position (no user proposals), so
// the worker's first — and only — Propose is the position bump itself.
type positionOnlyIngestable struct {
	position cluster.Position
}

func (p *positionOnlyIngestable) Ingest(ctx context.Context, _ cluster.Position, _ chan<- *cluster.Proposal, po chan<- cluster.Position) error {
	select {
	case po <- p.position:
	case <-ctx.Done():
		return nil
	}
	<-ctx.Done()
	return nil
}

func (p *positionOnlyIngestable) Close() error { return nil }

// TestIngestPosition_FreezesOnErrProposalUnknown proves the new
// behavior: a position bump (not just a user proposal) that comes back
// ErrProposalUnknown freezes the worker. Under the old async bump this
// was swallowed and the worker continued; now the position bump
// participates in the same freeze contract as user data, so the
// supervisor restarts the ingestable from the un-advanced position.
func TestIngestPosition_FreezesOnErrProposalUnknown(t *testing.T) {
	id := "freeze-on-position"

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { provider.Shutdown(context.Background()) })
	m := metrics.New(provider.Meter("test"))

	// slowApply pins the position-bump waiter long enough to inject the
	// signal; the 1h supervisor backoff keeps the frozen gauge pinned at
	// 1 (no auto-restart races the assertion).
	d, s := newIngestFailFastDBWith(t,
		db.WithMetrics(m),
		db.WithIngestSupervisorInitialBackoff(1*time.Hour),
	)

	require.Eventually(t,
		func() bool { return d.ObservedLeaderForTest() == 1 },
		2*time.Second, 2*time.Millisecond,
	)

	require.NoError(t, d.Ingest(context.Background(), id,
		&positionOnlyIngestable{position: cluster.Position([]byte("pos-1"))}))

	rid := d.WaitForAnyWaiterForTest(2 * time.Second)
	require.NotZero(t, rid, "ingest worker never submitted the position bump")

	d.SignalWaiterForTest(rid, db.ErrProposalUnknown)

	require.Eventually(t, func() bool {
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			return false
		}
		return findSupervisorGaugeForID(rm, "committed.ingest.frozen", id) == 1.0
	}, 2*time.Second, 10*time.Millisecond,
		"position-bump ErrProposalUnknown must freeze the ingest worker")

	// NB: we do NOT assert the position is absent from the log. The
	// injected ErrProposalUnknown is artificial — the entry actually
	// committed (slowApply only stalls the apply, not the commit) — and
	// in the real leader-flap case the entry's fate is genuinely unknown
	// too. Freezing on that uncertainty is the whole point; the resume
	// reads the persisted (un-advanced) position regardless.

	s.Unblock()
	require.NoError(t, d.Close())
}

// TestIngestPosition_FreezesOnErrProposalLost is the ErrProposalLost twin of
// TestIngestPosition_FreezesOnErrProposalUnknown: a position bump that comes
// back ErrProposalLost (the truncated-entry signal that wins on the old
// leader, where the ingest worker runs) must freeze the worker just like
// Unknown. Without it, a Lost bump on a leader flap would advance the durable
// checkpoint past data that never committed.
func TestIngestPosition_FreezesOnErrProposalLost(t *testing.T) {
	id := "freeze-on-position-lost"

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { provider.Shutdown(context.Background()) })
	m := metrics.New(provider.Meter("test"))

	d, s := newIngestFailFastDBWith(t,
		db.WithMetrics(m),
		db.WithIngestSupervisorInitialBackoff(1*time.Hour),
	)

	require.Eventually(t,
		func() bool { return d.ObservedLeaderForTest() == 1 },
		2*time.Second, 2*time.Millisecond,
	)

	require.NoError(t, d.Ingest(context.Background(), id,
		&positionOnlyIngestable{position: cluster.Position([]byte("pos-1"))}))

	rid := d.WaitForAnyWaiterForTest(2 * time.Second)
	require.NotZero(t, rid, "ingest worker never submitted the position bump")

	d.SignalWaiterForTest(rid, db.ErrProposalLost)

	require.Eventually(t, func() bool {
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			return false
		}
		return findSupervisorGaugeForID(rm, "committed.ingest.frozen", id) == 1.0
	}, 2*time.Second, 10*time.Millisecond,
		"position-bump ErrProposalLost must freeze the ingest worker")

	s.Unblock()
	require.NoError(t, d.Close())
}

// TestDeleteIngestable_StalePositionBumpDoesNotOrphan is the ingest delete-race
// regression: a worker position bump that commits AFTER the config+position
// delete must not re-establish an orphaned IngestablePosition — otherwise a
// same-id recreate resumes CDC from a stale LSN whose replication slot Teardown
// has since dropped (error) or a silent data gap. The invariant enforced in
// saveIngestablePosition ("a position persists only while its config exists")
// makes the stale bump a no-op, independent of worker/propose timing.
func TestDeleteIngestable_StalePositionBumpDoesNotOrphan(t *testing.T) {
	d, s := newWalDB(t)
	const id = "orphan-ingest"
	seedIngestableConfig(t, d, id)

	bump := func(pos string) {
		e, err := cluster.NewUpsertIngestablePositionEntity(&cluster.IngestablePosition{ID: id, Position: []byte(pos)})
		require.NoError(t, err)
		require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{e}}))
	}
	bump("lsn-5")
	require.Equal(t, cluster.Position("lsn-5"), s.Position(id), "position persists while the config exists")

	// Delete the ingestable (config + position tombstone).
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: cluster.NewDeleteIngestableEntities(id)}))
	require.Nil(t, s.Position(id), "position cleared by the delete")

	// A stale bump commits after the delete — must NOT re-orphan the position.
	bump("lsn-9")
	require.Nil(t, s.Position(id), "a position bump for a deleted ingestable must be dropped (no orphan)")
}
