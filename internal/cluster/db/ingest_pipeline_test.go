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
)

// pipelineIngestable emits a fixed sequence of proposals, then optionally a
// standalone position, then parks until ctx cancellation — the shape of a
// dialect's per-row snapshot stream.
type pipelineIngestable struct {
	proposals []*cluster.Proposal
	position  cluster.Position

	mu      sync.Mutex
	emitted int
}

func (p *pipelineIngestable) Ingest(ctx context.Context, _ cluster.Position, pr chan<- *cluster.Proposal, po chan<- cluster.Position) error {
	for _, prop := range p.proposals {
		select {
		case pr <- prop:
			p.mu.Lock()
			p.emitted++
			p.mu.Unlock()
		case <-ctx.Done():
			return nil
		}
	}
	if p.position != nil {
		select {
		case po <- p.position:
		case <-ctx.Done():
			return nil
		}
	}
	<-ctx.Done()
	return nil
}

func (p *pipelineIngestable) Close() error { return nil }

func (p *pipelineIngestable) Status(context.Context, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{}, nil
}

func (p *pipelineIngestable) Emitted() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.emitted
}

func bareRow(typ *cluster.Type, i int) *cluster.Proposal {
	return &cluster.Proposal{Entities: []*cluster.Entity{{
		Type: typ,
		Key:  fmt.Appendf(nil, "k%d", i),
		Data: fmt.Appendf(nil, "v%d", i),
	}}}
}

// TestIngestPipeline_PerRowStreamCommitsAllAndCheckpoints is the happy-path
// pin for the per-row snapshot shape: a window of bare single-row proposals
// whose FINAL row bundles the checkpoint. Every row lands as its own Actual
// (a true single-row transaction) and the position is exactly the bundled
// one — the pipelined lane and the drain-before-position lane composing.
func TestIngestPipeline_PerRowStreamCommitsAllAndCheckpoints(t *testing.T) {
	d, s := newWalDBOpts(t)
	const id = "pipeline-happy"
	seedIngestableConfig(t, d, id)
	typ := oversizeTestTopic(t, d, s)

	const rows = 8
	proposals := make([]*cluster.Proposal, rows)
	for i := range proposals {
		proposals[i] = bareRow(typ, i)
	}
	proposals[rows-1].Position = cluster.Position("cp-w1")

	ing := &pipelineIngestable{proposals: proposals}
	require.NoError(t, d.Ingest(context.Background(), id, ing))

	require.Eventually(t, func() bool {
		return string(s.Position(id)) == "cp-w1"
	}, 10*time.Second, 5*time.Millisecond, "the window-final bundled checkpoint must apply")
	require.Equal(t, rows, countTopicRows(t, s, typ.ID),
		"every per-row proposal must land in the log")
}

// TestIngestPipeline_RowsAreConcurrentlyInFlight pins the mechanism: with
// applies parked, the worker must have MULTIPLE proposals in flight at once
// (observable as concurrent Propose waiters). The synchronous worker never
// held more than one — this is what recovers batch-like throughput for
// per-row proposals.
func TestIngestPipeline_RowsAreConcurrentlyInFlight(t *testing.T) {
	const id = "pipeline-inflight"
	d, s := newIngestFailFastDBWith(t)
	require.Eventually(t,
		func() bool { return d.ObservedLeaderForTest() == 1 },
		2*time.Second, 2*time.Millisecond,
	)

	typ := &cluster.Type{ID: "string"}
	proposals := make([]*cluster.Proposal, 3)
	for i := range proposals {
		proposals[i] = bareRow(typ, i)
	}
	// Baseline BEFORE the ingest starts: the harness parks internal
	// startup proposals (node-version announce) too, and those waiters
	// must not be mistaken for pipeline depth.
	baseline := d.WaiterCountForTest()
	ing := &pipelineIngestable{proposals: proposals}
	require.NoError(t, d.Ingest(context.Background(), id, ing))

	require.Eventually(t, func() bool { return d.WaiterCountForTest() >= baseline+3 },
		2*time.Second, 5*time.Millisecond,
		"the pipelined worker must hold multiple rows in flight concurrently")

	s.Unblock()
	require.NoError(t, d.Close())
}

// TestIngestPipeline_HoleFreezesAndHoldsPosition pins the prefix-commit rule:
// if any in-flight row fails (a leader-flap orphan), the later standalone
// checkpoint must NEVER persist — the drain discovers the hole and the worker
// freezes. This is the pipelined generalization of the one-in-flight freeze
// tests.
func TestIngestPipeline_HoleFreezesAndHoldsPosition(t *testing.T) {
	const id = "pipeline-hole"
	// Hour-long supervisor backoff: the frozen worker must STAY frozen for
	// the assertion window — a supervisor restart would legitimately
	// re-emit and checkpoint (that recovery is the supervisor test's job).
	d, s := newIngestFailFastDBWith(t,
		db.WithIngestSupervisorInitialBackoff(time.Hour),
		db.WithIngestSupervisorMaxBackoff(time.Hour),
	)
	require.Eventually(t,
		func() bool { return d.ObservedLeaderForTest() == 1 },
		2*time.Second, 2*time.Millisecond,
	)

	typ := &cluster.Type{ID: "string"}
	proposals := make([]*cluster.Proposal, 3)
	for i := range proposals {
		proposals[i] = bareRow(typ, i)
	}
	// Baseline BEFORE the ingest starts: startup parks internal proposals
	// too, and the sweep must not run before every ROW is in flight.
	baseline := d.WaiterCountForTest()
	ing := &pipelineIngestable{proposals: proposals, position: cluster.Position("cp-after-hole")}
	require.NoError(t, d.Ingest(context.Background(), id, ing))

	// All three rows in flight (applies parked), then kill EVERY current
	// waiter: the harness cannot tell which rid is which proposer, and the
	// invariant must hold with any (indeed every) row holed.
	require.Eventually(t, func() bool { return d.WaiterCountForTest() >= baseline+3 },
		2*time.Second, 5*time.Millisecond,
		"all three rows must be in flight before the sweep")
	for _, rid := range d.WaiterIDsForTest() {
		d.SignalWaiterForTest(rid, db.ErrProposalUnknown)
	}

	// Let the surviving rows apply; the drain must still discover the hole
	// and the checkpoint must never land.
	s.Unblock()
	require.Never(t,
		func() bool { return s.HasIngestablePositionEntity(id) },
		300*time.Millisecond, 10*time.Millisecond,
		"a checkpoint must never commit past a pipelined hole")

	require.NoError(t, d.Close())
}
