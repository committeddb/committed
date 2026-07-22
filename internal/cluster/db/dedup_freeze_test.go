package db_test

import (
	"context"
	"fmt"
	"sort"
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

// freezeSeqIngestable re-emits SourceSeq 2 flagged DedupUnsafe (the shape a
// failover-lineage regression produces — real data whose coordinate encodes
// below the durable highwater), then 4 and 5. If the dedup drops the unsafe
// re-emit silently, 4 and 5 commit and the highwater reaches 5; if it freezes
// (the fix), the worker halts at the unsafe proposal and 4,5 never commit.
type freezeSeqIngestable struct {
	typ *cluster.Type
}

func (f *freezeSeqIngestable) Ingest(ctx context.Context, _ cluster.Position, pr chan<- *cluster.Proposal, _ chan<- cluster.Position) error {
	emit := func(seq uint64, unsafe bool) bool {
		p := &cluster.Proposal{
			SourceSeq:   seq,
			DedupUnsafe: unsafe,
			Entities: []*cluster.Entity{{
				Type: f.typ,
				Key:  fmt.Appendf(nil, "k%d", seq),
				Data: fmt.Appendf(nil, "v%d", seq),
			}},
		}
		select {
		case pr <- p:
			return true
		case <-ctx.Done():
			return false
		}
	}
	if !emit(2, true) || !emit(4, false) || !emit(5, false) {
		return nil
	}
	<-ctx.Done()
	return nil
}

func (f *freezeSeqIngestable) Close() error { return nil }

func (f *freezeSeqIngestable) Status(context.Context, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{}, nil
}

// TestIngestDedup_UnsafeBelowHighwaterFreezes pins the db half of
// mysql-samecoordinate-dedup-stability: a proposal at/below the SourceSeq
// highwater that a dialect flags DedupUnsafe (a lineage regression or a
// re-chunk under changed config/binary — cases where SourceSeq is no longer a
// trustworthy content identity) must FREEZE the worker, not be silently
// dropped. Silent drop is data loss for a system of record; freeze is the
// operator-visible fail-safe.
//
// The freeze is also the red-proof: a drop would let the later seqs 4,5 commit
// (highwater → 5), so "highwater never passes 3" fails against today's
// unconditional dedup-drop.
func TestIngestDedup_UnsafeBelowHighwaterFreezes(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	m := metrics.New(provider.Meter("test"))

	dir := t.TempDir()
	p := parser.New()
	s, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	// Real applies (so the highwater advances) + a 1h supervisor backoff so the
	// frozen gauge stays pinned at 1 for the assertion (no auto-restart race).
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, nil, nil,
		db.WithTickInterval(testTickInterval), db.WithMetrics(m),
		db.WithIngestSupervisorInitialBackoff(1*time.Hour))
	t.Cleanup(func() { _ = d.Close(); _ = s.Close() })

	id := "dedup-freeze"
	seedIngestableConfig(t, d, id)
	proposeTypeTOML(t, d, "evt", "evt", "", "")
	typ, err := s.ResolveType(cluster.LatestTypeRef("evt"))
	require.NoError(t, err)

	// Commit seqs 1,2,3 → highwater 3.
	require.NoError(t, d.Ingest(context.Background(), id, &seqIngestable{typ: typ, events: []seqEvent{
		{1, "v1"}, {2, "v2"}, {3, "v3"},
	}}))
	require.Eventually(t, func() bool { return s.IngestSourceSeqHighwater(id) == 3 },
		10*time.Second, 5*time.Millisecond, "worker 1 never advanced the highwater to 3")

	// Replacement worker re-emits seq 2 flagged DedupUnsafe, then 4,5.
	require.NoError(t, d.Ingest(context.Background(), id, &freezeSeqIngestable{typ: typ}))

	// Positive: the unsafe below-highwater re-emit froze the worker.
	require.Eventually(t, func() bool {
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			return false
		}
		return findSupervisorGaugeForID(rm, "committed.ingest.frozen", id) == 1.0
	}, 5*time.Second, 10*time.Millisecond,
		"an unsafe below-highwater re-emit must freeze the worker")

	// Red-distinguishing: a silent drop would let 4,5 commit (highwater → 5).
	// The freeze halts the worker at the unsafe proposal, so it must never pass 3.
	require.Never(t, func() bool { return s.IngestSourceSeqHighwater(id) > 3 },
		500*time.Millisecond, 25*time.Millisecond,
		"a frozen worker must not commit past the unsafe re-emit (a drop would reach highwater 5)")

	seqs := committedIngestSeqs(t, s, id)
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	require.Equal(t, []uint64{1, 2, 3}, seqs,
		"only the pre-freeze seqs may be committed; the unsafe re-emit and everything after it must not")
}
