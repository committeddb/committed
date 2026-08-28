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

// txnEvent is one emitted source event for txnIngestable: a SourceSeq, the
// transaction identity that scopes its dedup, and the transient hints a
// dialect would stamp.
type txnEvent struct {
	seq      uint64
	txn      string
	lineage  bool // LineageRegressed (GTID-resume failover shape)
	unsafeRe bool // DedupUnsafe (re-chunk shape)
}

// txnIngestable emits a fixed list of transaction-stamped events, then idles
// until cancelled (so the worker doesn't exit-freeze after the list).
type txnIngestable struct {
	typ    *cluster.Type
	events []txnEvent
}

func (ti *txnIngestable) Ingest(ctx context.Context, _ cluster.Position, pr chan<- *cluster.Proposal, _ chan<- cluster.Position) error {
	for _, e := range ti.events {
		p := &cluster.Proposal{
			SourceSeq:        e.seq,
			SourceTxnID:      e.txn,
			TxnScopedDedup:   e.txn != "", // the bundling dialects' opt-in
			LineageRegressed: e.lineage,
			DedupUnsafe:      e.unsafeRe,
			Entities: []*cluster.Entity{{
				Type: ti.typ,
				Key:  fmt.Appendf(nil, "k-%s-%d", e.txn, e.seq),
				Data: fmt.Appendf(nil, "v%d", e.seq),
			}},
		}
		select {
		case pr <- p:
		case <-ctx.Done():
			return nil
		}
	}
	<-ctx.Done()
	return nil
}

func (ti *txnIngestable) Close() error { return nil }

func (ti *txnIngestable) Status(context.Context, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{}, nil
}

// txnDedupHarness wires the real wal storage + real worker + metrics reader.
func txnDedupHarness(t *testing.T, id string) (*db.DB, *wal.Storage, *sdkmetric.ManualReader, *cluster.Type) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	m := metrics.New(provider.Meter("test"))

	dir := t.TempDir()
	p := parser.New()
	s, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, nil, nil,
		db.WithTickInterval(testTickInterval), db.WithMetrics(m),
		db.WithIngestSupervisorInitialBackoff(1*time.Hour))
	t.Cleanup(func() { _ = d.Close(); _ = s.Close() })

	seedIngestableConfig(t, d, id)
	proposeTypeTOML(t, d, "evt", "evt", "", "")
	typ, err := s.ResolveType(cluster.LatestTypeRef("evt"))
	require.NoError(t, err)
	return d, s, reader, typ
}

// frozenGauge reads the committed.ingest.frozen gauge for id; an
// absent metric (nothing ever froze — findSupervisorGaugeForID's -1)
// reads as 0.
func frozenGauge(t *testing.T, reader *sdkmetric.ManualReader, id string) float64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	if v := findSupervisorGaugeForID(rm, "committed.ingest.frozen", id); v > 0 {
		return v
	}
	return 0
}

// TestTxnDedup_DifferentTxnIsNeverComparedAcrossTxns is the core red-proof
// for BOTH failover ride-through and the compressed-transaction
// sub-collapse: a huge transaction inflates the seq highwater (parts 1..N at
// one coordinate), and a FOLLOWING small transaction encodes below it. Under
// the old global-scalar dedup the follower was silently dropped; under
// transaction scoping a different SourceTxnID resets the record and the
// follower commits.
func TestTxnDedup_DifferentTxnIsNeverComparedAcrossTxns(t *testing.T) {
	id := "txn-dedup-reset"
	d, s, _, typ := txnDedupHarness(t, id)

	// The big transaction: sub-inflated parts up to seq 3349.
	require.NoError(t, d.Ingest(context.Background(), id, &txnIngestable{typ: typ, events: []txnEvent{
		{seq: 3000, txn: "BIG"}, {seq: 3349, txn: "BIG"},
	}}))
	require.Eventually(t, func() bool {
		txn, seq := s.IngestSourceDedup(id)
		return txn == "BIG" && seq == 3349
	}, 10*time.Second, 5*time.Millisecond, "the big transaction never established its record")

	// The small follower encodes below the inflated highwater — a different
	// transaction, so it must COMMIT, not be dedup-dropped.
	require.NoError(t, d.Ingest(context.Background(), id, &txnIngestable{typ: typ, events: []txnEvent{
		{seq: 288, txn: "SMALL"},
	}}))
	require.Eventually(t, func() bool {
		txn, seq := s.IngestSourceDedup(id)
		return txn == "SMALL" && seq == 288
	}, 10*time.Second, 5*time.Millisecond,
		"the follower transaction must reset the record, not be swallowed by the inflated highwater")

	seqs := committedIngestSeqs(t, s, id)
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	require.Equal(t, []uint64{288, 3000, 3349}, seqs, "every transaction's parts must be committed exactly once")
}

// TestTxnDedup_SameTxnPartsStillSkip pins the torn-transaction replay half:
// a crash mid-oversized-transaction re-emits parts 1..k of the SAME
// transaction; those are true duplicates and must be skipped silently, while
// the transaction's continuation commits.
func TestTxnDedup_SameTxnPartsStillSkip(t *testing.T) {
	id := "txn-dedup-parts"
	d, s, reader, typ := txnDedupHarness(t, id)

	require.NoError(t, d.Ingest(context.Background(), id, &txnIngestable{typ: typ, events: []txnEvent{
		{seq: 1, txn: "T"}, {seq: 2, txn: "T"}, {seq: 3, txn: "T"},
	}}))
	require.Eventually(t, func() bool {
		_, seq := s.IngestSourceDedup(id)
		return seq == 3
	}, 10*time.Second, 5*time.Millisecond)

	// The replacement worker replays parts 2,3 (duplicates) then continues 4.
	require.NoError(t, d.Ingest(context.Background(), id, &txnIngestable{typ: typ, events: []txnEvent{
		{seq: 2, txn: "T"}, {seq: 3, txn: "T"}, {seq: 4, txn: "T"},
	}}))
	require.Eventually(t, func() bool {
		_, seq := s.IngestSourceDedup(id)
		return seq == 4
	}, 10*time.Second, 5*time.Millisecond)

	seqs := committedIngestSeqs(t, s, id)
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	require.Equal(t, []uint64{1, 2, 3, 4}, seqs, "replayed same-transaction parts must be skipped, never duplicated")
	require.Zero(t, frozenGauge(t, reader, id), "a clean part replay must not freeze")
}

// TestTxnDedup_LineageRegressionRidesThroughOnTxnRecord pins the failover
// ride-through: with a transaction-stamped record established, a promoted
// replica's low-numbered lineage (LineageRegressed proposals whose seqs sit
// below the old highwater) commits without a freeze and without loss.
func TestTxnDedup_LineageRegressionRidesThroughOnTxnRecord(t *testing.T) {
	id := "txn-dedup-failover"
	d, s, reader, typ := txnDedupHarness(t, id)

	require.NoError(t, d.Ingest(context.Background(), id, &txnIngestable{typ: typ, events: []txnEvent{
		{seq: 9_000_000, txn: "OLD"},
	}}))
	require.Eventually(t, func() bool {
		txn, _ := s.IngestSourceDedup(id)
		return txn == "OLD"
	}, 10*time.Second, 5*time.Millisecond)

	// Post-promotion: new lineage, low coordinates, new transactions.
	require.NoError(t, d.Ingest(context.Background(), id, &txnIngestable{typ: typ, events: []txnEvent{
		{seq: 1000, txn: "NEW-1", lineage: true},
		{seq: 2000, txn: "NEW-2", lineage: true},
	}}))
	require.Eventually(t, func() bool {
		txn, seq := s.IngestSourceDedup(id)
		return txn == "NEW-2" && seq == 2000
	}, 10*time.Second, 5*time.Millisecond,
		"post-failover transactions must ride through, not freeze or be dropped")
	require.Zero(t, frozenGauge(t, reader, id), "a ride-through must never freeze")

	seqs := committedIngestSeqs(t, s, id)
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	require.Equal(t, []uint64{1000, 2000, 9_000_000}, seqs)
}

// TestTxnDedup_LineageRegressionFreezesOnLegacyRecord pins the
// upgrade-window fail-safe: while the stored record is still the LEGACY
// scalar regime (no transaction identity — a pre-upgrade log), a scalar
// comparison cannot distinguish a post-failover proposal from a duplicate,
// so a LineageRegressed proposal at/below the highwater must FREEZE exactly
// as the pre-upgrade DedupUnsafe path did.
func TestTxnDedup_LineageRegressionFreezesOnLegacyRecord(t *testing.T) {
	id := "txn-dedup-legacy"
	d, s, reader, typ := txnDedupHarness(t, id)

	// Legacy regime: committed proposals with NO transaction identity.
	require.NoError(t, d.Ingest(context.Background(), id, &txnIngestable{typ: typ, events: []txnEvent{
		{seq: 1}, {seq: 2}, {seq: 3},
	}}))
	require.Eventually(t, func() bool {
		txn, seq := s.IngestSourceDedup(id)
		return txn == "" && seq == 3
	}, 10*time.Second, 5*time.Millisecond)

	// A post-failover proposal below the legacy highwater: freeze, don't drop.
	require.NoError(t, d.Ingest(context.Background(), id, &txnIngestable{typ: typ, events: []txnEvent{
		{seq: 2, txn: "NEW", lineage: true},
		{seq: 4, txn: "NEW"},
	}}))
	require.Eventually(t, func() bool { return frozenGauge(t, reader, id) == 1.0 },
		5*time.Second, 10*time.Millisecond,
		"a lineage regression against a legacy scalar record must freeze (the upgrade-window fail-safe)")
	require.Never(t, func() bool {
		_, seq := s.IngestSourceDedup(id)
		return seq > 3
	}, 500*time.Millisecond, 25*time.Millisecond,
		"the frozen worker must not commit past the regression point")
}
