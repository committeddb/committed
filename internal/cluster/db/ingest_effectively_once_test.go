package db_test

import (
	"context"
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
	"github.com/committeddb/committed/internal/cluster/metrics"
)

// seqEvent is one source event a seqIngestable emits: a proposal stamped
// with SourceSeq=seq carrying a single entity whose data is payload.
type seqEvent struct {
	seq     uint64
	payload string
}

// seqIngestable emits a fixed list of source events as proposals,
// stamping each with its SourceSeq. It ignores the resume position — the
// test drives the "resumed from a stale checkpoint and re-emitted"
// scenario by handing the replacement worker a list that starts below
// the committed highwater.
type seqIngestable struct {
	typ    *cluster.Type
	events []seqEvent
}

func (si *seqIngestable) Ingest(ctx context.Context, _ cluster.Position, pr chan<- *cluster.Proposal, _ chan<- cluster.Position) error {
	for _, e := range si.events {
		p := &cluster.Proposal{
			SourceSeq: e.seq,
			Entities: []*cluster.Entity{{
				Type: si.typ,
				Key:  fmt.Appendf(nil, "k%d", e.seq),
				Data: []byte(e.payload),
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

func (si *seqIngestable) Close() error { return nil }

// committedIngestSeqs returns the SourceSeqs of every committed ingest
// user proposal for id, in raft-log order. Position bumps carry no
// IngestableID, so filtering on it isolates the user data.
func committedIngestSeqs(t *testing.T, s *wal.Storage, id string) []uint64 {
	t.Helper()
	fi, err := s.FirstIndex()
	require.NoError(t, err)
	li, err := s.LastIndex()
	require.NoError(t, err)
	if li < fi {
		return nil
	}
	ents, err := s.Entries(fi, li+1, math.MaxUint64)
	require.NoError(t, err)

	var seqs []uint64
	for _, e := range ents {
		if e.GetType() != raftpb.EntryNormal || e.Data == nil {
			continue
		}
		p := &cluster.Proposal{}
		if err := p.Unmarshal(e.Data, s); err != nil {
			continue
		}
		if p.IngestableID == id {
			seqs = append(seqs, p.SourceSeq)
		}
	}
	return seqs
}

func newWalDBWithMetrics(t *testing.T) (*db.DB, *wal.Storage, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	m := metrics.New(provider.Meter("test"))

	dir := t.TempDir()
	p := parser.New()
	s, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)

	id := uint64(1)
	peers := db.Peers{id: ""}
	d := db.New(id, peers, s, p, nil, nil, db.WithTickInterval(testTickInterval), db.WithMetrics(m))
	t.Cleanup(func() { _ = d.Close(); _ = s.Close() })
	return d, s, reader
}

// TestIngestDedup_RecoveryHasNoDuplicateCommittedEntries is the headline
// criterion: a crash/flap that makes the dialect re-emit already-committed
// proposals produces ZERO duplicate entries in the committed log.
//
// Worker 1 commits source seqs 1,2,3 (highwater → 3). The replacement
// worker (the in-process stand-in for a restarted dialect resuming from a
// stale checkpoint) re-emits 2,3 and then new 4,5. The re-emitted 2,3 are
// at/below the durable highwater, so they're dropped before raft; the log
// ends up with each source seq exactly once.
func TestIngestDedup_RecoveryHasNoDuplicateCommittedEntries(t *testing.T) {
	d, s, reader := newWalDBWithMetrics(t)
	id := "dedup-ingest"

	proposeTypeTOML(t, d, "evt", "evt", "", "")
	typ, err := s.ResolveType(cluster.LatestTypeRef("evt"))
	require.NoError(t, err)

	worker1 := &seqIngestable{typ: typ, events: []seqEvent{
		{1, "v1"}, {2, "v2"}, {3, "v3"},
	}}
	require.NoError(t, d.Ingest(context.Background(), id, worker1))

	// Wait until 1,2,3 are committed AND applied (highwater reflects it).
	require.Eventually(t, func() bool {
		return s.IngestSourceSeqHighwater(id) == 3
	}, 10*time.Second, 5*time.Millisecond, "worker 1 never advanced the highwater to 3")

	// Replace with a worker that re-emits 2,3 (stale-checkpoint resume)
	// then new 4,5.
	worker2 := &seqIngestable{typ: typ, events: []seqEvent{
		{2, "v2"}, {3, "v3"}, {4, "v4"}, {5, "v5"},
	}}
	require.NoError(t, d.Ingest(context.Background(), id, worker2))

	require.Eventually(t, func() bool {
		return s.IngestSourceSeqHighwater(id) == 5
	}, 10*time.Second, 5*time.Millisecond, "worker 2 never advanced the highwater to 5")

	// The committed log must hold each source seq exactly once.
	seqs := committedIngestSeqs(t, s, id)
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	require.Equal(t, []uint64{1, 2, 3, 4, 5}, seqs,
		"committed log should contain each source seq exactly once (no re-emitted duplicates)")

	// And the two re-emits should have been counted as skipped.
	require.Eventually(t, func() bool {
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			return false
		}
		return findSupervisorCounterForID(rm, "committed.ingest.dedup_skipped_total", id) == 2
	}, 5*time.Second, 5*time.Millisecond, "expected exactly 2 deduped re-emits")
}

// TestIngestDedup_ZeroSeqNeverDeduped confirms SourceSeq==0 proposals
// (snapshot rows / non-CDC / legacy) bypass dedup entirely — they're
// always proposed, even repeatedly, since 0 carries no ordering.
func TestIngestDedup_ZeroSeqNeverDeduped(t *testing.T) {
	d, s, _ := newWalDBWithMetrics(t)
	id := "zero-seq-ingest"

	proposeTypeTOML(t, d, "evt", "evt", "", "")
	typ, err := s.ResolveType(cluster.LatestTypeRef("evt"))
	require.NoError(t, err)

	// Three proposals all with SourceSeq 0 (the zero value).
	worker := &seqIngestable{typ: typ, events: []seqEvent{
		{0, "a"}, {0, "b"}, {0, "c"},
	}}
	require.NoError(t, d.Ingest(context.Background(), id, worker))

	require.Eventually(t, func() bool {
		return len(committedIngestSeqs(t, s, id)) == 3
	}, 10*time.Second, 5*time.Millisecond, "all three zero-seq proposals should commit")

	require.Equal(t, uint64(0), s.IngestSourceSeqHighwater(id),
		"zero-seq proposals must not advance the highwater")
}
