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
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// durableProbe records which source seqs the worker reported durable, and
// whether each had applied when it did. A dialect confirms to its source only
// what OnDurable reports, so it must fire after the apply, and never for a
// proposal that did not commit.
type durableProbe struct {
	s  *wal.Storage
	id string

	mu      sync.Mutex
	durable map[uint64]int
	early   []uint64 // reported before its seq had applied
}

func newDurableProbe(s *wal.Storage, id string) *durableProbe {
	return &durableProbe{s: s, id: id, durable: map[uint64]int{}}
}

func (p *durableProbe) proposal(typ *cluster.Type, seq uint64, data []byte) *cluster.Proposal {
	return &cluster.Proposal{
		SourceSeq: seq,
		Position:  fmt.Appendf(nil, "pos-%d", seq),
		Entities:  []*cluster.Entity{{Type: typ, Key: fmt.Appendf(nil, "k%d", seq), Data: data}},
		OnDurable: func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			p.durable[seq]++
			if p.s.IngestSourceSeqHighwater(p.id) < seq {
				p.early = append(p.early, seq)
			}
		},
	}
}

func (p *durableProbe) count(seq uint64) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.durable[seq]
}

// listIngestable hands the worker a fixed list of proposals, then idles.
type listIngestable struct {
	proposals []*cluster.Proposal
	handed    chan struct{} // closed once every proposal was handed off
}

func (l *listIngestable) Ingest(ctx context.Context, _ cluster.Position, pr chan<- *cluster.Proposal, _ chan<- cluster.Position) error {
	for _, p := range l.proposals {
		select {
		case pr <- p:
		case <-ctx.Done():
			return nil
		}
	}
	close(l.handed)
	<-ctx.Done()
	return nil
}

func (l *listIngestable) Close() error { return nil }

func (l *listIngestable) Status(context.Context, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{}, nil
}

// TestIngestOnDurable_AfterApplyAndForDuplicates: the worker reports each
// committed proposal durable once it has applied, and a re-emitted proposal
// it drops as a duplicate durable too (its applied copy is).
func TestIngestOnDurable_AfterApplyAndForDuplicates(t *testing.T) {
	d, s, _ := newWalDBWithMetrics(t)
	const id = "on-durable"
	seedIngestableConfig(t, d, id)
	proposeTypeTOML(t, d, "evt", "evt", "", "")
	typ, err := s.ResolveType(cluster.LatestTypeRef("evt"))
	require.NoError(t, err)
	probe := newDurableProbe(s, id)

	w1 := &listIngestable{handed: make(chan struct{}), proposals: []*cluster.Proposal{
		probe.proposal(typ, 1, []byte("v1")), probe.proposal(typ, 2, []byte("v2")),
	}}
	require.NoError(t, d.Ingest(context.Background(), id, w1))
	require.Eventually(t, func() bool { return probe.count(1) == 1 && probe.count(2) == 1 },
		10*time.Second, 5*time.Millisecond, "each applied proposal is reported durable")

	// A resumed worker re-emits 2 (a duplicate) and then 3.
	w2 := &listIngestable{handed: make(chan struct{}), proposals: []*cluster.Proposal{
		probe.proposal(typ, 2, []byte("v2")), probe.proposal(typ, 3, []byte("v3")),
	}}
	require.NoError(t, d.Ingest(context.Background(), id, w2))
	require.Eventually(t, func() bool { return probe.count(2) == 2 && probe.count(3) == 1 },
		10*time.Second, 5*time.Millisecond, "a dropped duplicate is reported durable")

	probe.mu.Lock()
	defer probe.mu.Unlock()
	require.Empty(t, probe.early, "reported durable before applying")
}

// TestIngestOnDurable_NotForAFailedProposal: a proposal the worker freezes on
// is never reported durable, so the source is never told it is done.
func TestIngestOnDurable_NotForAFailedProposal(t *testing.T) {
	d, s := newWalDBOpts(t, db.WithMaxProposalBytes(1024))
	const id = "on-durable-fail"
	seedIngestableConfig(t, d, id)
	typ := oversizeTestTopic(t, d, s)
	probe := newDurableProbe(s, id)

	ing := &listIngestable{handed: make(chan struct{}), proposals: []*cluster.Proposal{
		probe.proposal(typ, 1, make([]byte, 4096)),
	}}
	require.NoError(t, d.Ingest(context.Background(), id, ing))
	<-ing.handed
	require.Never(t, func() bool { return probe.count(1) > 0 },
		700*time.Millisecond, 20*time.Millisecond, "a proposal that never committed was reported durable")
}
