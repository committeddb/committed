package wal_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	"github.com/philborlin/committed/internal/cluster/db"
	parser "github.com/philborlin/committed/internal/cluster/db/parser"
	"github.com/philborlin/committed/internal/cluster/db/wal"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestIngestable(t *testing.T) {
	cfg1 := createIngestableConfiguration("foo")
	cfg2 := createIngestableConfiguration("bar")

	tests := []struct {
		cfgs []*cluster.Configuration
	}{
		{[]*cluster.Configuration{}},
		{[]*cluster.Configuration{cfg1}},
		{[]*cluster.Configuration{cfg1, cfg2}},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			var p db.Parser = parser.New()
			s := NewStorageWithParser(t, index(3).terms(3, 4, 5), p)
			defer s.Cleanup()

			ingestables := make(map[string]*clusterfakes.FakeIngestable)
			ingestables["foo"] = &clusterfakes.FakeIngestable{}
			ingestables["bar"] = &clusterfakes.FakeIngestable{}

			fooIngestableParser := &clusterfakes.FakeIngestableParser{}
			fooIngestableParser.ParseReturns(ingestables["foo"], nil)
			p.AddIngestableParser("foo", fooIngestableParser)

			barIngestableParser := &clusterfakes.FakeIngestableParser{}
			barIngestableParser.ParseReturns(ingestables["bar"], nil)
			p.AddIngestableParser("bar", barIngestableParser)

			currentIndex := uint64(6)
			currentTerm := uint64(6)
			insertIngestables(t, s, tt.cfgs, currentIndex, currentTerm)

			ctgs, err := s.Ingestables()
			require.Equal(t, nil, err)
			for _, expected := range tt.cfgs {
				require.Contains(t, ctgs, expected)
			}

			s = s.CloseAndReopenStorage(t)
			defer s.Cleanup()

			ctgs, err = s.Ingestables()
			require.Equal(t, nil, err)
			for _, expected := range tt.cfgs {
				require.Contains(t, ctgs, expected)
			}
		})
	}
}

func TestEmptyIngestables(t *testing.T) {
	s := NewStorage(t, index(3).terms(3, 4, 5))
	defer s.Cleanup()

	is, err := s.Ingestables()
	require.Nil(t, err)
	require.Equal(t, 0, len(is))
}

// TestApplyCommitted_RestartReplay verifies that wal.Storage's persisted
// appliedIndex correctly survives a close + reopen, and that already-applied
// committed entries are skipped on the second pass.
//
// This is the load-bearing assertion behind PR1's idempotency story:
// without persisted appliedIndex, restart would replay every committed
// entry through ApplyCommitted, which would re-parse every ingestable and
// re-spawn its worker — exactly the worker-registry-by-id.md bug.
//
// We verify by installing a parser on reopen whose Parse panics. If the
// re-apply fails to short-circuit on the index check, the panic surfaces.
func TestApplyCommitted_RestartReplay(t *testing.T) {
	dir := t.TempDir()

	// First incarnation: apply an ingestable through a working parser.
	workingParser := parser.New()
	fakeIngestable := &clusterfakes.FakeIngestable{}
	fakeParser := &clusterfakes.FakeIngestableParser{}
	fakeParser.ParseReturns(fakeIngestable, nil)
	workingParser.AddIngestableParser("test-replay", fakeParser)

	// No channels — this test focuses on apply + bucket state, not the
	// channel-fanout side. saveIngestable's `if s.ingest != nil` guard
	// keeps it quiet.
	s, err := wal.Open(dir, workingParser, nil, nil, testOpenOptions...)
	require.Nil(t, err)

	cfg := &cluster.Configuration{
		ID:       "ingest-replay",
		MimeType: "application/json",
		Data:     []byte(`{"ingestable": {"name": "ingest-replay", "type": "test-replay"}}`),
	}
	entity, err := cluster.NewUpsertIngestableEntity(cfg)
	require.Nil(t, err)

	p := &cluster.Proposal{Entities: []*cluster.Entity{entity}}
	bs, err := p.Marshal()
	require.Nil(t, err)
	entry := raftpb.Entry{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: bs}

	require.Nil(t, s.Save(defaultHardState, []raftpb.Entry{entry}, defaultSnap))
	require.Nil(t, s.ApplyCommitted(entry))
	require.Equal(t, uint64(1), s.AppliedIndex(), "appliedIndex bumped after first apply")
	require.Equal(t, 1, fakeParser.ParseCallCount(), "parser invoked exactly once on first apply")

	// Bucket should hold the configuration we just put.
	cfgs, err := s.Ingestables()
	require.Nil(t, err)
	require.Equal(t, 1, len(cfgs))

	require.Nil(t, s.Close())

	// Second incarnation: install a parser that PANICS if Parse is called.
	// If appliedIndex was not persisted (or the index-skip in ApplyCommitted
	// was wrong), this is the line that catches it.
	panicParser := parser.New()
	panickingFake := &clusterfakes.FakeIngestableParser{}
	panickingFake.ParseStub = func(*viper.Viper) (cluster.Ingestable, error) {
		panic("parser must not be called on restart-replay of an already-applied entry")
	}
	panicParser.AddIngestableParser("test-replay", panickingFake)

	s2, err := wal.Open(dir, panicParser, nil, nil, testOpenOptions...)
	require.Nil(t, err)
	defer s2.Close()

	// appliedIndex must survive the close/reopen.
	require.Equal(t, uint64(1), s2.AppliedIndex(), "appliedIndex restored from bbolt")

	// Re-applying the same entry must be a no-op — neither parse nor put
	// runs. If the index-skip is broken, the panicParser triggers.
	require.Nil(t, s2.ApplyCommitted(entry))

	// Bucket state from the first incarnation must still be visible
	// (BoltDB persistence is independent of this test's appliedIndex story,
	// but worth asserting so a regression in bucket persistence is caught).
	cfgs2, err := s2.Ingestables()
	require.Nil(t, err)
	require.Equal(t, 1, len(cfgs2))
	require.Equal(t, "ingest-replay", cfgs2[0].ID)
}

func insertIngestables(t *testing.T, s db.Storage, ts []*cluster.Configuration, term, index uint64) uint64 {
	for i, tipe := range ts {
		e, err := cluster.NewUpsertIngestableEntity(tipe)
		require.Equal(t, nil, err)

		saveEntity(t, e, s, term, index+uint64(i))
	}

	return index + uint64(len(ts))
}

func createIngestableConfiguration(name string) *cluster.Configuration {
	d := &IngestableConfig{Details: &Details{Name: name, Type: name}}
	return createConfiguration(name, d)
}

// TestProposeIngestable_StartsIngestionWiring verifies the integration between
// db.ProposeIngestable, the wal storage's ingest channel, and
// db.listenForIngestables. When an ingestable is proposed, wal saveIngestable
// must send the parsed ingestable to the channel, db must consume it and
// invoke db.Ingest, and the proposals the ingestable produces must appear as
// committed entries in the raft log.
//
// This wiring is not exercised by the existing tests:
//
//   - parser_test.go:TestIngestable uses MemoryStorage, whose Save() does not
//     run handleIngestable, so the channel send never happens.
//   - ingest_test.go:TestIngest calls db.Ingest directly, bypassing the
//     channel entirely.
//
// Without this test, the only coverage of the full wiring lived in
// http_test.go:TestEndToEnd, where it was tangled up with HTTP, syncable, and
// in-memory mysql concerns.
func TestProposeIngestable_StartsIngestionWiring(t *testing.T) {
	p := parser.New()

	// Buffered so wal saveIngestable's send does not block on the consumer.
	// db.listenForIngestables receives unbuffered, but a buffer of 1 keeps
	// the test deterministic if the consumer goroutine is slow to schedule.
	// (Named syncCh rather than sync to avoid shadowing the sync package,
	// which the closeOnce wrapper below needs.)
	ingest := make(chan *db.IngestableWithID, 1)
	syncCh := make(chan *db.SyncableWithID, 1)

	storage, err := wal.Open(t.TempDir(), p, syncCh, ingest, testOpenOptions...)
	require.Nil(t, err)
	defer storage.Close()

	// Configure the parser so it returns a fake ingestable that produces one
	// known proposal when Ingest is called. The stub blocks on ctx.Done()
	// after sending so the goroutine stays alive for db.Close() to cancel.
	expected := &cluster.Proposal{Entities: []*cluster.Entity{{
		Type: &cluster.Type{ID: "test-type"},
		Key:  []byte("test-key"),
		Data: []byte(`{"x":1}`),
	}}}
	fakeIngestable := &clusterfakes.FakeIngestable{}
	fakeIngestable.IngestStub = func(
		ctx context.Context,
		_ cluster.Position,
		pr chan<- *cluster.Proposal,
		_ chan<- cluster.Position,
	) error {
		select {
		case pr <- expected:
		case <-ctx.Done():
			return nil
		}
		<-ctx.Done()
		return nil
	}
	fakeParser := &clusterfakes.FakeIngestableParser{}
	fakeParser.ParseReturns(fakeIngestable, nil)
	p.AddIngestableParser("test-ingestable", fakeParser)

	// Build a real db.DB wired to the same storage and channels. We use a
	// 1ms tick so single-node Raft elects itself in ~10ms. The empty
	// local-peer URL tells httptransport to skip binding a TCP listener
	// (we have no peers to receive from), so this test can run in parallel
	// with any other package that constructs a db.DB.
	id := uint64(1)
	peers := db.Peers{id: ""}
	d := db.New(id, peers, storage, p, syncCh, ingest, db.WithTickInterval(1*time.Millisecond))
	defer d.Close()
	d.EatCommitC()

	cfg := &cluster.Configuration{
		ID:       "ingestable-1",
		MimeType: "application/json",
		Data: []byte(`{
			"ingestable": {"name": "ingestable-1", "type": "test-ingestable"}
		}`),
	}

	require.Nil(t, d.ProposeIngestable(context.Background(), cfg))

	// d.ProposeIngestable is blocking after PR2: when it returns, the
	// configuration entry is already applied and storage.AppliedIndex
	// reflects whatever raft index it landed at (this depends on
	// pre-existing entries from leader election, conf-change bootstrap,
	// etc., so we can't hard-code the expected value). We then wait for
	// AppliedIndex to grow further, which signals that the ingest worker
	// has emitted its test-key proposal AND that proposal has been
	// applied. AppliedIndex is backed by atomic.Uint64 so it's safe to
	// read concurrently with the raft goroutine that writes it.
	configAppliedIndex := storage.AppliedIndex()
	require.Eventually(t, func() bool {
		return storage.AppliedIndex() > configAppliedIndex
	}, 2*time.Second, 5*time.Millisecond,
		"expected ingested Proposal to be applied after Configuration")

	ents := readNormalProposals(storage)
	require.GreaterOrEqual(t, len(ents), 2,
		"expected at least Configuration + ingested Proposal in raft log")

	// Find the proposal the fake ingestable emitted (key = "test-key").
	// We can't always count on it being at index 1 because the raft loop
	// may interleave system entries (e.g. an ingestable position bump)
	// between the configuration and the user proposal.
	var got *cluster.Entity
	for _, p := range ents {
		for _, e := range p.Entities {
			if string(e.Key) == "test-key" {
				got = e
			}
		}
	}
	require.NotNil(t, got, "ingested test-key proposal not found in raft log")
	require.Equal(t, "test-type", got.Type.ID)
	require.Equal(t, []byte(`{"x":1}`), got.Data)

	// And the parser was actually invoked by saveIngestable, proving the
	// wal->parse->channel hop ran (rather than the consumer somehow seeing a
	// stale ingestable from elsewhere).
	require.GreaterOrEqual(t, fakeParser.ParseCallCount(), 1)
}

// readNormalProposals returns the cluster.Proposals stored in raft normal
// entries, in index order. It is the same shape as DB.ents() in db_test.go but
// reads directly from the storage so the test does not need access to the
// db package's internal helper.
func readNormalProposals(s *wal.Storage) []*cluster.Proposal {
	fi, _ := s.FirstIndex()
	li, _ := s.LastIndex()
	if li+1 <= fi {
		return nil
	}
	ents, _ := s.Entries(fi, li+1, 10000)

	var ps []*cluster.Proposal
	for _, e := range ents {
		if e.Type != raftpb.EntryNormal || e.Data == nil {
			continue
		}
		p := &cluster.Proposal{}
		if err := p.Unmarshal(e.Data); err != nil {
			fmt.Printf("unmarshal proposal: %v\n", err)
			continue
		}
		if len(p.Entities) == 0 {
			continue
		}
		ps = append(ps, p)
	}
	return ps
}
