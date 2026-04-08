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
	ingest := make(chan *db.IngestableWithID, 1)
	sync := make(chan *db.SyncableWithID, 1)

	storage, err := wal.Open(t.TempDir(), p, sync, ingest, testOpenOptions...)
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
	d := db.New(id, peers, storage, p, sync, ingest, db.WithTickInterval(1*time.Millisecond))
	defer d.Close()
	d.EatCommitC()

	cfg := &cluster.Configuration{
		ID:       "ingestable-1",
		MimeType: "application/json",
		Data: []byte(`{
			"ingestable": {"name": "ingestable-1", "type": "test-ingestable"}
		}`),
	}

	require.Nil(t, d.ProposeIngestable(cfg))

	// Two committed entries should appear: the ingestable Configuration and
	// the proposal that the ingestable produced via the wired channel.
	require.Eventually(t, func() bool {
		ents := readNormalProposals(storage)
		return len(ents) == 2
	}, 2*time.Second, 5*time.Millisecond,
		"expected Configuration + ingested Proposal in raft log")

	ents := readNormalProposals(storage)
	require.Equal(t, 2, len(ents))

	// Second entry should be the proposal the fake ingestable emitted.
	require.Equal(t, 1, len(ents[1].Entities))
	got := ents[1].Entities[0]
	require.Equal(t, "test-type", got.Type.ID)
	require.Equal(t, "test-key", string(got.Key))
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
