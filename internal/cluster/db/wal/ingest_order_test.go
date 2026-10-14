package wal_test

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

const (
	sessionA = uint64(0xA1)
	sessionB = uint64(0xB1)
)

// sequenced is one sequenced ingest proposal of the scenario below.
type sequenced struct {
	key      string
	seq      uint64
	session  uint64
	start    bool
	position string
}

// orderScenario is a run of sequenced proposals with every case of the rule:
// in order, a gap, another session's continuation, a new session's start,
// and the superseded session's straggler. The keys of the accepted ones, in
// order, are orderAccepted.
var orderScenario = []sequenced{
	{key: "a1", seq: 1, session: sessionA, start: true},
	{key: "a2", seq: 2, session: sessionA},
	{key: "gap", seq: 4, session: sessionA, position: "gap"},
	{key: "other", seq: 3, session: sessionB, position: "other"},
	{key: "a3", seq: 3, session: sessionA, position: "p3"},
	{key: "b4", seq: 4, session: sessionB, start: true},
	{key: "straggler", seq: 5, session: sessionA, position: "straggler"},
}

var orderAccepted = []string{"a1", "a2", "a3", "b4"}

// orderRejectedIndexes are the scenario's rejected raft indexes, with the
// first proposal at index 3.
var orderRejectedIndexes = []uint64{5, 6, 9}

func newOrderStorage(t *testing.T) (*StorageWrapper, *cluster.Type, []*pb.Entry) {
	t.Helper()
	s := NewStorageWithParser(t, nil, parser.New())
	t.Cleanup(func() { s.Cleanup() })
	tp := &cluster.Type{ID: "order-topic", Name: "order", Version: 1}
	reg, err := cluster.NewUpsertTypeEntity(tp)
	require.NoError(t, err)
	saveEntity(t, reg, s, 1, 1)
	seedIngestableConfig(t, s, "ing", 2)

	ents := make([]*pb.Entry, len(orderScenario))
	for i, sp := range orderScenario {
		p := &cluster.Proposal{
			Entities:           []*cluster.Entity{cluster.NewUpsertEntity(tp, []byte(sp.key), []byte(sp.key))},
			IngestableID:       "ing",
			SourceSeq:          100 + sp.seq,
			IngestSeq:          sp.seq,
			IngestSession:      sp.session,
			IngestSessionStart: sp.start,
		}
		if sp.position != "" {
			p.Position = cluster.Position(sp.position)
		}
		bs, err := p.Marshal()
		require.NoError(t, err)
		ents[i] = &pb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(uint64(i) + 3), Type: pb.EntryNormal.Enum(), Data: bs}
	}
	require.NoError(t, s.Save(&defaultHardState, ents, &defaultSnap))
	return s, tp, ents
}

func readOrderKeys(t *testing.T, s *StorageWrapper) []string {
	t.Helper()
	r := s.Reader("order-reader")
	var keys []string
	for {
		a, err := r.Read()
		if errors.Is(err, io.EOF) {
			return keys
		}
		require.NoError(t, err)
		for _, e := range a.Entities {
			if e.Type.ID == "order-topic" {
				keys = append(keys, string(e.Key))
			}
		}
	}
}

// requireOrderOutcome checks everything the scenario's apply must leave: the
// accepted proposals readable in order and nothing else, the rejected ones
// stored without data, the order record at the last acceptance, the bundled
// position from the last ACCEPTED proposal, and the dedup record untouched by
// a rejected one.
func requireOrderOutcome(t *testing.T, s *StorageWrapper) {
	t.Helper()
	require.Equal(t, orderAccepted, readOrderKeys(t, s))
	for _, idx := range orderRejectedIndexes {
		_, err := s.ActualAt(idx)
		require.ErrorIs(t, err, wal.ErrActualNotFound, "rejected entry %d must be stored without data", idx)
	}
	seq, session := s.IngestOrder("ing")
	require.Equal(t, uint64(4), seq)
	require.Equal(t, sessionB, session)
	require.Equal(t, "p3", string(s.Position("ing")), "a rejected proposal's bundled position must not apply")
	require.Equal(t, uint64(104), s.IngestSourceSeqHighwater("ing"), "a rejected proposal must not advance the dedup record")
	require.Equal(t, uint64(len(orderScenario)+2), s.AppliedIndex(), "a rejected entry still counts as applied")
}

// TestIngestOrder_ApplyRule pins the apply rule on a Ready batch: accept the
// next number in the same session or a new session's start; reject a gap,
// another session's continuation, and a superseded session's straggler.
func TestIngestOrder_ApplyRule(t *testing.T) {
	s, _, ents := newOrderStorage(t)
	require.NoError(t, s.ApplyCommittedBatch(ents))
	requireOrderOutcome(t, s)

	for _, idx := range orderRejectedIndexes {
		require.True(t, s.TakeIngestRejected(idx), "the notifier must learn entry %d was rejected", idx)
		require.False(t, s.TakeIngestRejected(idx), "taken once")
	}
	for _, idx := range []uint64{3, 4, 7, 8} {
		require.False(t, s.TakeIngestRejected(idx), "entry %d was accepted", idx)
	}
}

// TestIngestOrder_PerEntryMatchesBatch: the per-entry path decides alike, so
// replicas reach the same state however their Readies are cut.
func TestIngestOrder_PerEntryMatchesBatch(t *testing.T) {
	s, _, ents := newOrderStorage(t)
	for _, e := range ents {
		require.NoError(t, s.ApplyCommitted(e))
	}
	requireOrderOutcome(t, s)

	split, _, ents2 := newOrderStorage(t)
	require.NoError(t, split.ApplyCommittedBatch(ents2[:3]))
	require.NoError(t, split.ApplyCommittedBatch(ents2[3:]))
	requireOrderOutcome(t, split)
}

// TestIngestOrder_ReplayKeepsTheStoredVerdict covers the crash between the
// event append and the applied-index save: the replay finds the order record
// already past the batch, so the rule alone would now reject the accepted
// entries. The stored event decides instead.
func TestIngestOrder_ReplayKeepsTheStoredVerdict(t *testing.T) {
	s, _, ents := newOrderStorage(t)
	require.NoError(t, s.ApplyCommittedBatch(ents))
	for _, idx := range orderRejectedIndexes {
		s.TakeIngestRejected(idx)
	}

	require.NoError(t, s.SetAppliedIndexForTest(2))
	require.NoError(t, s.ApplyCommittedBatch(ents))
	requireOrderOutcome(t, s)
	for _, idx := range orderRejectedIndexes {
		require.True(t, s.TakeIngestRejected(idx), "a replayed rejection still reaches the notifier")
	}
	for _, idx := range []uint64{3, 4, 7, 8} {
		require.False(t, s.TakeIngestRejected(idx), "a replayed acceptance stays accepted")
	}
}

// TestIngestOrder_UnsequencedUntouched: a proposal without IngestSeq (a
// snapshot row, any pre-feature entry) is neither checked nor recorded.
func TestIngestOrder_UnsequencedUntouched(t *testing.T) {
	s := NewStorageWithParser(t, nil, parser.New())
	defer s.Cleanup()
	seedIngestableConfig(t, s, "ing", 1)
	applyIngestSeq(t, s, 2, "ing", 7)
	seq, session := s.IngestOrder("ing")
	require.Zero(t, seq)
	require.Zero(t, session)
	require.Equal(t, uint64(7), s.IngestSourceSeqHighwater("ing"))
}

// TestIngestOrder_SurvivesDelete: the record only moves forward, so a deleted
// generation's straggler can never match a recreated one's numbering.
func TestIngestOrder_SurvivesDelete(t *testing.T) {
	s, _, ents := newOrderStorage(t)
	require.NoError(t, s.ApplyCommittedBatch(ents))
	saveProposal(t, &cluster.Proposal{Entities: cluster.NewDeleteIngestableEntities("ing")}, s, 1, uint64(len(ents))+3)
	seq, session := s.IngestOrder("ing")
	require.Equal(t, uint64(4), seq)
	require.Equal(t, sessionB, session)
}
