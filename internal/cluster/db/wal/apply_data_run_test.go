package wal_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
)

// cdcEntries are n CDC transactions of one ingestable at raft indexes from
// start: each a row stamped with a refresh generation, the dedup stamp, and
// the bundled position after it — every data write a streaming transaction
// makes at apply. Every fifth one also deletes a row, adding the tombstone
// writes.
func cdcEntries(t *testing.T, tp *cluster.Type, start uint64, n int) []*pb.Entry {
	t.Helper()
	ents := make([]*pb.Entry, n)
	for i := range ents {
		row := cluster.NewUpsertEntity(tp, fmt.Appendf(nil, "k%d", i), []byte("v"))
		row.Generation = 1
		entities := []*cluster.Entity{row}
		if i%5 == 4 {
			entities = append(entities, cluster.NewDeleteEntity(tp, fmt.Appendf(nil, "k%d", i-1)))
		}
		p := &cluster.Proposal{
			Entities:       entities,
			IngestableID:   "ing",
			SourceSeq:      uint64(i + 1),
			SourceTxnID:    fmt.Sprintf("tx-%d", i),
			TxnScopedDedup: true,
			Position:       cluster.Position(fmt.Sprintf("p-%d", i+1)),
		}
		bs, err := p.Marshal()
		require.NoError(t, err)
		ents[i] = &pb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(start + uint64(i)), Type: pb.EntryNormal.Enum(), Data: bs}
	}
	return ents
}

func newDataRunStorage(t *testing.T) (*StorageWrapper, *cluster.Type) {
	t.Helper()
	s := NewStorageWithParser(t, nil, parser.New())
	t.Cleanup(func() { s.Cleanup() })
	tp := &cluster.Type{ID: "cdc-topic", Name: "cdc", Version: 1}
	reg, err := cluster.NewUpsertTypeEntity(tp)
	require.NoError(t, err)
	saveEntity(t, reg, s, 1, 1)
	seedIngestableConfig(t, s, "ing", 2)
	return s, tp
}

// TestApplyDataRun_OneCommitPerReady pins the apply-side write batching: a
// Ready of N CDC transactions makes ONE bbolt commit for all their data
// writes and the applied-index save, not up to three commits per entry.
// Each bbolt commit is two fsyncs, and they ran on the raft goroutine.
func TestApplyDataRun_OneCommitPerReady(t *testing.T) {
	s, tp := newDataRunStorage(t)
	const n = 50
	ents := cdcEntries(t, tp, 3, n)
	require.NoError(t, s.Save(&defaultHardState, ents, &defaultSnap))

	before := s.KVWriteOpsForTest()
	require.NoError(t, s.ApplyCommittedBatch(ents))
	require.Equal(t, int64(1), s.KVWriteOpsForTest()-before,
		"one commit for the run's data writes and the applied index together")

	require.Equal(t, fmt.Sprintf("p-%d", n), string(s.Position("ing")))
	require.Equal(t, uint64(n), s.IngestSourceSeqHighwater("ing"))
	require.Equal(t, uint64(1), s.TopicRefreshEpoch(tp.ID))
	require.Equal(t, uint64(n+2), s.AppliedIndex())
}

// TestApplyDataRun_SameStateAsPerEntry: the batched run leaves exactly the
// state per-entry apply leaves, so replicas agree however their Readies are
// cut, and a replayed batch is a no-op.
func TestApplyDataRun_SameStateAsPerEntry(t *testing.T) {
	batched, tp := newDataRunStorage(t)
	ents := cdcEntries(t, tp, 3, 40)
	require.NoError(t, batched.Save(&defaultHardState, ents, &defaultSnap))
	require.NoError(t, batched.ApplyCommittedBatch(ents))
	require.NoError(t, batched.SetAppliedIndexForTest(2))
	require.NoError(t, batched.ApplyCommittedBatch(ents), "replay of the whole batch")

	single, tp2 := newDataRunStorage(t)
	ents2 := cdcEntries(t, tp2, 3, 40)
	require.NoError(t, single.Save(&defaultHardState, ents2, &defaultSnap))
	for _, e := range ents2 {
		require.NoError(t, single.ApplyCommitted(e))
	}

	require.Equal(t, single.Position("ing"), batched.Position("ing"))
	txn1, seq1 := single.IngestSourceDedup("ing")
	txn2, seq2 := batched.IngestSourceDedup("ing")
	require.Equal(t, txn1, txn2)
	require.Equal(t, seq1, seq2)
	require.Equal(t, single.TopicRefreshEpoch(tp.ID), batched.TopicRefreshEpoch(tp.ID))
	require.Equal(t, single.AppliedIndex(), batched.AppliedIndex())
	for i := 4; i < 40; i += 5 {
		key := fmt.Appendf(nil, "k%d", i-1)
		require.Equal(t, single.EventTombstoneIndicesForTest(tp.ID, key), batched.EventTombstoneIndicesForTest(tp.ID, key))
		require.Len(t, batched.EventTombstoneIndicesForTest(tp.ID, key), 1, "a replayed delete is recorded once")
	}
}

// TestApplyDataRun_ConfigInTheMiddle: a config entry splits the run. The
// entries after it see its effect — here, a type it creates — and the
// ingestable delete it carries reaps the position the run before it wrote,
// as per-entry apply would.
func TestApplyDataRun_ConfigInTheMiddle(t *testing.T) {
	s, tp := newDataRunStorage(t)
	first := cdcEntries(t, tp, 3, 5)

	late := &cluster.Type{ID: "late-topic", Name: "late", Version: 1}
	reg, err := cluster.NewUpsertTypeEntity(late)
	require.NoError(t, err)
	regBytes, err := (&cluster.Proposal{Entities: []*cluster.Entity{reg}}).Marshal()
	require.NoError(t, err)
	regEntry := &pb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(8), Type: pb.EntryNormal.Enum(), Data: regBytes}

	useBytes, err := (&cluster.Proposal{Entities: []*cluster.Entity{cluster.NewUpsertEntity(late, []byte("x"), []byte("v"))}}).Marshal()
	require.NoError(t, err)
	useEntry := &pb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(9), Type: pb.EntryNormal.Enum(), Data: useBytes}

	delBytes, err := (&cluster.Proposal{Entities: cluster.NewDeleteIngestableEntities("ing")}).Marshal()
	require.NoError(t, err)
	delEntry := &pb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(10), Type: pb.EntryNormal.Enum(), Data: delBytes}

	ents := append(append([]*pb.Entry{}, first...), regEntry, useEntry, delEntry)
	require.NoError(t, s.Save(&defaultHardState, ents, &defaultSnap))
	require.NoError(t, s.ApplyCommittedBatch(ents), "a type created mid-Ready must resolve for the entry after it")

	require.Nil(t, s.Position("ing"), "the delete after the run reaps the position the run wrote")
	_, seq := s.IngestSourceDedup("ing")
	require.Zero(t, seq, "the delete after the run reaps the dedup record")
	require.Equal(t, uint64(10), s.AppliedIndex())
}
