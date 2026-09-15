package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
)

// applyIngestSeq commits one ingest proposal at raft index idx tagged
// with (ingestableID, seq). The entity is an IngestablePosition (a
// self-applying system entity that needs no prior type registration) and
// is just a vehicle — the highwater is advanced from the proposal-level
// IngestableID/SourceSeq, which is what this exercises.
func applyIngestSeq(t *testing.T, s *StorageWrapper, idx uint64, ingestableID string, seq uint64) {
	t.Helper()
	ent, err := cluster.NewUpsertIngestablePositionEntity(&cluster.IngestablePosition{ID: "vehicle", Position: []byte("p")})
	require.NoError(t, err)
	p := &cluster.Proposal{
		IngestableID: ingestableID,
		SourceSeq:    seq,
		Entities:     []*cluster.Entity{ent},
	}
	bs, err := p.Marshal()
	require.NoError(t, err)
	entry := &pb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(idx), Type: pb.EntryNormal.Enum(), Data: bs}
	require.NoError(t, s.Save(&defaultHardState, []*pb.Entry{entry}, &defaultSnap))
	require.NoError(t, s.ApplyCommitted(entry))
}

// TestSourceSeqHighwater_DeterministicAndIdempotent proves the dedup
// highwater is a deterministic function of the committed source seqs —
// the property that makes the per-replica dedup decision safe. Two
// storages fed the same seqs in different order, one with a repeat,
// converge to the same highwater because the advance is a monotonic max.
func TestSourceSeqHighwater_DeterministicAndIdempotent(t *testing.T) {
	const id = "ing"

	s1 := NewStorageWithParser(t, nil, parser.New())
	defer s1.Cleanup()
	// The highwater now advances only while the ingestable config exists (the
	// per-config-id write-guard), so seed the config before applying seqs.
	seedIngestableConfig(t, s1, id, 1)
	for i, seq := range []uint64{1, 2, 3, 4, 5} {
		applyIngestSeq(t, s1, uint64(i+2), id, seq)
	}

	s2 := NewStorageWithParser(t, nil, parser.New())
	defer s2.Cleanup()
	seedIngestableConfig(t, s2, id, 1)
	// Different order, and seq 5 applied twice — max() must absorb both.
	for i, seq := range []uint64{3, 1, 5, 2, 5, 4} {
		applyIngestSeq(t, s2, uint64(i+2), id, seq)
	}

	require.Equal(t, uint64(5), s1.IngestSourceSeqHighwater(id))
	require.Equal(t, s1.IngestSourceSeqHighwater(id), s2.IngestSourceSeqHighwater(id),
		"highwater must be a deterministic max over committed source seqs, independent of order/repeats")

	// A never-seen ingestable reports 0 (dedup nothing).
	require.Equal(t, uint64(0), s1.IngestSourceSeqHighwater("other"))
}

// TestSourceSeqHighwater_SurvivesReopen is the durability guarantee the
// whole feature rests on: the highwater must survive a process restart,
// or a restarted node would re-accept re-emitted proposals it already
// committed. Apply some seqs, close, reopen the same dir, assert the
// highwater is still there.
func TestSourceSeqHighwater_SurvivesReopen(t *testing.T) {
	const id = "ing"

	s := NewStorageWithParser(t, nil, parser.New())
	defer s.Cleanup()
	seedIngestableConfig(t, s, id, 1)
	for i, seq := range []uint64{10, 20, 30} {
		applyIngestSeq(t, s, uint64(i+2), id, seq)
	}
	require.Equal(t, uint64(30), s.IngestSourceSeqHighwater(id))

	reopened, err := s.CloseAndReopen()
	require.NoError(t, err)
	defer reopened.Cleanup()

	require.Equal(t, uint64(30), reopened.IngestSourceSeqHighwater(id),
		"highwater must survive a restart so a recovered node still dedups re-emits")
}

// applyIngestTxnSeq is applyIngestSeq with a source-transaction identity and
// an optional bundled position — the shape the unified watermark path emits.
func applyIngestTxnSeq(t *testing.T, s *StorageWrapper, idx uint64, ingestableID string, seq uint64, txnID string, bundle []byte) {
	t.Helper()
	ent, err := cluster.NewUpsertIngestablePositionEntity(&cluster.IngestablePosition{ID: "vehicle", Position: []byte("p")})
	require.NoError(t, err)
	p := &cluster.Proposal{
		IngestableID:   ingestableID,
		SourceSeq:      seq,
		SourceTxnID:    txnID,
		TxnScopedDedup: txnID != "", // the bundling dialects' opt-in
		Position:       bundle,
		Entities:       []*cluster.Entity{ent},
	}
	bs, err := p.Marshal()
	require.NoError(t, err)
	entry := &pb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(idx), Type: pb.EntryNormal.Enum(), Data: bs}
	require.NoError(t, s.Save(&defaultHardState, []*pb.Entry{entry}, &defaultSnap))
	require.NoError(t, s.ApplyCommitted(entry))
}

// TestSourceDedup_TxnScoped pins the transaction-scoped record fold: within
// one transaction the seq highwater is a monotonic max; a DIFFERENT
// transaction RESETS the record — an inflated highwater from one
// transaction is never compared against the next (the sub-collapse and
// failover closures), and the fold is a deterministic function of the
// committed order.
func TestSourceDedup_TxnScoped(t *testing.T) {
	const id = "ing-txn"

	s := NewStorageWithParser(t, nil, parser.New())
	defer s.Cleanup()
	seedIngestableConfig(t, s, id, 1)

	// Txn A: parts at 5 then 3349 (inflated), monotonic within the txn.
	applyIngestTxnSeq(t, s, 2, id, 5, "A", nil)
	applyIngestTxnSeq(t, s, 3, id, 3349, "A", nil)
	txn, seq := s.IngestSourceDedup(id)
	require.Equal(t, "A", txn)
	require.Equal(t, uint64(3349), seq)

	// A replayed part of the SAME txn does not regress the record.
	applyIngestTxnSeq(t, s, 4, id, 5, "A", nil)
	_, seq = s.IngestSourceDedup(id)
	require.Equal(t, uint64(3349), seq, "a same-transaction replay must not regress the highwater")

	// Txn B at a LOWER seq resets the record: B is a different transaction,
	// so A's inflated highwater must not shadow it.
	applyIngestTxnSeq(t, s, 5, id, 288, "B", nil)
	txn, seq = s.IngestSourceDedup(id)
	require.Equal(t, "B", txn)
	require.Equal(t, uint64(288), seq, "a new transaction resets the record to its own coordinates")

	// The record survives a reopen, like the legacy highwater always has.
	reopened, err := s.CloseAndReopen()
	require.NoError(t, err)
	defer reopened.Cleanup()
	txn, seq = reopened.IngestSourceDedup(id)
	require.Equal(t, "B", txn)
	require.Equal(t, uint64(288), seq)
}

// TestSourceDedup_LegacyRegimeAndUpgrade pins the coexistence story: entries
// with no transaction identity (a pre-upgrade log, or a dialect that stamps
// none) fold under the legacy scalar semantics bit-for-bit, and the first
// transaction-stamped apply flips the record to the new regime.
func TestSourceDedup_LegacyRegimeAndUpgrade(t *testing.T) {
	const id = "ing-legacy"

	s := NewStorageWithParser(t, nil, parser.New())
	defer s.Cleanup()
	seedIngestableConfig(t, s, id, 1)

	// Legacy regime: empty txn ids, global monotonic max.
	for i, seq := range []uint64{3, 1, 5, 2} {
		applyIngestTxnSeq(t, s, uint64(i+2), id, seq, "", nil)
	}
	txn, seq := s.IngestSourceDedup(id)
	require.Empty(t, txn, "no-identity entries stay in the legacy scalar regime")
	require.Equal(t, uint64(5), seq, "legacy semantics are a global monotonic max")

	// The first transaction-stamped apply flips the regime.
	applyIngestTxnSeq(t, s, 6, id, 2, "T1", nil)
	txn, seq = s.IngestSourceDedup(id)
	require.Equal(t, "T1", txn)
	require.Equal(t, uint64(2), seq, "the regime flip resets the record to the stamped transaction")
}

// TestSourceDedup_BundledPositionAtomicWithRecord is the watermark-atomicity
// criterion: a commit-flush proposal carrying the encoded post-transaction
// position applies BOTH the entities/dedup record AND the resume position in
// one entry — after a crash-reopen there is no window where the committed
// transaction is missing from the durable resume state.
func TestSourceDedup_BundledPositionAtomicWithRecord(t *testing.T) {
	const id = "ing-bundle"

	s := NewStorageWithParser(t, nil, parser.New())
	defer s.Cleanup()
	seedIngestableConfig(t, s, id, 1)

	watermark := []byte("gtid-set-after-T7")
	applyIngestTxnSeq(t, s, 2, id, 10, "T7", watermark)

	txn, seq := s.IngestSourceDedup(id)
	require.Equal(t, "T7", txn)
	require.Equal(t, uint64(10), seq)
	require.Equal(t, cluster.Position(watermark), s.Position(id),
		"the bundled position must be durable the moment the transaction is")

	reopened, err := s.CloseAndReopen()
	require.NoError(t, err)
	defer reopened.Cleanup()
	require.Equal(t, cluster.Position(watermark), reopened.Position(id),
		"crash-reopen: the resume position must cover every committed transaction — no window")
	txn, _ = reopened.IngestSourceDedup(id)
	require.Equal(t, "T7", txn)
}

// TestSourceDedup_StampedWithoutOptInStaysScalar pins the
// provenance/dedup-scope decoupling: a dialect that stamps SourceTxnID for
// provenance but does NOT opt into txn-scoped dedup (its checkpoint is
// coarser than its transaction stamps — the SQL Server shape) folds under
// the legacy scalar semantics, identical to today.
func TestSourceDedup_StampedWithoutOptInStaysScalar(t *testing.T) {
	const id = "ing-noopt"

	s := NewStorageWithParser(t, nil, parser.New())
	defer s.Cleanup()
	seedIngestableConfig(t, s, id, 1)

	apply := func(idx, seq uint64, txn string) {
		t.Helper()
		ent, err := cluster.NewUpsertIngestablePositionEntity(&cluster.IngestablePosition{ID: "vehicle", Position: []byte("p")})
		require.NoError(t, err)
		p := &cluster.Proposal{
			IngestableID: id, SourceSeq: seq, SourceTxnID: txn, // NO TxnScopedDedup
			Entities: []*cluster.Entity{ent},
		}
		bs, err := p.Marshal()
		require.NoError(t, err)
		entry := &pb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(idx), Type: pb.EntryNormal.Enum(), Data: bs}
		require.NoError(t, s.Save(&defaultHardState, []*pb.Entry{entry}, &defaultSnap))
		require.NoError(t, s.ApplyCommitted(entry))
	}

	apply(2, 5, "v100")
	apply(3, 3, "v101") // a lower seq from a "different transaction"
	txn, seq := s.IngestSourceDedup(id)
	require.Empty(t, txn, "without the opt-in the record must stay in the scalar regime")
	require.Equal(t, uint64(5), seq, "scalar semantics: global monotonic max, txn stamps notwithstanding")
}
