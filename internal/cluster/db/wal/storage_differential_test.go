package wal_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
)

// Differential conformance: drive wal.Storage and raft.MemoryStorage — the
// reference implementation every in-process raft test runs against — through
// the SAME operation sequences and require structural agreement on the whole
// observable raft Storage surface (FirstIndex/LastIndex/Term/Entries/
// Snapshot index, including error identity: raft branches on ErrCompacted vs
// ErrUnavailable). This pins the CLASS of bug the never-compacted-log fix repaired:
// wal.Storage drifting from the contract invisibly, because the reference
// masks it in-process and only a real-binary scenario needs the divergent
// behavior (the "need non-empty snapshot" leader panic on member add).
//
// Agreement is one-sided by design: the contract requires availability from
// FirstIndex, and serving MORE than the reference is legal — after a reopen,
// a Compact that physically removed nothing leaves the wal able to serve a
// longer prefix than MemoryStorage's dummy admits. So the harness asserts:
//   - LastIndex identical;
//   - FirstIndex_wal <= FirstIndex_mem (never LESS available — the bug
//     direction);
//   - wherever the reference answers, the wal answers identically;
//   - wherever the wal answers MORE, the answer matches the oracle (the full
//     append history), so extra availability is never wrong data;
//   - ErrUnavailable (above the log) agrees exactly.

// diffHarness holds the two storages under test plus the oracle history.
type diffHarness struct {
	wal    *StorageWrapper
	mem    *raft.MemoryStorage
	oracle map[uint64]uint64 // raft index -> term, every entry ever appended
	last   uint64
}

func newDiffHarness(t *testing.T) *diffHarness {
	t.Helper()
	return &diffHarness{
		wal:    NewStorage(t, nil),
		mem:    raft.NewMemoryStorage(),
		oracle: map[uint64]uint64{},
	}
}

// append writes entries [from..to] at the given term to both storages (real
// HardState on the wal side — an empty one reads as a torn first Save) and
// applies them on the wal side so CreateSnapshot's applied-index gate holds.
func (h *diffHarness) append(t *testing.T, from, to, term uint64) {
	t.Helper()
	tp, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: fmt.Sprintf("t%d", from), Name: "T", Version: 1})
	require.NoError(t, err)
	var ents []*pb.Entry
	for i := from; i <= to; i++ {
		i, term := i, term
		p := &cluster.Proposal{Entities: []*cluster.Entity{tp}}
		bs, merr := p.Marshal()
		require.NoError(t, merr)
		ents = append(ents, &pb.Entry{Term: &term, Index: &i, Type: pb.EntryNormal.Enum(), Data: bs})
		h.oracle[i] = term
		if i > h.last {
			h.last = i
		}
	}
	vote := uint64(1)
	commit := to
	hs := pb.HardState{Term: &term, Vote: &vote, Commit: &commit}
	require.NoError(t, h.wal.Save(&hs, ents, &pb.Snapshot{}))
	for _, e := range ents {
		require.NoError(t, h.wal.ApplyCommitted(e))
	}
	require.NoError(t, h.mem.Append(ents))
	require.NoError(t, h.mem.SetHardState(&hs))
}

// snapshotAndCompact takes a snapshot at snapIdx and compacts to compactTo on
// both storages — the exact pairing the production ready loop performs.
func (h *diffHarness) snapshotAndCompact(t *testing.T, snapIdx, compactTo uint64) {
	t.Helper()
	_, err := h.wal.CreateSnapshot(snapIdx, &pb.ConfState{})
	require.NoError(t, err)
	require.NoError(t, h.wal.Compact(compactTo))
	_, err = h.mem.CreateSnapshot(snapIdx, &pb.ConfState{}, nil)
	require.NoError(t, err)
	require.NoError(t, h.mem.Compact(compactTo))
}

// reopen close-and-reopens the wal side only: the reference lives in RAM, so
// comparing against it afterwards asserts that a restart is observationally
// invisible — the half of the never-compacted-log bug that lived in Open's boundary
// recovery.
func (h *diffHarness) reopen(t *testing.T) {
	t.Helper()
	h.wal = h.wal.CloseAndReopenStorage(t)
}

func (h *diffHarness) cleanup() { h.wal.Cleanup() }

// errClass collapses errors to the identities raft branches on.
func errClass(err error) string {
	switch err {
	case nil:
		return "ok"
	case raft.ErrCompacted:
		return "compacted"
	case raft.ErrUnavailable:
		return "unavailable"
	default:
		return "other:" + err.Error()
	}
}

// conform sweeps the observable surface and asserts the agreement rules.
func (h *diffHarness) conform(t *testing.T, label string) {
	t.Helper()

	walLast, err := h.wal.LastIndex()
	require.NoError(t, err, label)
	memLast, err := h.mem.LastIndex()
	require.NoError(t, err, label)
	require.Equal(t, memLast, walLast, "%s: LastIndex must agree", label)
	require.Equal(t, h.last, walLast, "%s: LastIndex must match the oracle", label)

	walFirst, err := h.wal.FirstIndex()
	require.NoError(t, err, label)
	memFirst, err := h.mem.FirstIndex()
	require.NoError(t, err, label)
	require.LessOrEqual(t, walFirst, memFirst,
		"%s: the wal must never be LESS available than the reference (the never-compacted-bug direction)", label)

	// Term parity across [0, last+2].
	for i := uint64(0); i <= h.last+2; i++ {
		memTerm, memErr := h.mem.Term(i)
		walTerm, walErr := h.wal.Term(i)
		switch errClass(memErr) {
		case "ok":
			require.Equalf(t, "ok", errClass(walErr), "%s: Term(%d): reference answers, wal must too", label, i)
			require.Equalf(t, memTerm, walTerm, "%s: Term(%d) value", label, i)
		case "unavailable":
			require.Equalf(t, "unavailable", errClass(walErr), "%s: Term(%d): above the log must agree exactly", label, i)
		case "compacted":
			// The wal may answer where the reference has compacted — extra
			// availability — but only with the truth.
			if errClass(walErr) == "ok" {
				want := uint64(0) // index 0: the virtual dummy's term
				if i > 0 {
					want = h.oracle[i]
				}
				require.Equalf(t, want, walTerm, "%s: Term(%d): extra availability must match the oracle", label, i)
			} else {
				require.Equalf(t, "compacted", errClass(walErr), "%s: Term(%d) error identity", label, i)
			}
		default:
			t.Fatalf("%s: reference Term(%d) unexpected error: %v", label, i, memErr)
		}
	}

	// Entries parity across every in-contract [lo, hi) range.
	for lo := uint64(0); lo <= h.last; lo++ {
		for hi := lo + 1; hi <= h.last+1; hi++ {
			memEnts, memErr := h.mem.Entries(lo, hi, math.MaxUint64)
			walEnts, walErr := h.wal.Entries(lo, hi, math.MaxUint64)
			switch errClass(memErr) {
			case "ok":
				require.Equalf(t, "ok", errClass(walErr), "%s: Entries(%d,%d): reference answers, wal must too", label, lo, hi)
				requireSameIndexTerms(t, label, lo, hi, memEnts, walEnts)
			case "compacted":
				if errClass(walErr) == "ok" {
					for _, e := range walEnts { // extra availability: oracle-true
						require.Equalf(t, h.oracle[e.GetIndex()], e.GetTerm(),
							"%s: Entries(%d,%d): extra entry %d must match the oracle", label, lo, hi, e.GetIndex())
					}
				} else {
					require.Equalf(t, "compacted", errClass(walErr), "%s: Entries(%d,%d) error identity", label, lo, hi)
				}
			default:
				t.Fatalf("%s: reference Entries(%d,%d) unexpected error: %v", label, lo, hi, memErr)
			}
		}
	}

	// Snapshot index parity: both sides received the same CreateSnapshot
	// calls, and the wal's must survive its reopens.
	memSnap, err := h.mem.Snapshot()
	require.NoError(t, err, label)
	walSnap, err := h.wal.Snapshot()
	require.NoError(t, err, label)
	require.Equal(t, memSnap.GetMetadata().GetIndex(), walSnap.GetMetadata().GetIndex(),
		"%s: snapshot index must agree", label)
}

func requireSameIndexTerms(t *testing.T, label string, lo, hi uint64, want, got []*pb.Entry) {
	t.Helper()
	require.Equalf(t, len(want), len(got), "%s: Entries(%d,%d) length", label, lo, hi)
	for k := range want {
		require.Equalf(t, want[k].GetIndex(), got[k].GetIndex(), "%s: Entries(%d,%d)[%d] index", label, lo, hi, k)
		require.Equalf(t, want[k].GetTerm(), got[k].GetTerm(), "%s: Entries(%d,%d)[%d] term", label, lo, hi, k)
	}
}

// TestStorageDifferential_NeverCompacted: the never-compacted log — the
// regime the boundary fix repaired (pre-fix: FirstIndex=2, Term(0)=ErrCompacted, entry 1
// unservable; the reference serves from 1 with Term(0)=0).
func TestStorageDifferential_NeverCompacted(t *testing.T) {
	h := newDiffHarness(t)
	defer h.cleanup()
	h.append(t, 1, 5, 1)
	h.conform(t, "never-compacted")

	h.reopen(t)
	h.conform(t, "never-compacted+reopen")
}

// TestStorageDifferential_CompactedThenGrows: the production pairing
// (snapshot at applied, compact behind it), continued appends at a later
// term, and a reopen of the compacted log.
func TestStorageDifferential_CompactedThenGrows(t *testing.T) {
	h := newDiffHarness(t)
	defer h.cleanup()
	h.append(t, 1, 8, 1)
	h.snapshotAndCompact(t, 6, 4)
	h.conform(t, "compacted")

	h.append(t, 9, 12, 2)
	h.conform(t, "compacted+grown")

	h.reopen(t)
	h.conform(t, "compacted+grown+reopen")
}

// TestStorageDifferential_CompactAtOne: the smallest legal compaction
// (compactTo can reach 1 when applied is barely past the safety buffer).
// TruncateFront at seq 1 removes nothing physical, so after a reopen the wal
// legitimately serves MORE than the reference — the one-sided agreement
// rules carry the proof that the extra availability is oracle-true.
func TestStorageDifferential_CompactAtOne(t *testing.T) {
	h := newDiffHarness(t)
	defer h.cleanup()
	h.append(t, 1, 9, 1)
	h.snapshotAndCompact(t, 9, 1)
	h.conform(t, "compact@1")

	h.reopen(t)
	h.conform(t, "compact@1+reopen")
}

// TestStorageDifferential_RepeatedCompaction: the boundary must track the
// LATEST compaction across restarts, not the first.
func TestStorageDifferential_RepeatedCompaction(t *testing.T) {
	h := newDiffHarness(t)
	defer h.cleanup()
	h.append(t, 1, 10, 1)
	h.snapshotAndCompact(t, 5, 3)
	h.append(t, 11, 14, 2)
	h.snapshotAndCompact(t, 12, 8)
	h.conform(t, "recompacted")

	h.reopen(t)
	h.conform(t, "recompacted+reopen")
}
