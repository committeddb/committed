package wal_test

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// Differential conformance: drive wal.Storage and raft.MemoryStorage — the
// reference implementation every in-process raft test runs against — through
// the SAME operation sequences and require structural agreement on the whole
// observable raft Storage surface (FirstIndex/LastIndex/Term/Entries/
// Snapshot index, including error identity: raft branches on ErrCompacted vs
// ErrUnavailable). This pins the CLASS of bug the never-compacted-log fix
// repaired: wal.Storage drifting from the contract invisibly, because the
// reference masks it in-process and only a real-binary scenario needs the
// divergent behavior (the "need non-empty snapshot" leader panic on member
// add).
//
// The operation set covers append, term bumps, CONFLICTING-suffix truncation
// (a higher-term leader overwriting an uncommitted tail — the other place
// our bespoke append logic could drift, with data-divergence consequences
// rather than availability ones), the production snapshot+compact pairing,
// and clean wal-only reopens (the reference lives in RAM, so post-reopen
// comparison asserts restarts are observationally invisible). Sequences run
// both scripted (the known regimes) and seeded-random (the corners nobody
// scripts — the enabling condition of the original bug).
//
// Deliberately OUT of this harness, covered by their own targeted suites:
//   - snapshot INSTALL (leader → lagging follower): production drives it
//     through the db-layer processSnapshot before Save (the event-index
//     guard in saveWithSnapshot makes a raw-Save install a no-op), and the
//     installed regime's surface is pinned by apply_snapshot_test.
//   - DIRTY reopens: the torn-write recovery paths (reconcileEntryLog*)
//     have targeted crash-shape tests; the differential's reopen is clean.
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
//     append/truncation history), so extra availability is never wrong data;
//   - ErrUnavailable (above the log) agrees exactly.

// diffHarness holds the two storages under test, the oracle history, and the
// raft-legality bookkeeping the operations respect (conflicts only above
// applied, snapshots only at-or-below applied, both only above the last
// compaction).
type diffHarness struct {
	wal    *StorageWrapper
	mem    *raft.MemoryStorage
	oracle map[uint64]uint64    // raft index -> term: current truth
	ents   map[uint64]*pb.Entry // raft index -> live entry (for apply)
	tp     *cluster.Entity

	last      uint64
	applied   uint64
	term      uint64
	snapIdx   uint64
	compacted uint64
}

func newDiffHarness(t *testing.T) *diffHarness {
	t.Helper()
	tp, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "t", Name: "T", Version: 1})
	require.NoError(t, err)
	return &diffHarness{
		wal:    NewStorage(t, nil),
		mem:    raft.NewMemoryStorage(),
		oracle: map[uint64]uint64{},
		ents:   map[uint64]*pb.Entry{},
		tp:     tp,
		term:   1,
	}
}

// save writes entries [from..from+n-1] at the harness's current term to both
// storages, with a REAL HardState (an empty one reads as a torn first Save;
// Commit tracks applied, which the legality rules keep un-truncatable).
func (h *diffHarness) save(t *testing.T, from uint64, n int) {
	t.Helper()
	var ents []*pb.Entry
	for k := 0; k < n; k++ {
		i, term := from+uint64(k), h.term
		p := &cluster.Proposal{Entities: []*cluster.Entity{h.tp}}
		bs, merr := p.Marshal()
		require.NoError(t, merr)
		ents = append(ents, &pb.Entry{Term: &term, Index: &i, Type: pb.EntryNormal.Enum(), Data: bs})
	}
	vote := uint64(1)
	hsTerm, commit := h.term, h.applied
	hs := pb.HardState{Term: &hsTerm, Vote: &vote, Commit: &commit}
	require.NoError(t, h.wal.Save(&hs, ents, &pb.Snapshot{}))
	require.NoError(t, h.mem.Append(ents))
	require.NoError(t, h.mem.SetHardState(&hs))

	// Oracle: a save at `from` truncates every prior entry >= from.
	for i := from; i <= h.last; i++ {
		delete(h.oracle, i)
		delete(h.ents, i)
	}
	for _, e := range ents {
		h.oracle[e.GetIndex()] = e.GetTerm()
		h.ents[e.GetIndex()] = e
	}
	h.last = from + uint64(n) - 1
}

// append extends the log by n entries at the current term.
func (h *diffHarness) append(t *testing.T, n int) {
	t.Helper()
	h.save(t, h.last+1, n)
}

// conflict simulates a higher-term leader overwriting the uncommitted tail:
// bump the term and save n entries starting at `from`. Raft's real
// constraint is that conflicts start above COMMIT (log matching: no
// leader's log conflicts with committed entries); this harness pins
// HardState.Commit == applied in save(), which is the only reason the rule
// below may read `from > applied`. If commit is ever decoupled from applied
// here, this guard must move to commit or the generator will produce
// sequences real raft cannot — failing the differential with false
// positives that look like storage bugs.
func (h *diffHarness) conflict(t *testing.T, from uint64, n int) {
	t.Helper()
	require.Greater(t, from, h.applied, "test-sequence bug: conflicts only above applied")
	require.LessOrEqual(t, from, h.last, "test-sequence bug: a conflict overlaps the existing log")
	h.term++
	h.save(t, from, n)
}

// apply runs ApplyCommitted on the wal side up to upTo (the reference has no
// apply concept). CreateSnapshot's applied-index gate is why sequences apply
// before they snapshot.
func (h *diffHarness) apply(t *testing.T, upTo uint64) {
	t.Helper()
	require.LessOrEqual(t, upTo, h.last)
	for i := h.applied + 1; i <= upTo; i++ {
		require.NoError(t, h.wal.ApplyCommitted(h.ents[i]))
	}
	h.applied = upTo
}

// snapshotAndCompact takes a snapshot at snapIdx and compacts to compactTo on
// both storages — the exact pairing the production ready loop performs.
func (h *diffHarness) snapshotAndCompact(t *testing.T, snapIdx, compactTo uint64) {
	t.Helper()
	require.LessOrEqual(t, snapIdx, h.applied, "test-sequence bug: snapshot above applied")
	_, err := h.wal.CreateSnapshot(snapIdx, &pb.ConfState{})
	require.NoError(t, err)
	require.NoError(t, h.wal.Compact(compactTo))
	_, err = h.mem.CreateSnapshot(snapIdx, &pb.ConfState{}, nil)
	require.NoError(t, err)
	require.NoError(t, h.mem.Compact(compactTo))
	h.snapIdx, h.compacted = snapIdx, compactTo
}

// reopen close-and-reopens the wal side only: the reference lives in RAM, so
// comparing against it afterwards asserts that a restart is observationally
// invisible — the half of the never-compacted-log bug that lived in Open's
// boundary recovery.
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
// regime the boundary fix repaired (pre-fix: FirstIndex=2,
// Term(0)=ErrCompacted, entry 1 unservable; the reference serves from 1 with
// Term(0)=0).
func TestStorageDifferential_NeverCompacted(t *testing.T) {
	h := newDiffHarness(t)
	defer h.cleanup()
	h.append(t, 5)
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
	h.append(t, 8)
	h.apply(t, 8)
	h.snapshotAndCompact(t, 6, 4)
	h.conform(t, "compacted")

	h.term++
	h.append(t, 4)
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
	h.append(t, 9)
	h.apply(t, 9)
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
	h.append(t, 10)
	h.apply(t, 10)
	h.snapshotAndCompact(t, 5, 3)
	h.term++
	h.append(t, 4)
	h.apply(t, 14)
	h.snapshotAndCompact(t, 12, 8)
	h.conform(t, "recompacted")

	h.reopen(t)
	h.conform(t, "recompacted+reopen")
}

// TestStorageDifferential_ConflictTruncation: a higher-term leader overwrites
// the uncommitted tail — the routine leader-change path our bespoke append
// truncation implements (TruncateBack + the lost-proposal sweep). Drift here
// would be replica DATA divergence, strictly worse than an availability bug.
func TestStorageDifferential_ConflictTruncation(t *testing.T) {
	h := newDiffHarness(t)
	defer h.cleanup()
	h.append(t, 8)
	h.apply(t, 3)

	// Overwrite 5..6 at term 2: entries 7..8 die with the truncation, the
	// log SHRINKS to 6, and index 4 keeps its term-1 entry below the
	// conflict point.
	h.conflict(t, 5, 2)
	h.conform(t, "conflicted")

	// The replacement suffix grows and survives a restart.
	h.append(t, 3)
	h.conform(t, "conflicted+grown")
	h.reopen(t)
	h.conform(t, "conflicted+grown+reopen")

	// A second conflict after the reopen, then compaction across the healed
	// log — the full lifecycle over a truncated history.
	h.conflict(t, 7, 2)
	h.apply(t, 8)
	h.snapshotAndCompact(t, 8, 5)
	h.conform(t, "conflicted+recompacted")
	h.reopen(t)
	h.conform(t, "conflicted+recompacted+reopen")
}

// TestStorageDifferential_ConflictWipesEntireLog: the contested-first-
// election shape — a higher-term leader replaces a log with NOTHING
// committed, so the conflict starts at index 1 and every retained entry
// dies. tidwall's TruncateBack cannot express an empty log, so appendEntries
// swaps in a fresh one; before that fix (found by seed 2 of the randomized
// differential) the Save failed ErrOutOfRange and wedged the node
// mid-election.
func TestStorageDifferential_ConflictWipesEntireLog(t *testing.T) {
	h := newDiffHarness(t)
	defer h.cleanup()
	h.append(t, 4) // nothing applied: the whole log is an uncommitted tail

	h.conflict(t, 1, 2)
	h.conform(t, "wiped")

	h.append(t, 3)
	h.apply(t, 4)
	h.snapshotAndCompact(t, 4, 2)
	h.conform(t, "wiped+regrown+compacted")

	h.reopen(t)
	h.conform(t, "wiped+regrown+compacted+reopen")
}

// TestStorageDifferential_MidReadWipeReturnsSentinels: the raft Storage
// contract requires a mutation racing a read to surface as a BARE sentinel
// (etcd raft panics on anything else). Compact races were already mapped;
// the full-wipe conflicting append introduced a new racing shape — the log
// SHRINKS under a reader that has already passed its bounds checks. The
// test-only hook injects the wipe exactly between the bounds check and the
// entry read, on the same goroutine, which is observationally identical to
// the cross-goroutine race (raft reads on its node goroutine, Save runs on
// the serve goroutine).
func TestStorageDifferential_MidReadWipeReturnsSentinels(t *testing.T) {
	h := newDiffHarness(t)
	defer h.cleanup()
	h.append(t, 4) // uncommitted tail: nothing applied, commit=0

	// The wipe a hook firing will perform: a term-2 leader replacing the
	// whole log with entries 1..2 — raw wal-side Save only (the reference
	// and oracle are updated after the race so the final conform is honest).
	wipe := func() {
		var ents []*pb.Entry
		for i := uint64(1); i <= 2; i++ {
			i, term := i, uint64(2)
			p := &cluster.Proposal{Entities: []*cluster.Entity{h.tp}}
			bs, err := p.Marshal()
			require.NoError(t, err)
			ents = append(ents, &pb.Entry{Term: &term, Index: &i, Type: pb.EntryNormal.Enum(), Data: bs})
		}
		vote, hsTerm, commit := uint64(1), uint64(2), uint64(0)
		hs := pb.HardState{Term: &hsTerm, Vote: &vote, Commit: &commit}
		require.NoError(t, h.wal.Save(&hs, ents, &pb.Snapshot{}))
	}

	// Term(4) was in-bounds when the read began; the wipe lands mid-read and
	// the log now ends at 2 — the fallback must answer ErrUnavailable, never
	// a wrapped error.
	fired := false
	wal.SetEntryReadRaceHookForTest(func() {
		if !fired {
			fired = true
			wipe()
		}
	})
	defer wal.SetEntryReadRaceHookForTest(nil)
	_, err := h.wal.Term(4)
	require.True(t, fired, "the hook must have injected the wipe")
	require.ErrorIs(t, err, raft.ErrUnavailable,
		"a mid-read shrink must surface as the bare Unavailable sentinel")

	// Same shape for Entries: the range was legal at the bounds check, the
	// log shrank underneath — ErrCompacted (the sentinel raftLog.slice
	// tolerates), never a wrapped error.
	h2 := newDiffHarness(t)
	defer h2.cleanup()
	h2.append(t, 4)
	fired = false
	wal.SetEntryReadRaceHookForTest(func() {
		if !fired {
			fired = true
			var ents []*pb.Entry
			for i := uint64(1); i <= 2; i++ {
				i, term := i, uint64(2)
				p := &cluster.Proposal{Entities: []*cluster.Entity{h2.tp}}
				bs, merr := p.Marshal()
				require.NoError(t, merr)
				ents = append(ents, &pb.Entry{Term: &term, Index: &i, Type: pb.EntryNormal.Enum(), Data: bs})
			}
			vote, hsTerm, commit := uint64(1), uint64(2), uint64(0)
			hs := pb.HardState{Term: &hsTerm, Vote: &vote, Commit: &commit}
			require.NoError(t, h2.wal.Save(&hs, ents, &pb.Snapshot{}))
		}
	})
	_, err = h2.wal.Entries(3, 5, math.MaxUint64)
	require.True(t, fired)
	require.ErrorIs(t, err, raft.ErrCompacted,
		"a mid-read shrink must surface as the bare Compacted sentinel")
	wal.SetEntryReadRaceHookForTest(nil)

	// Steady state after the race is coherent: bring the reference and
	// oracle up to date with the wipe and run the full conformance sweep.
	for i := uint64(1); i <= 4; i++ {
		delete(h2.oracle, i)
		delete(h2.ents, i)
	}
	h2.term = 2
	h2.last = 4
	h2.save(t, 1, 2)
	h2.conform(t, "post-race")
}

// TestStorageDifferential_Randomized: seeded-random interleavings of the
// whole operation set. The scripted tests pin the regimes we know about; the
// original bug lived in a corner nobody scripted, and this driver guards
// against the NEXT such corner. Fixed seeds keep it deterministic and
// CI-stable; each sequence respects raft legality (conflicts above applied,
// snapshots at-or-below applied, compactions above the previous one).
func TestStorageDifferential_Randomized(t *testing.T) {
	for seed := int64(1); seed <= 6; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			h := newDiffHarness(t)
			defer h.cleanup()
			h.append(t, 1+rng.Intn(3)) // never start empty: raft logs begin at 1

			const ops = 28
			for op := 0; op < ops; op++ {
				switch draw := rng.Intn(100); {
				case draw < 35 && h.last < 22:
					if rng.Intn(4) == 0 {
						h.term++ // an election without a conflict
					}
					h.append(t, 1+rng.Intn(3))
				case draw < 50 && h.last > h.applied:
					from := h.applied + 1 + uint64(rng.Intn(int(h.last-h.applied)))
					h.conflict(t, from, 1+rng.Intn(3))
				case draw < 70 && h.applied < h.last:
					h.apply(t, h.applied+1+uint64(rng.Intn(int(h.last-h.applied))))
				case draw < 85 && h.applied > h.compacted && h.applied > h.snapIdx:
					lo := max(h.snapIdx, h.compacted)
					snap := lo + 1 + uint64(rng.Intn(int(h.applied-lo)))
					compactTo := h.compacted + 1 + uint64(rng.Intn(int(snap-h.compacted)))
					h.snapshotAndCompact(t, snap, compactTo)
				default:
					h.reopen(t)
				}
				if op%6 == 5 {
					h.conform(t, fmt.Sprintf("seed%d/op%d", seed, op))
				}
			}
			h.conform(t, fmt.Sprintf("seed%d/final", seed))
		})
	}
}
