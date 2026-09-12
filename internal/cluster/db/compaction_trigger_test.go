package db_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster/db"
	dbtesting "github.com/committeddb/committed/internal/cluster/db/testing"
)

// TestMaybeCompact_SizeLimb drives the size limb of the "10GB or 1hr"
// policy: when RaftLogApproxSize crosses the configured threshold
// maybeCompact fires exactly one CreateSnapshot + Compact, at a
// compact point ≤ EventIndex (the safety constraint).
func TestMaybeCompact_SizeLimb(t *testing.T) {
	fake := newCompactionStorage(100, 100, 5000)
	r := db.NewRaftForCompactionTest(fake, 4096 /*maxSize*/, 0 /*maxAge disabled*/, zap.NewNop())

	r.MaybeCompactForTest()

	require.Equal(t, 1, len(fake.snapshots), "size-limb trigger must call CreateSnapshot")
	require.Equal(t, 1, len(fake.compacts), "and Compact")

	// Compact point = applied (100) - safetyBuffer (8) = 92 — leaves
	// a small window for follower AppendEntries catchup.
	require.Equal(t, uint64(92), fake.compacts[0])
	require.Equal(t, uint64(92), r.LastCompactedIndexForTest())
}

// TestMaybeCompact_AgeLimb drives the age limb: size is well below
// the threshold, but the last compaction happened long enough ago
// that the clock-driven trigger fires.
func TestMaybeCompact_AgeLimb(t *testing.T) {
	// Size well under the threshold so only the age limb can fire.
	fake := newCompactionStorage(100, 100, 128)
	r := db.NewRaftForCompactionTest(fake, 10*1024*1024 /*maxSize big*/, 10*time.Millisecond /*maxAge*/, zap.NewNop())

	// Back-date lastCompactTime so the age threshold is already past.
	r.SetLastCompactTimeForTest(time.Now().Add(-time.Hour))

	r.MaybeCompactForTest()

	require.Equal(t, 1, len(fake.snapshots), "age-limb trigger must call CreateSnapshot")
	require.Equal(t, 1, len(fake.compacts))
}

// TestMaybeCompact_BothDisabled verifies the policy-disabled shape:
// with both limbs at 0, maybeCompact is a no-op no matter how big
// the log is or how long since the last compaction.
func TestMaybeCompact_BothDisabled(t *testing.T) {
	fake := newCompactionStorage(100, 100, 1<<40)
	r := db.NewRaftForCompactionTest(fake, 0, 0, zap.NewNop())
	r.SetLastCompactTimeForTest(time.Now().Add(-100 * time.Hour))

	r.MaybeCompactForTest()

	require.Equal(t, 0, len(fake.snapshots))
	require.Equal(t, 0, len(fake.compacts))
}

// TestMaybeCompact_SkipsWhenAppliedPastEventIndex is the safety constraint:
// the snapshot is taken AT applied, so if applied somehow exceeds EventIndex
// there is no internally consistent state to snapshot — the payload (as-of
// applied) would claim state past the durable event log. maybeCompact must skip
// entirely rather than ship such a snapshot. This is a defensive path
// (checkStorageInvariant fatals on this P<R state earlier in the same Ready
// iteration, so it should never be reached), but the guard must hold.
func TestMaybeCompact_SkipsWhenAppliedPastEventIndex(t *testing.T) {
	// Applied = 200 but EventIndex = 50 (event log far behind — a P<R state that
	// should never occur under the Ready loop's invariant check).
	fake := newCompactionStorage(200 /*applied*/, 50 /*eventIdx*/, 1<<30 /*size>threshold*/)
	r := db.NewRaftForCompactionTest(fake, 4096, 0, zap.NewNop())

	r.MaybeCompactForTest()

	require.Equal(t, 0, len(fake.snapshots), "must not snapshot state past the event log")
	require.Equal(t, 0, len(fake.compacts), "and must not compact")
}

// TestMaybeCompact_SnapshotsAtAppliedNotCompactPoint pins the decouple of the
// snapshot index from the raft-log truncation point. The snapshot payload is the
// whole live bbolt (state as-of applied) and its ConfState is the live one, so
// the snapshot MUST be taken at applied. Taking it at the trailing compact point
// (applied - safetyBuffer, the old behavior) shipped a snapshot whose
// payload/ConfState led its own Index by safetyBuffer entries — panicking a
// lagging follower that replays the intervening conf-change, and bricking a
// crashed install whose embedded applied index sat ahead of the Index.
func TestMaybeCompact_SnapshotsAtAppliedNotCompactPoint(t *testing.T) {
	fake := newCompactionStorage(100 /*applied*/, 100 /*eventIdx*/, 4096 /*size>threshold*/)
	r := db.NewRaftForCompactionTest(fake, 4096, 0, zap.NewNop())

	r.MaybeCompactForTest()

	require.Equal(t, 1, len(fake.snapshots))
	snapIndex := fake.snapshots[0]
	require.Equal(t, uint64(100), snapIndex,
		"snapshot must be taken AT applied (100), not the trailing compact point (92)")
	require.Equal(t, uint64(92), fake.compacts[0],
		"but the raft-log truncation still trails applied by safetyBuffer")
}

// TestMaybeCompact_BelowSafetyBufferIsNoOp verifies that a node that
// has barely applied anything doesn't compact — even if size
// somehow crossed the threshold (e.g., a noisy fresh cluster).
func TestMaybeCompact_BelowSafetyBufferIsNoOp(t *testing.T) {
	fake := newCompactionStorage(5 /*applied small*/, 5, 1<<30)
	r := db.NewRaftForCompactionTest(fake, 4096, 0, zap.NewNop())

	r.MaybeCompactForTest()

	require.Equal(t, 0, len(fake.compacts), "must not compact when appliedIndex ≤ safetyBuffer")
}

// TestMaybeCompact_DiskPressureLimb drives the disk-pressure hint: with both
// the size and age limbs disabled and the log well under any size threshold,
// compaction still fires when the watcher has flagged disk pressure — the
// "try to free space first" nudge — at the same safety-buffered compact point.
func TestMaybeCompact_DiskPressureLimb(t *testing.T) {
	fake := newCompactionStorage(100 /*applied*/, 100 /*eventIdx*/, 0 /*size*/)
	r := db.NewRaftForCompactionTest(fake, 0 /*maxSize disabled*/, 0 /*maxAge disabled*/, zap.NewNop())

	r.SetCompactionPressureForTest(true)
	r.MaybeCompactForTest()

	require.Equal(t, 1, len(fake.snapshots), "disk pressure must trigger CreateSnapshot")
	require.Equal(t, 1, len(fake.compacts), "and Compact")
	require.Equal(t, uint64(92), r.LastCompactedIndexForTest(), "compact point = applied(100) - safetyBuffer(8)")
}

// TestMaybeCompact_PressureClearedIsNoOp confirms the hint is a toggle: once
// the watcher clears pressure (disk recovered) and the size/age limbs are
// disabled, maybeCompact is a no-op again.
func TestMaybeCompact_PressureClearedIsNoOp(t *testing.T) {
	fake := newCompactionStorage(100, 100, 1<<40)
	r := db.NewRaftForCompactionTest(fake, 0, 0, zap.NewNop())

	r.SetCompactionPressureForTest(true)
	r.SetCompactionPressureForTest(false)
	r.MaybeCompactForTest()

	require.Equal(t, 0, len(fake.compacts), "no pressure and no limbs must not compact")
}

// compactionStorage is the raft-level storage double these tests drive:
// the three values maybeCompact reads (AppliedIndex, EventIndex,
// RaftLogApproxSize) scripted, and the two calls it makes (CreateSnapshot,
// Compact) recorded by index. It satisfies only what the Raft holds — the
// consensus + membership roles — not the whole Storage union: etcd's
// MemoryStorage supplies the raft.Storage reads, StorageStubs the
// membership no-ops, and the remaining consensus writes are no-ops here
// because a bare Raft (no Ready loop) never issues them.
type compactionStorage struct {
	*raft.MemoryStorage
	dbtesting.StorageStubs
	applied, eventIdx, size uint64
	snapshots               []uint64 // CreateSnapshot indexes, in call order
	compacts                []uint64 // Compact indexes, in call order
}

func newCompactionStorage(applied, eventIdx, size uint64) *compactionStorage {
	return &compactionStorage{MemoryStorage: raft.NewMemoryStorage(), applied: applied, eventIdx: eventIdx, size: size}
}

func (s *compactionStorage) AppliedIndex() uint64                      { return s.applied }
func (s *compactionStorage) EventIndex() uint64                        { return s.eventIdx }
func (s *compactionStorage) RaftLogApproxSize() (uint64, error)        { return s.size, nil }
func (s *compactionStorage) ConfState(*raftpb.ConfState)               {}
func (s *compactionStorage) RestoreSnapshot(*raftpb.Snapshot) error    { return nil }
func (s *compactionStorage) ApplyCommitted(*raftpb.Entry) error        { return nil }
func (s *compactionStorage) ApplyCommittedBatch([]*raftpb.Entry) error { return nil }

func (s *compactionStorage) Save(*raftpb.HardState, []*raftpb.Entry, *raftpb.Snapshot) error {
	return nil
}

func (s *compactionStorage) CreateSnapshot(index uint64, _ *raftpb.ConfState) (*raftpb.Snapshot, error) {
	s.snapshots = append(s.snapshots, index)
	return &raftpb.Snapshot{}, nil
}

func (s *compactionStorage) Compact(index uint64) error {
	s.compacts = append(s.compacts, index)
	return nil
}
