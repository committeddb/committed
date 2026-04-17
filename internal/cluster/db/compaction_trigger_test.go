package db_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/philborlin/committed/internal/cluster/db"
	"github.com/philborlin/committed/internal/cluster/db/dbfakes"
)

// TestMaybeCompact_SizeLimb drives the size limb of the "10GB or 1hr"
// policy: when RaftLogApproxSize crosses the configured threshold
// maybeCompact fires exactly one CreateSnapshot + Compact, at a
// compact point ≤ EventIndex (the safety constraint).
func TestMaybeCompact_SizeLimb(t *testing.T) {
	fake := newFakeStorageWithIndexes(100, 100, 5000)
	r := db.NewRaftForCompactionTest(fake, 4096 /*maxSize*/, 0 /*maxAge disabled*/, zap.NewNop())

	r.MaybeCompactForTest()

	require.Equal(t, 1, fake.CreateSnapshotCallCount(), "size-limb trigger must call CreateSnapshot")
	require.Equal(t, 1, fake.CompactCallCount(), "and Compact")

	// Compact point = applied (100) - safetyBuffer (8) = 92 — leaves
	// a small window for follower AppendEntries catchup.
	require.Equal(t, uint64(92), fake.CompactArgsForCall(0))
	require.Equal(t, uint64(92), r.LastCompactedIndexForTest())
}

// TestMaybeCompact_AgeLimb drives the age limb: size is well below
// the threshold, but the last compaction happened long enough ago
// that the clock-driven trigger fires.
func TestMaybeCompact_AgeLimb(t *testing.T) {
	// Size well under the threshold so only the age limb can fire.
	fake := newFakeStorageWithIndexes(100, 100, 128)
	r := db.NewRaftForCompactionTest(fake, 10*1024*1024 /*maxSize big*/, 10*time.Millisecond /*maxAge*/, zap.NewNop())

	// Back-date lastCompactTime so the age threshold is already past.
	r.SetLastCompactTimeForTest(time.Now().Add(-time.Hour))

	r.MaybeCompactForTest()

	require.Equal(t, 1, fake.CreateSnapshotCallCount(), "age-limb trigger must call CreateSnapshot")
	require.Equal(t, 1, fake.CompactCallCount())
}

// TestMaybeCompact_BothDisabled verifies the policy-disabled shape:
// with both limbs at 0, maybeCompact is a no-op no matter how big
// the log is or how long since the last compaction.
func TestMaybeCompact_BothDisabled(t *testing.T) {
	fake := newFakeStorageWithIndexes(100, 100, 1<<40)
	r := db.NewRaftForCompactionTest(fake, 0, 0, zap.NewNop())
	r.SetLastCompactTimeForTest(time.Now().Add(-100 * time.Hour))

	r.MaybeCompactForTest()

	require.Equal(t, 0, fake.CreateSnapshotCallCount())
	require.Equal(t, 0, fake.CompactCallCount())
}

// TestMaybeCompact_CappedAtEventIndex is the safety constraint:
// compact point must never exceed EventIndex, because a raft log
// that forgot entries the event log doesn't have would be the exact
// shape that trips the storage invariant on a follower receiving
// InstallSnapshot.
func TestMaybeCompact_CappedAtEventIndex(t *testing.T) {
	// Applied = 200 but EventIndex = 50 (event log far behind —
	// should never happen under the Ready loop's invariant check,
	// but maybeCompact is defensive). Compact target should cap at
	// 50 instead of (200 - 8) = 192.
	fake := newFakeStorageWithIndexes(200 /*applied*/, 50 /*eventIdx*/, 1<<30 /*size>threshold*/)
	r := db.NewRaftForCompactionTest(fake, 4096, 0, zap.NewNop())

	r.MaybeCompactForTest()

	require.Equal(t, 1, fake.CompactCallCount())
	require.Equal(t, uint64(50), r.LastCompactedIndexForTest(), "compact point must be capped at EventIndex")
}

// TestMaybeCompact_BelowSafetyBufferIsNoOp verifies that a node that
// has barely applied anything doesn't compact — even if size
// somehow crossed the threshold (e.g., a noisy fresh cluster).
func TestMaybeCompact_BelowSafetyBufferIsNoOp(t *testing.T) {
	fake := newFakeStorageWithIndexes(5 /*applied small*/, 5, 1<<30)
	r := db.NewRaftForCompactionTest(fake, 4096, 0, zap.NewNop())

	r.MaybeCompactForTest()

	require.Equal(t, 0, fake.CompactCallCount(), "must not compact when appliedIndex ≤ safetyBuffer")
}

// newFakeStorageWithIndexes wires a FakeStorage with the three
// values maybeCompact reads: AppliedIndex, EventIndex, and
// RaftLogApproxSize. Everything else the fake returns is the zero
// value for its type, which is fine for these tests.
func newFakeStorageWithIndexes(applied, eventIdx, size uint64) *dbfakes.FakeStorage {
	f := &dbfakes.FakeStorage{}
	f.AppliedIndexReturns(applied)
	f.EventIndexReturns(eventIdx)
	f.RaftLogApproxSizeReturns(size, nil)
	f.CreateSnapshotReturns(raftpb.Snapshot{}, nil)
	f.CompactReturns(nil)
	return f
}
