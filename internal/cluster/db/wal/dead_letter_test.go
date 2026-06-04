package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/philborlin/committed/internal/cluster"
)

// applyDeadLetter commits one SyncableDeadLetter entity at raft entry
// index entryIdx. entryIdx (where the record is committed) is independent
// of dl.Index (the raft index of the proposal that was dead-lettered,
// which becomes the bucket key). The dead-letter type is a system type,
// so no prior type registration is needed.
func applyDeadLetter(t *testing.T, s *StorageWrapper, entryIdx uint64, dl *cluster.SyncableDeadLetter) {
	t.Helper()
	ent, err := cluster.NewUpsertSyncableDeadLetterEntity(dl)
	require.NoError(t, err)
	p := &cluster.Proposal{Entities: []*cluster.Entity{ent}}
	bs, err := p.Marshal()
	require.NoError(t, err)
	entry := pb.Entry{Term: 1, Index: entryIdx, Type: pb.EntryNormal, Data: bs}
	require.NoError(t, s.Save(defaultHardState, []pb.Entry{entry}, defaultSnap))
	require.NoError(t, s.ApplyCommitted(entry))
}

// TestSyncableDeadLetters_StoreOrderingAndCursor covers the query surface
// the HTTP endpoint relies on: records come back in ascending raft-index
// order, `since` is an exclusive cursor, and `limit` bounds the page.
func TestSyncableDeadLetters_StoreOrderingAndCursor(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	const id = "orders-sync"
	applyDeadLetter(t, s, 1, &cluster.SyncableDeadLetter{ID: id, Index: 30, TimestampUnixNano: 300, Kind: "permanent", Message: "c"})
	applyDeadLetter(t, s, 2, &cluster.SyncableDeadLetter{ID: id, Index: 10, TimestampUnixNano: 100, Kind: "permanent", Message: "a"})
	applyDeadLetter(t, s, 3, &cluster.SyncableDeadLetter{ID: id, Index: 20, TimestampUnixNano: 200, Kind: "permanent", Message: "b"})

	// Ascending by failed-proposal index, regardless of apply order.
	all, err := s.SyncableDeadLetters(id, 0, 100)
	require.NoError(t, err)
	require.Equal(t, []uint64{10, 20, 30}, indexesOf(all))
	require.Equal(t, "a", all[0].Message)
	require.Equal(t, "permanent", all[0].Kind)

	// `since` is exclusive: since=10 drops the index-10 record.
	page, err := s.SyncableDeadLetters(id, 10, 100)
	require.NoError(t, err)
	require.Equal(t, []uint64{20, 30}, indexesOf(page))

	// since at the highest index returns nothing.
	page, err = s.SyncableDeadLetters(id, 30, 100)
	require.NoError(t, err)
	require.Empty(t, page)

	// limit bounds the page; the last index is the cursor for the next.
	page, err = s.SyncableDeadLetters(id, 0, 2)
	require.NoError(t, err)
	require.Equal(t, []uint64{10, 20}, indexesOf(page))
	next, err := s.SyncableDeadLetters(id, page[len(page)-1].Index, 2)
	require.NoError(t, err)
	require.Equal(t, []uint64{30}, indexesOf(next))
}

// TestSyncableDeadLetters_PerSyncableAndUnknown proves the keyspace is
// partitioned by syncable id and an unknown id is an empty result, not an
// error.
func TestSyncableDeadLetters_PerSyncableAndUnknown(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	applyDeadLetter(t, s, 1, &cluster.SyncableDeadLetter{ID: "a", Index: 5, Kind: "permanent", Message: "x"})
	applyDeadLetter(t, s, 2, &cluster.SyncableDeadLetter{ID: "b", Index: 7, Kind: "permanent", Message: "y"})

	a, err := s.SyncableDeadLetters("a", 0, 100)
	require.NoError(t, err)
	require.Equal(t, []uint64{5}, indexesOf(a))

	b, err := s.SyncableDeadLetters("b", 0, 100)
	require.NoError(t, err)
	require.Equal(t, []uint64{7}, indexesOf(b))

	none, err := s.SyncableDeadLetters("never-failed", 0, 100)
	require.NoError(t, err)
	require.Empty(t, none)
}

// TestSyncableDeadLetters_IdempotentAndDeterministic proves the record is
// a deterministic function of the committed entities: re-applying the
// same failed-proposal index overwrites in place (a crash-replay re-emit
// doesn't duplicate the row), and two replicas fed the same records in
// different order converge to identical state.
func TestSyncableDeadLetters_IdempotentAndDeterministic(t *testing.T) {
	const id = "sync"

	s1 := NewStorage(t, nil)
	defer s1.Cleanup()
	applyDeadLetter(t, s1, 1, &cluster.SyncableDeadLetter{ID: id, Index: 10, TimestampUnixNano: 100, Kind: "permanent", Message: "a"})
	applyDeadLetter(t, s1, 2, &cluster.SyncableDeadLetter{ID: id, Index: 20, TimestampUnixNano: 200, Kind: "permanent", Message: "b"})
	// Re-apply index 10 (replay) — must overwrite, not duplicate.
	applyDeadLetter(t, s1, 3, &cluster.SyncableDeadLetter{ID: id, Index: 10, TimestampUnixNano: 100, Kind: "permanent", Message: "a"})

	got, err := s1.SyncableDeadLetters(id, 0, 100)
	require.NoError(t, err)
	require.Equal(t, []uint64{10, 20}, indexesOf(got), "re-applying the same index must not duplicate the record")

	// A second replica applies the same records in the opposite order.
	s2 := NewStorage(t, nil)
	defer s2.Cleanup()
	applyDeadLetter(t, s2, 1, &cluster.SyncableDeadLetter{ID: id, Index: 20, TimestampUnixNano: 200, Kind: "permanent", Message: "b"})
	applyDeadLetter(t, s2, 2, &cluster.SyncableDeadLetter{ID: id, Index: 10, TimestampUnixNano: 100, Kind: "permanent", Message: "a"})

	got2, err := s2.SyncableDeadLetters(id, 0, 100)
	require.NoError(t, err)
	require.Equal(t, got, got2, "replicas applying the same dead-letter records must converge")
}

// TestSyncableDeadLetters_SurvivesReopen is the durability guarantee:
// records persist across a process restart so an operator can still query
// what was skipped after a node bounce.
func TestSyncableDeadLetters_SurvivesReopen(t *testing.T) {
	const id = "sync"
	s := NewStorage(t, nil)
	defer s.Cleanup()
	applyDeadLetter(t, s, 1, &cluster.SyncableDeadLetter{ID: id, Index: 42, TimestampUnixNano: 1, Kind: "permanent", Message: "boom"})

	reopened, err := s.CloseAndReopen()
	require.NoError(t, err)
	defer reopened.Cleanup()

	got, err := reopened.SyncableDeadLetters(id, 0, 100)
	require.NoError(t, err)
	require.Equal(t, []uint64{42}, indexesOf(got))
	require.Equal(t, "boom", got[0].Message)
}

func indexesOf(dls []cluster.SyncableDeadLetter) []uint64 {
	out := make([]uint64, len(dls))
	for i, d := range dls {
		out[i] = d.Index
	}
	return out
}
