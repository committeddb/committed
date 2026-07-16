package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
)

// applyMigrationDeadLetter commits one TypeMigrationDeadLetter entity at
// raft entry index entryIdx. As with applyDeadLetter, entryIdx is
// independent of dl.Index (the raft index of the proposal that failed to
// migrate, which becomes the bucket key).
func applyMigrationDeadLetter(t *testing.T, s *StorageWrapper, entryIdx uint64, dl *cluster.TypeMigrationDeadLetter) {
	t.Helper()
	ent, err := cluster.NewUpsertTypeMigrationDeadLetterEntity(dl)
	require.NoError(t, err)
	applyMigrationDeadLetterEntity(t, s, entryIdx, ent)
}

func applyMigrationDeadLetterEntity(t *testing.T, s *StorageWrapper, entryIdx uint64, ent *cluster.Entity) {
	t.Helper()
	p := &cluster.Proposal{Entities: []*cluster.Entity{ent}}
	bs, err := p.Marshal()
	require.NoError(t, err)
	entry := &pb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(entryIdx), Type: pb.EntryNormal.Enum(), Data: bs}
	require.NoError(t, s.Save(&defaultHardState, []*pb.Entry{entry}, &defaultSnap))
	require.NoError(t, s.ApplyCommitted(entry))
}

// TestTypeMigrationDeadLetters_StoreOrderingAndCursor covers the query
// surface GET /type/{id}/migration-errors relies on: ascending raft-index
// order, exclusive `since` cursor, bounded page.
func TestTypeMigrationDeadLetters_StoreOrderingAndCursor(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	const id = "person"
	// A migration dead-letter now persists only while its type config exists (the
	// per-config-id write-guard), so register the type before recording failures.
	s.RegisterType(t, id, 1, 1)
	applyMigrationDeadLetter(t, s, 2, &cluster.TypeMigrationDeadLetter{TypeID: id, Index: 30, TimestampUnixNano: 300, FromVersion: 2, ToVersion: 3, Message: "c"})
	applyMigrationDeadLetter(t, s, 3, &cluster.TypeMigrationDeadLetter{TypeID: id, Index: 10, TimestampUnixNano: 100, FromVersion: 1, ToVersion: 2, Message: "a"})
	applyMigrationDeadLetter(t, s, 4, &cluster.TypeMigrationDeadLetter{TypeID: id, Index: 20, TimestampUnixNano: 200, FromVersion: 1, ToVersion: 2, Message: "b"})

	// Ascending by failed-proposal index, regardless of apply order.
	all, err := s.TypeMigrationDeadLetters(id, 0, 100)
	require.NoError(t, err)
	require.Equal(t, []uint64{10, 20, 30}, migrationIndexesOf(all))
	require.Equal(t, "a", all[0].Message)
	require.Equal(t, 1, all[0].FromVersion)
	require.Equal(t, 2, all[0].ToVersion)

	// `since` is exclusive; limit bounds the page.
	page, err := s.TypeMigrationDeadLetters(id, 10, 100)
	require.NoError(t, err)
	require.Equal(t, []uint64{20, 30}, migrationIndexesOf(page))

	page, err = s.TypeMigrationDeadLetters(id, 0, 2)
	require.NoError(t, err)
	require.Equal(t, []uint64{10, 20}, migrationIndexesOf(page))
	next, err := s.TypeMigrationDeadLetters(id, page[len(page)-1].Index, 2)
	require.NoError(t, err)
	require.Equal(t, []uint64{30}, migrationIndexesOf(next))
}

// TestTypeMigrationDeadLetters_PerTypeAndUnknown proves the keyspace is
// partitioned by type id and an unknown id is an empty result, not an error.
func TestTypeMigrationDeadLetters_PerTypeAndUnknown(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	s.RegisterType(t, "a", 1, 1)
	s.RegisterType(t, "b", 1, 2)
	applyMigrationDeadLetter(t, s, 3, &cluster.TypeMigrationDeadLetter{TypeID: "a", Index: 5, FromVersion: 1, ToVersion: 2, Message: "x"})
	applyMigrationDeadLetter(t, s, 4, &cluster.TypeMigrationDeadLetter{TypeID: "b", Index: 7, FromVersion: 3, ToVersion: 4, Message: "y"})

	a, err := s.TypeMigrationDeadLetters("a", 0, 100)
	require.NoError(t, err)
	require.Equal(t, []uint64{5}, migrationIndexesOf(a))

	none, err := s.TypeMigrationDeadLetters("never-failed", 0, 100)
	require.NoError(t, err)
	require.Empty(t, none)
}

// TestTypeMigrationDeadLetters_IdempotentOverwrite proves re-applying the
// same failed-proposal index (a second always-current syncable tripping
// over the same proposal, or a crash-replay re-emit) overwrites in place
// rather than duplicating the row.
func TestTypeMigrationDeadLetters_IdempotentOverwrite(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	const id = "person"
	s.RegisterType(t, id, 1, 1)
	applyMigrationDeadLetter(t, s, 2, &cluster.TypeMigrationDeadLetter{TypeID: id, Index: 10, TimestampUnixNano: 100, FromVersion: 1, ToVersion: 2, Message: "a"})
	applyMigrationDeadLetter(t, s, 3, &cluster.TypeMigrationDeadLetter{TypeID: id, Index: 10, TimestampUnixNano: 100, FromVersion: 1, ToVersion: 2, Message: "a"})

	got, err := s.TypeMigrationDeadLetters(id, 0, 100)
	require.NoError(t, err)
	require.Equal(t, []uint64{10}, migrationIndexesOf(got), "re-applying the same index must not duplicate the record")
}

// TestTypeMigrationDeadLetters_HasAndDelete covers the retry path's two
// storage touches: Has finds a recorded failure, and applying the delete
// entity (what a successful migration retry proposes) removes it on every
// replica.
func TestTypeMigrationDeadLetters_HasAndDelete(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	const id = "person"
	s.RegisterType(t, id, 1, 1)
	applyMigrationDeadLetter(t, s, 2, &cluster.TypeMigrationDeadLetter{TypeID: id, Index: 42, FromVersion: 1, ToVersion: 2, Message: "boom"})

	has, err := s.HasTypeMigrationDeadLetter(id, 42)
	require.NoError(t, err)
	require.True(t, has)

	has, err = s.HasTypeMigrationDeadLetter(id, 43)
	require.NoError(t, err)
	require.False(t, has)

	has, err = s.HasTypeMigrationDeadLetter("unknown-type", 42)
	require.NoError(t, err)
	require.False(t, has)

	applyMigrationDeadLetterEntity(t, s, 3, cluster.NewDeleteTypeMigrationDeadLetterEntity(id, 42))

	has, err = s.HasTypeMigrationDeadLetter(id, 42)
	require.NoError(t, err)
	require.False(t, has, "the delete entity must clear the record")

	got, err := s.TypeMigrationDeadLetters(id, 0, 100)
	require.NoError(t, err)
	require.Empty(t, got)
}

// TestTypeMigrationDeadLetters_SurvivesReopen is the durability guarantee:
// records persist across a process restart.
func TestTypeMigrationDeadLetters_SurvivesReopen(t *testing.T) {
	const id = "person"
	s := NewStorage(t, nil)
	defer s.Cleanup()
	s.RegisterType(t, id, 1, 1)
	applyMigrationDeadLetter(t, s, 2, &cluster.TypeMigrationDeadLetter{TypeID: id, Index: 42, TimestampUnixNano: 1, FromVersion: 1, ToVersion: 2, Message: "boom"})

	reopened, err := s.CloseAndReopen()
	require.NoError(t, err)
	defer reopened.Cleanup()

	got, err := reopened.TypeMigrationDeadLetters(id, 0, 100)
	require.NoError(t, err)
	require.Equal(t, []uint64{42}, migrationIndexesOf(got))
	require.Equal(t, "boom", got[0].Message)
}

// TestDeleteType_SweepsMigrationDeadLettersSoRecreateStartsClean is the half-A
// (sweep) proof for the type domain: deleting a type removes its migration
// dead-letter sub-bucket (kept outside the config sub-bucket and not a delete-bundle
// tombstone), so a same-id type recreate does not inherit it and silently skip those
// migration indices.
func TestDeleteType_SweepsMigrationDeadLettersSoRecreateStartsClean(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	const id = "person"
	s.RegisterType(t, id, 1, 1)
	applyMigrationDeadLetter(t, s, 2, &cluster.TypeMigrationDeadLetter{TypeID: id, Index: 5, FromVersion: 1, ToVersion: 2, Message: "boom"})

	has, err := s.HasTypeMigrationDeadLetter(id, 5)
	require.NoError(t, err)
	require.True(t, has, "dead-letter recorded while the type exists")

	saveEntity(t, cluster.NewDeleteTypeEntity(id), s, 1, 3)

	has, err = s.HasTypeMigrationDeadLetter(id, 5)
	require.NoError(t, err)
	require.False(t, has, "delete must sweep the migration dead-letter sub-bucket")

	// Recreate the same type id: migration must not inherit the old dead-letter.
	s.RegisterType(t, id, 1, 4)
	has, err = s.HasTypeMigrationDeadLetter(id, 5)
	require.NoError(t, err)
	require.False(t, has, "a same-id type recreate must start clean")

	// And durable across a restart.
	reopened := s.CloseAndReopenStorage(t)
	defer reopened.Cleanup()
	has, err = reopened.HasTypeMigrationDeadLetter(id, 5)
	require.NoError(t, err)
	require.False(t, has, "the swept migration dead-letter must not resurrect on reopen")
}

// TestSaveTypeMigrationDeadLetter_GuardReapsPostDeleteWrite is the half-B
// (write-guard) proof for the type domain: a migration dead-letter that commits
// after the type was deleted (a racing always-current syncable, or an old write
// replayed before a same-id recreate) must be reaped, not re-established as an
// orphan.
func TestSaveTypeMigrationDeadLetter_GuardReapsPostDeleteWrite(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	const id = "person"
	s.RegisterType(t, id, 1, 1)
	saveEntity(t, cluster.NewDeleteTypeEntity(id), s, 1, 2)

	applyMigrationDeadLetter(t, s, 3, &cluster.TypeMigrationDeadLetter{TypeID: id, Index: 9, FromVersion: 1, ToVersion: 2, Message: "late"})

	has, err := s.HasTypeMigrationDeadLetter(id, 9)
	require.NoError(t, err)
	require.False(t, has, "a migration dead-letter committing after the type delete must be reaped")
}

func migrationIndexesOf(dls []cluster.TypeMigrationDeadLetter) []uint64 {
	out := make([]uint64, len(dls))
	for i, d := range dls {
		out[i] = d.Index
	}
	return out
}
