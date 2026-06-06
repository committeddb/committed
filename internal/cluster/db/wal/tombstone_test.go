package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// TestTombstone_RecordedOnUserDelete verifies a user-defined (topic) delete
// records a tombstone keyed by (type, key) and stamped with the delete's raft
// index, surfaced via tombstoneSelections.
func TestTombstone_RecordedOnUserDelete(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	s.RegisterType(t, "user-events", 1, 1)
	// Upsert then delete the same key. Upsert at 2, delete at 3.
	up := &cluster.Entity{Type: &cluster.Type{ID: "user-events"}, Key: []byte("alice"), Data: []byte(`{"a":1}`)}
	saveEntity(t, up, s, 1, 2)
	del := cluster.NewDeleteEntity(&cluster.Type{ID: "user-events"}, []byte("alice"))
	saveEntity(t, del, s, 1, 3)

	sel, err := s.TombstoneSelections(100)
	require.Nil(t, err)
	require.Equal(t, map[string]uint64{
		wal.TombstoneKey("user-events", []byte("alice")): 3,
	}, sel)
}

// TestTombstone_NotRecordedForConfigDelete verifies built-in config deletes
// (here a Type delete) do NOT create event-log tombstones — only user data is
// scrubbed.
func TestTombstone_NotRecordedForConfigDelete(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	s.RegisterType(t, "doomed", 1, 1)
	del := cluster.NewDeleteTypeEntity("doomed")
	saveEntity(t, del, s, 1, 2)

	sel, err := s.TombstoneSelections(100)
	require.Nil(t, err)
	require.Empty(t, sel)
}

// TestTombstone_MultipleDeletesAppendSorted verifies repeated deletes for the
// same (type, key) accumulate as an ascending list, and that tombstoneSelections
// returns the maximum delete index <= bound — the determinism-safe predicate
// that lets the scrubber preserve data re-created after an earlier delete.
func TestTombstone_MultipleDeletesAppendSorted(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	s.RegisterType(t, "user-events", 1, 1)
	key := []byte("bob")
	// delete @2, re-create @3, delete again @4.
	saveEntity(t, cluster.NewDeleteEntity(&cluster.Type{ID: "user-events"}, key), s, 1, 2)
	saveEntity(t, &cluster.Entity{Type: &cluster.Type{ID: "user-events"}, Key: key, Data: []byte(`{"b":1}`)}, s, 1, 3)
	saveEntity(t, cluster.NewDeleteEntity(&cluster.Type{ID: "user-events"}, key), s, 1, 4)

	tk := wal.TombstoneKey("user-events", key)

	// bound below the first delete: nothing selected.
	sel, err := s.TombstoneSelections(1)
	require.Nil(t, err)
	require.Empty(t, sel)

	// bound between the two deletes: max delete <= bound is the first (2).
	sel, err = s.TombstoneSelections(3)
	require.Nil(t, err)
	require.Equal(t, uint64(2), sel[tk])

	// bound past both deletes: max is the second (4).
	sel, err = s.TombstoneSelections(100)
	require.Nil(t, err)
	require.Equal(t, uint64(4), sel[tk])
}

// TestTombstone_SurvivesRestart verifies tombstones persist across a
// close/reopen (they live in bbolt, which the rebuild rsync covers).
func TestTombstone_SurvivesRestart(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	s.RegisterType(t, "user-events", 1, 1)
	saveEntity(t, cluster.NewDeleteEntity(&cluster.Type{ID: "user-events"}, []byte("carol")), s, 1, 2)

	s = s.CloseAndReopenStorage(t)
	defer s.Cleanup()

	sel, err := s.TombstoneSelections(100)
	require.Nil(t, err)
	require.Equal(t, uint64(2), sel[wal.TombstoneKey("user-events", []byte("carol"))])
}
