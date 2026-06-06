package wal_test

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db/wal"
)

func userUpsert(id string, key, data string) *cluster.Entity {
	return &cluster.Entity{Type: &cluster.Type{ID: id}, Key: []byte(key), Data: []byte(data)}
}

func userDelete(id string, key string) *cluster.Entity {
	return cluster.NewDeleteEntity(&cluster.Type{ID: id}, []byte(key))
}

// TestScrub_RemovesPIIKeepsTombstoneAndSiblings is the core RTBF test: scrubbing
// physically removes the deleted subject's PII originals, retains the delete-
// tombstone, leaves unrelated entries, and preserves EventIndex (the tail).
func TestScrub_RemovesPIIKeepsTombstoneAndSiblings(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	s.RegisterType(t, "u", 1, 1) // idx 1: type
	saveEntity(t, userUpsert("u", "alice", `{"pii":true}`), s, 1, 2)
	saveEntity(t, userUpsert("u", "bob", `{"ok":1}`), s, 1, 3)
	saveEntity(t, userDelete("u", "alice"), s, 1, 4) // tombstone alice@4
	saveEntity(t, userUpsert("u", "carol", `{"ok":2}`), s, 1, 5)

	require.Equal(t, uint64(5), s.EventIndex())

	require.Nil(t, s.RunScrubForTest(4))

	// alice's PII original (idx 2) is physically gone.
	_, err := s.ActualAt(2)
	require.ErrorIs(t, err, wal.ErrActualNotFound)

	// Unrelated entries survive.
	bob, err := s.ActualAt(3)
	require.Nil(t, err)
	require.Equal(t, []byte("bob"), bob.Entities[0].Key)
	carol, err := s.ActualAt(5)
	require.Nil(t, err)
	require.Equal(t, []byte("carol"), carol.Entities[0].Key)

	// The delete-tombstone (idx 4) is retained.
	del, err := s.ActualAt(4)
	require.Nil(t, err)
	require.True(t, del.Entities[0].IsDelete())
	require.Equal(t, []byte("alice"), del.Entities[0].Key)

	// EventIndex (P_local) never regresses; firstEventIndex still points at the
	// retained type entry (idx 1, not removed).
	require.Equal(t, uint64(5), s.EventIndex(), "EventIndex must not regress")
	require.Equal(t, uint64(1), s.FirstEventIndex())
}

// TestScrub_EntityGranular verifies a multi-entity proposal keeps its
// untombstoned siblings: only the tombstoned entity is erased, the record is
// re-marshaled in place.
func TestScrub_EntityGranular(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	s.RegisterType(t, "u", 1, 1)
	// idx 2: one proposal, two entities — alice (PII) + dave (kept).
	saveProposal(t, &cluster.Proposal{Entities: []*cluster.Entity{
		userUpsert("u", "alice", `{"pii":true}`),
		userUpsert("u", "dave", `{"ok":1}`),
	}}, s, 1, 2)
	saveEntity(t, userDelete("u", "alice"), s, 1, 3) // tombstone alice@3
	saveEntity(t, userUpsert("u", "tail", `{"ok":2}`), s, 1, 4)

	require.Nil(t, s.RunScrubForTest(3))

	// idx 2 survives but now holds only dave.
	a, err := s.ActualAt(2)
	require.Nil(t, err)
	require.Len(t, a.Entities, 1)
	require.Equal(t, []byte("dave"), a.Entities[0].Key)
	require.Equal(t, []byte(`{"ok":1}`), a.Entities[0].Data)

	require.Equal(t, uint64(4), s.EventIndex())
}

// TestScrub_GappedReader verifies a Reader bootstrapping after a scrub returns
// exactly the survivors in ascending raft-index order and skips removed indices.
func TestScrub_GappedReader(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	s.RegisterType(t, "u", 1, 1)
	saveEntity(t, userUpsert("u", "alice", `{"pii":true}`), s, 1, 2)
	saveEntity(t, userUpsert("u", "bob", `{"ok":1}`), s, 1, 3)
	saveEntity(t, userDelete("u", "alice"), s, 1, 4)
	saveEntity(t, userUpsert("u", "carol", `{"ok":2}`), s, 1, 5)

	require.Nil(t, s.RunScrubForTest(4))

	r := s.Reader("fresh")
	var idxs []uint64
	for {
		a, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		require.Nil(t, err)
		idxs = append(idxs, a.Index)
	}
	// idx 2 (alice PII) is gone; 1 (type), 3 (bob), 4 (alice delete), 5 (carol)
	// remain, in ascending order across the raftIndex gap.
	require.Equal(t, []uint64{1, 3, 4, 5}, idxs)
}

// TestScrub_SurvivesRestart verifies the rewritten log persists and that
// EventIndex/AppliedIndex remain consistent (P_local == R_local) after reopen —
// the storage invariant the Ready loop checks every iteration.
func TestScrub_SurvivesRestart(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	s.RegisterType(t, "u", 1, 1)
	saveEntity(t, userUpsert("u", "alice", `{"pii":true}`), s, 1, 2)
	saveEntity(t, userDelete("u", "alice"), s, 1, 3)
	saveEntity(t, userUpsert("u", "tail", `{"ok":1}`), s, 1, 4)
	require.Nil(t, s.RunScrubForTest(3))

	s = s.CloseAndReopenStorage(t)
	defer s.Cleanup()

	require.Equal(t, uint64(4), s.EventIndex())
	require.Equal(t, s.EventIndex(), s.AppliedIndex(), "P_local == R_local after reopen")
	_, err := s.ActualAt(2)
	require.ErrorIs(t, err, wal.ErrActualNotFound)
	tail, err := s.ActualAt(4)
	require.Nil(t, err)
	require.Equal(t, []byte("tail"), tail.Entities[0].Key)
}

// TestScrubDeterminism applies the identical history + scrub to three fresh
// nodes and requires byte-identical event logs and equal bounds — the
// rsync-rebuild / cross-node-hash contract for the scrubber.
func TestScrubDeterminism(t *testing.T) {
	const nodes = 3
	snaps := make([][]string, nodes)
	eventIdx := make([]uint64, nodes)
	firstIdx := make([]uint64, nodes)
	buckets := make([][]string, nodes)

	for i := 0; i < nodes; i++ {
		s := NewStorage(t, nil)
		defer s.Cleanup()

		s.RegisterType(t, "u", 1, 1)
		// A mix: single + multi-entity proposals, an unrelated key, two deletes.
		saveProposal(t, &cluster.Proposal{Entities: []*cluster.Entity{
			userUpsert("u", "alice", `{"pii":1}`),
			userUpsert("u", "dave", `{"ok":1}`),
		}}, s, 1, 2)
		saveEntity(t, userUpsert("u", "alice", `{"pii":2}`), s, 1, 3)
		saveEntity(t, userUpsert("u", "bob", `{"ok":2}`), s, 1, 4)
		saveEntity(t, userDelete("u", "alice"), s, 1, 5)
		saveEntity(t, userDelete("u", "bob"), s, 1, 6)
		saveEntity(t, userUpsert("u", "carol", `{"ok":3}`), s, 1, 7)

		require.Nil(t, s.RunScrubForTest(6))

		var err error
		snaps[i], err = s.EventLogSnapshot()
		require.Nil(t, err)
		eventIdx[i] = s.EventIndex()
		firstIdx[i] = s.FirstEventIndex()
		buckets[i], err = s.BucketSnapshot()
		require.Nil(t, err)
	}

	for i := 1; i < nodes; i++ {
		require.Equalf(t, snaps[0], snaps[i], "event log differs: node %d vs 0", i)
		require.Equalf(t, eventIdx[0], eventIdx[i], "EventIndex differs: node %d vs 0", i)
		require.Equalf(t, firstIdx[0], firstIdx[i], "FirstEventIndex differs: node %d vs 0", i)
		require.Equalf(t, buckets[0], buckets[i], "bbolt differs: node %d vs 0", i)
	}
	require.Equal(t, uint64(7), eventIdx[0], "tail (EventIndex) preserved")
}

// TestScrub_ReaderMidStreamNotDesynced guards the cursor-invalidation bug: a
// scrub that removes an entry BEHIND an active Reader's position re-densifies
// the wal seqs, so a naively-cached walSeq would skip the next entry. The
// Reader must re-derive its cursor from its (stable) raft index and still
// return every surviving entry after its position — in particular the delete-
// tombstone must not be skipped.
func TestScrub_ReaderMidStreamNotDesynced(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	s.RegisterType(t, "u", 1, 1)
	saveEntity(t, userUpsert("u", "alice", `{"pii":true}`), s, 1, 2)
	saveEntity(t, userUpsert("u", "bob", `{"ok":1}`), s, 1, 3)
	saveEntity(t, userDelete("u", "alice"), s, 1, 4)
	saveEntity(t, userUpsert("u", "carol", `{"ok":2}`), s, 1, 5)

	r := s.Reader("midstream")
	// Read up to and including bob (raft index 3): type(1), alice(2), bob(3).
	for _, want := range []uint64{1, 2, 3} {
		a, err := r.Read()
		require.Nil(t, err)
		require.Equal(t, want, a.Index)
	}

	// Scrub removes alice's original (idx 2), which sits BEHIND the cursor.
	require.Nil(t, s.RunScrubForTest(4))

	// The next reads must be the alice delete (4) then carol (5) — the delete
	// tombstone must NOT be skipped despite the seq re-densification.
	for _, want := range []uint64{4, 5} {
		a, err := r.Read()
		require.Nil(t, err)
		require.Equal(t, want, a.Index)
	}
	_, err := r.Read()
	require.ErrorIs(t, err, io.EOF)
}

// TestScrub_AsyncViaCommand exercises the full path: a committed Scrub command
// records a pending bound, the background worker runs the rewrite, and the
// result matches a direct scrub. Polls the completed bound rather than sleeping.
func TestScrub_AsyncViaCommand(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	s.RegisterType(t, "u", 1, 1)
	saveEntity(t, userUpsert("u", "alice", `{"pii":true}`), s, 1, 2)
	saveEntity(t, userDelete("u", "alice"), s, 1, 3)
	saveEntity(t, userUpsert("u", "tail", `{"ok":1}`), s, 1, 4)

	// Apply a real Scrub command (bound 3) at idx 5 → handleScrub records the
	// pending bound and signals the worker.
	scrubEnt, err := cluster.NewScrubEntity(3)
	require.Nil(t, err)
	saveEntity(t, scrubEnt, s, 1, 5)

	require.Eventually(t, func() bool {
		return s.ScrubCompletedBound() >= 3
	}, 5*time.Second, 5*time.Millisecond, "background scrub did not complete")

	_, err = s.ActualAt(2)
	require.ErrorIs(t, err, wal.ErrActualNotFound)
	require.Equal(t, uint64(5), s.EventIndex(), "scrub command entry is the tail; EventIndex preserved")
}
