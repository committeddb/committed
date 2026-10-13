package wal_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// declaredType builds a user type registration with an onSupersede declaration.
func declaredType(t *testing.T, id string, kind cluster.EntityKind, on cluster.OnSupersede) *cluster.Entity {
	t.Helper()
	e, err := cluster.NewUpsertTypeEntity(&cluster.Type{
		ID: id, Name: id, Version: 1, EntityKind: kind, OnSupersede: on,
	})
	require.Nil(t, err)
	return e
}

// TestScrub_OnSupersedeScrubKeepsLatest: an event type declaring scrub keeps
// only each key's latest entry at or below the bound, like a snapshot type;
// keys written once and the tail are untouched.
func TestScrub_OnSupersedeScrubKeepsLatest(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	saveEntity(t, declaredType(t, "u", cluster.EntityKindEvent, cluster.OnSupersedeScrub), s, 1, 1)
	saveEntity(t, userUpsert("u", "1", `{"customer":"ada"}`), s, 1, 2)
	saveEntity(t, userUpsert("u", "2", `{"qty":2}`), s, 1, 3)
	saveEntity(t, userUpsert("u", "1", `{"customer":"***"}`), s, 1, 4) // masks event 1
	saveEntity(t, userUpsert("u", "3", `{"qty":1}`), s, 1, 5)
	saveEntity(t, userUpsert("u", "2", `{"qty":5}`), s, 1, 6) // tail (index > bound)

	require.Nil(t, s.RunScrubForTest(5))

	got, err := s.EventIndices()
	require.Nil(t, err)
	require.Equal(t, []uint64{1, 3, 4, 5, 6}, got, "only the unmasked original (2) is removed")
}

// TestScrub_OnSupersedeKeepRetains: the same writes under keep (and under an
// undeclared event type) survive whole.
func TestScrub_OnSupersedeKeepRetains(t *testing.T) {
	for _, on := range []cluster.OnSupersede{cluster.OnSupersedeKeep, cluster.OnSupersedeUnspecified} {
		t.Run(on.String(), func(t *testing.T) {
			s := NewStorage(t, nil)
			defer s.Cleanup()

			saveEntity(t, declaredType(t, "u", cluster.EntityKindEvent, on), s, 1, 1)
			saveEntity(t, userUpsert("u", "1", `{"customer":"ada"}`), s, 1, 2)
			saveEntity(t, userUpsert("u", "1", `{"customer":"***"}`), s, 1, 3)

			require.Nil(t, s.RunScrubForTest(3))

			got, err := s.EventIndices()
			require.Nil(t, err)
			require.Equal(t, []uint64{1, 2, 3}, got)
		})
	}
}

// TestScrub_OnSupersedeSwitchToScrubIsRetroactive: a type switched from keep to
// scrub has every superseded version at or below the bound removed, including
// ones written (original AND correction) before the switch.
func TestScrub_OnSupersedeSwitchToScrubIsRetroactive(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	saveEntity(t, declaredType(t, "u", cluster.EntityKindEvent, cluster.OnSupersedeKeep), s, 1, 1)
	saveEntity(t, userUpsert("u", "1", `{"customer":"ada"}`), s, 1, 2)
	saveEntity(t, userUpsert("u", "1", `{"customer":"***"}`), s, 1, 3)
	saveEntity(t, declaredType(t, "u", cluster.EntityKindEvent, cluster.OnSupersedeScrub), s, 1, 4)
	saveEntity(t, userUpsert("u", "2", `{"qty":1}`), s, 1, 5)

	require.Nil(t, s.RunScrubForTest(5))

	got, err := s.EventIndices()
	require.Nil(t, err)
	require.Equal(t, []uint64{1, 3, 4, 5}, got)
}

// TestScrub_OnSupersedeResolvedAtBound: the setting that counts is the one in
// force at the scrub bound. A switch to scrub after the bound does not apply to
// this scrub; a switch back to keep at or below the bound stops it.
func TestScrub_OnSupersedeResolvedAtBound(t *testing.T) {
	t.Run("scrub declared after the bound", func(t *testing.T) {
		s := NewStorage(t, nil)
		defer s.Cleanup()

		saveEntity(t, declaredType(t, "u", cluster.EntityKindEvent, cluster.OnSupersedeKeep), s, 1, 1)
		saveEntity(t, userUpsert("u", "1", `{"v":1}`), s, 1, 2)
		saveEntity(t, userUpsert("u", "1", `{"v":2}`), s, 1, 3)
		saveEntity(t, declaredType(t, "u", cluster.EntityKindEvent, cluster.OnSupersedeScrub), s, 1, 4)

		require.Nil(t, s.RunScrubForTest(3))

		got, err := s.EventIndices()
		require.Nil(t, err)
		require.Equal(t, []uint64{1, 2, 3, 4}, got)
	})
	t.Run("switched back to keep at or below the bound", func(t *testing.T) {
		s := NewStorage(t, nil)
		defer s.Cleanup()

		saveEntity(t, declaredType(t, "u", cluster.EntityKindEvent, cluster.OnSupersedeScrub), s, 1, 1)
		saveEntity(t, userUpsert("u", "1", `{"v":1}`), s, 1, 2)
		saveEntity(t, userUpsert("u", "1", `{"v":2}`), s, 1, 3)
		saveEntity(t, declaredType(t, "u", cluster.EntityKindEvent, cluster.OnSupersedeKeep), s, 1, 4)

		require.Nil(t, s.RunScrubForTest(4))

		got, err := s.EventIndices()
		require.Nil(t, err)
		require.Equal(t, []uint64{1, 2, 3, 4}, got)
	})
}

// TestScrub_OnSupersedeManyDistinctKeys: on a scrub-declared type whose keys are
// mostly written once (an events table keyed by event id), only the keys written
// more than once lose entries. The filter's growth is covered directly by
// TestSeenFilter.
func TestScrub_OnSupersedeManyDistinctKeys(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	saveEntity(t, declaredType(t, "u", cluster.EntityKindEvent, cluster.OnSupersedeScrub), s, 1, 1)
	idx := uint64(1)
	var want []uint64
	want = append(want, 1)
	const n = 500
	for i := range n {
		idx++
		saveEntity(t, userUpsert("u", fmt.Sprint(i), `{"v":1}`), s, 1, idx)
		if i%50 != 0 {
			want = append(want, idx)
		}
	}
	for i := 0; i < n; i += 50 {
		idx++
		saveEntity(t, userUpsert("u", fmt.Sprint(i), `{"v":2}`), s, 1, idx)
		want = append(want, idx)
	}

	require.Nil(t, s.RunScrubForTest(idx))

	got, err := s.EventIndices()
	require.Nil(t, err)
	require.Equal(t, want, got)
}

// TestScrubBacklog_OnSupersedeCeiling: a write to a scrub-declared event type
// does not count toward the metadata volume gate; it starts the supersede
// ceiling, and the scheduler reports backlog once the ceiling has elapsed since
// the last scrub was proposed.
func TestScrubBacklog_OnSupersedeCeiling(t *testing.T) {
	restoreThreshold := wal.SetMetadataBacklogThresholdForTest(1)
	defer restoreThreshold()
	restoreCeiling := wal.SetSupersedeScrubCeilingForTest(time.Hour)
	defer restoreCeiling()

	s := NewStorage(t, nil)
	defer s.Cleanup()

	saveEntity(t, declaredType(t, "u", cluster.EntityKindEvent, cluster.OnSupersedeScrub), s, 1, 1)
	require.True(t, s.HasScrubBacklog(), "no scrub has ever run: the ceiling has elapsed")
	applyScrub(t, s, 1, 2, false)
	require.False(t, s.HasScrubBacklog())

	saveEntity(t, userUpsert("u", "1", `{"v":1}`), s, 1, 3)
	require.False(t, s.HasScrubBacklog(), "a scrub ran within the ceiling, and the write is not metadata")

	restoreShort := wal.SetSupersedeScrubCeilingForTest(0)
	defer restoreShort()
	require.True(t, s.HasScrubBacklog(), "the ceiling elapsed with a write pending")

	applyScrub(t, s, 3, 4, false)
	require.False(t, s.HasScrubBacklog(), "nothing written since the scrub")
}

// TestScrubBacklog_OnSupersedeKeepDoesNotTrigger: writes to a keep (or
// undeclared) event type never start the ceiling.
func TestScrubBacklog_OnSupersedeKeepDoesNotTrigger(t *testing.T) {
	restoreCeiling := wal.SetSupersedeScrubCeilingForTest(0)
	defer restoreCeiling()

	s := NewStorage(t, nil)
	defer s.Cleanup()

	saveEntity(t, declaredType(t, "u", cluster.EntityKindEvent, cluster.OnSupersedeKeep), s, 1, 1)
	saveEntity(t, userUpsert("u", "1", `{"v":1}`), s, 1, 2)
	saveEntity(t, userUpsert("u", "1", `{"v":2}`), s, 1, 3)
	require.False(t, s.HasScrubBacklog())
}

// TestScrubBacklog_OnSupersedeSurvivesRestart: the in-memory record of pending
// corrections does not survive a restart, so Open re-seeds it from the live
// types, and the scrub time is persisted, so the ceiling still runs from the
// last scrub rather than from the restart.
func TestScrubBacklog_OnSupersedeSurvivesRestart(t *testing.T) {
	restoreCeiling := wal.SetSupersedeScrubCeilingForTest(time.Hour)
	defer restoreCeiling()

	s := NewStorage(t, nil)
	saveEntity(t, declaredType(t, "u", cluster.EntityKindEvent, cluster.OnSupersedeScrub), s, 1, 1)
	applyScrub(t, s, 1, 2, false)
	saveEntity(t, userUpsert("u", "1", `{"v":1}`), s, 1, 3)

	reopened, err := s.CloseAndReopen()
	require.NoError(t, err)
	defer reopened.Cleanup()
	require.False(t, reopened.HasScrubBacklog(), "the last scrub ran within the ceiling")

	restoreShort := wal.SetSupersedeScrubCeilingForTest(0)
	defer restoreShort()
	require.True(t, reopened.HasScrubBacklog(), "the pending write is re-seeded after the restart")
}

// TestScrubDeterminism_ScrubCommandTime: one stamped Scrub command applied on
// three nodes, completed at different wall-clock moments, leaves byte-identical
// bbolt. The ceiling's start comes from the command, not from each node's clock.
func TestScrubDeterminism_ScrubCommandTime(t *testing.T) {
	const nodes = 3
	scrub, err := cluster.NewScrubEntity(2, false)
	require.NoError(t, err)
	buckets := make([][]string, nodes)
	for i := range nodes {
		s := NewStorage(t, nil)
		defer s.Cleanup()
		saveEntity(t, declaredType(t, "u", cluster.EntityKindEvent, cluster.OnSupersedeScrub), s, 1, 1)
		saveEntity(t, userUpsert("u", "1", `{"v":1}`), s, 1, 2)
		time.Sleep(1100 * time.Millisecond) // a second apart, as nodes finish in practice
		saveEntity(t, scrub, s, 1, 3)
		require.Eventually(t, func() bool { return s.ScrubCompletedBound() >= 2 },
			10*time.Second, 5*time.Millisecond)
		buckets[i], err = s.BucketSnapshot()
		require.NoError(t, err)
	}
	for i := 1; i < nodes; i++ {
		require.Equalf(t, buckets[0], buckets[i], "bbolt differs: node %d vs 0", i)
	}
}
