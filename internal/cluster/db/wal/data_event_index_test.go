package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestDataEventIndex covers the head used for per-syncable lag: it advances
// only on user-topic DATA entries, never on committed's internal entries
// (config/type writes AND coordination like syncable-index bumps), exactly
// mirroring the reader's IsInternal filter. That mirror is what makes
// lag == 0 ⇔ caught up. The value also survives a restart via the Open-time
// backward scan.
func TestDataEventIndex(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	require.Equal(t, uint64(0), s.DataEventIndex(), "fresh storage has no data head")

	// index 1: a type/config write is internal — the reader filters it out
	// of the syncable stream, so the head must NOT advance on it.
	s.RegisterType(t, "type-x", 1, 1)
	require.Equal(t, uint64(0), s.DataEventIndex(), "config/type write is internal; must not advance the data head")

	// index 2: a user data entity advances the head.
	typ, err := s.ResolveType(cluster.LatestTypeRef("type-x"))
	require.NoError(t, err)
	saveEntity(t, cluster.NewUpsertEntity(typ, []byte("k0"), []byte("v0")), s, 1, 2)
	require.Equal(t, uint64(2), s.DataEventIndex(), "user data entry advances the data head")

	// index 3: a SyncableIndex bump is internal coordination — the reader
	// skips it, so the head must NOT advance (else an idle syncable shows
	// phantom lag against its own trailing bumps).
	siEnt, err := cluster.NewUpsertSyncableIndexEntity(&cluster.SyncableIndex{ID: "sync-1", Index: 2})
	require.NoError(t, err)
	saveEntity(t, siEnt, s, 1, 3)
	require.Equal(t, uint64(2), s.DataEventIndex(), "syncable-index bump must not advance the data head")

	// Survives restart: the Open-time backward scan skips the trailing
	// internal bump (index 3) and recovers the head as the last data entry
	// (index 2) — robust even on an rsync-restored node that never replays.
	s2 := s.CloseAndReopenStorage(t)
	require.Equal(t, uint64(2), s2.DataEventIndex(), "data head survives restart via Open backscan")
}
