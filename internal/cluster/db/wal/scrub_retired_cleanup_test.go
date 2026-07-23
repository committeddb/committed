package wal_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestScrub_RetiredDirRemovedAfterSwap guards the #4 code motion: the retired
// pre-scrub event log is reaped after a successful swap. The os.RemoveAll now
// runs in the deferred cleanup AFTER eventMu is released — so the O(files) unlink
// (which scales with log size) can't stall the raft Ready loop's appendEvents —
// but it must still run, or a leftover events.retired/ would persist until the
// next Open's recoverScrubDirs.
func TestScrub_RetiredDirRemovedAfterSwap(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	s.RegisterType(t, "user-events", 1, 1)
	key := []byte("alice")
	// Upsert @2 then delete @3 so a scrub at bound 3 removes the upsert and
	// performs a real event-log swap (events -> events.retired -> reaped).
	saveEntity(t, &cluster.Entity{Type: &cluster.Type{ID: "user-events"}, Key: key, Data: []byte(`{"a":1}`)}, s, 1, 2)
	saveEntity(t, cluster.NewDeleteEntity(&cluster.Type{ID: "user-events"}, key), s, 1, 3)

	require.NoError(t, s.RunScrubForTest(3))

	require.NoDirExists(t, filepath.Join(s.path, "events.retired"),
		"the retired pre-scrub log must be reaped after the swap (deferred cleanup, now outside eventMu)")
}
