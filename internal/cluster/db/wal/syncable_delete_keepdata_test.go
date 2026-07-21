package wal_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
)

// TestDeleteSyncable_KeepDataRidesTheEntity pins the determinism property of
// the entity-borne keepData intent: a node that applies a delete tombstone —
// with NO node-local state whatsoever (it never saw the DELETE request; think
// the new leader after a propose→apply leadership change) — still observes the
// operator's preserve-the-destination intent, because it rides the entity. The
// pre-fix node-local intent map made exactly this node drop the table.
func TestDeleteSyncable_KeepDataRidesTheEntity(t *testing.T) {
	for _, keep := range []bool{true, false} {
		dir, err := os.MkdirTemp("", "wal-keepdata-test-")
		require.NoError(t, err)
		syncCh := make(chan *db.SyncableWithID, 4)
		s := OpenStorage(t, dir, parser.New(), syncCh, nil)
		defer s.Cleanup()

		require.NoError(t, s.SeedSyncableConfigForTest("A"))

		// Apply ONLY the committed tombstones — the pure catch-up/apply path.
		ents := cluster.NewDeleteSyncableEntities("A", keep)
		saveEntity(t, ents[0], s, 1, 1)

		msg := <-syncCh
		require.True(t, msg.Delete)
		require.Equal(t, "A", msg.ID)
		require.Equal(t, keep, msg.KeepData,
			"keepData must reach the teardown decision from the ENTITY, not node-local state (keep=%v)", keep)
	}
}
