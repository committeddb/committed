package db_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSyncableReadPosition_PublishedByWorker pins the db half of the
// readPosition chain: on leadership gain the owning worker publishes its live
// reader, so SyncableReadPosition answers with the reader's scan position —
// which must cover at least the raft index of the last entry the syncable
// consumed. With no worker registered there is no live position (the
// degraded/non-owner shape the HTTP layer serves as an absent field). The
// wal-reader semantics of the position itself (advances per examined entry,
// skips included) are pinned in the wal package's reader tests.
func TestSyncableReadPosition_PublishedByWorker(t *testing.T) {
	d, s := newWalDB(t)
	const id = "read-position"

	_, ok := d.SyncableReadPosition(id)
	require.False(t, ok, "no worker registered — no live position to report")

	seedSyncableConfig(t, d, id)
	seedUserProposals(t, d, s, "evt", []string{"v0", "v1", "v2"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	syncable := NewSyncable(3, cancel)
	require.NoError(t, d.Sync(context.Background(), id, syncable))
	<-ctx.Done()

	pos, ok := d.SyncableReadPosition(id)
	require.True(t, ok, "the owning worker must publish its reader")
	actuals := syncable.Actuals()
	require.GreaterOrEqual(t, pos, actuals[len(actuals)-1].Index,
		"the position covers everything examined — at least the last consumed entry")

	require.Equal(t, d.ID(), d.SyncableOwner(id),
		"an unpinned syncable is owned by the leader — this node, single-node")
}
