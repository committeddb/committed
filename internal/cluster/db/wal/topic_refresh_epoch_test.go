package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestTopicRefreshEpoch_ApplyBumpsAndSurvivesDelete pins the core of the
// refresh-epoch reset fix: ApplyCommitted raises the per-topic highwater from
// every generation-stamped entity, and — because the highwater is keyed by TOPIC,
// not ingestable id, and lives in a bucket DeleteIngestable never clears — it
// SURVIVES a delete. That is what lets a same-topic recreate (whose position, and
// the epoch inside it, are wiped) resume its generation above the rows still on
// the sink instead of resetting to epoch 1 and stranding them.
func TestTopicRefreshEpoch_ApplyBumpsAndSurvivesDelete(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	// Register the topic type so a proposal referencing it resolves on apply.
	tp := &cluster.Type{ID: "orders", Name: "Orders", Version: 1}
	tpe, err := cluster.NewUpsertTypeEntity(tp)
	require.NoError(t, err)
	saveEntity(t, tpe, s, 1, 1)

	require.Equal(t, uint64(0), s.TopicRefreshEpoch("orders"))

	// Apply a refresh-boundary marker at epoch 3 (a generation-stamped entity);
	// ApplyCommitted must raise the per-topic highwater to 3.
	marker3 := cluster.NewRefreshBoundaryEntity(tp, 3)
	saveEntity(t, marker3, s, 1, 2)
	require.Equal(t, uint64(3), s.TopicRefreshEpoch("orders"),
		"ApplyCommitted must raise the highwater from a generation-stamped entity")

	// A later, higher generation advances it; the store is monotonic.
	marker5 := cluster.NewRefreshBoundaryEntity(tp, 5)
	saveEntity(t, marker5, s, 1, 3)
	require.Equal(t, uint64(5), s.TopicRefreshEpoch("orders"))

	// Delete the ingestable producing the topic. Its position (and the epoch
	// inside it) is cleared, but the per-topic highwater must persist.
	del := cluster.NewDeleteIngestableEntities("orders-ingestable")
	saveProposal(t, &cluster.Proposal{Entities: del}, s, 1, 4)
	require.Equal(t, uint64(5), s.TopicRefreshEpoch("orders"),
		"the topic refresh-epoch highwater must survive DeleteIngestable")

	// And across a restart after the delete.
	s = s.CloseAndReopenStorage(t)
	defer s.Cleanup()
	require.Equal(t, uint64(5), s.TopicRefreshEpoch("orders"),
		"the surviving highwater must be durable across a restart")
}
