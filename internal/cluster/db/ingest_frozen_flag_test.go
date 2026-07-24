package db

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIngestFrozenFlag_Lifecycle covers the node-local recovering flag: set on
// freeze-exit, cleared on durable progress (clearIngestFrozen), and cleared on
// worker teardown (pruneIngestSupervisorState) so a fresh worker isn't inherited
// as recovering.
func TestIngestFrozenFlag_Lifecycle(t *testing.T) {
	db := &DB{}
	const id = "ing"

	require.False(t, db.isIngestFrozen(id), "unset by default")

	db.setIngestFrozen(id, true)
	require.True(t, db.isIngestFrozen(id))

	db.clearIngestFrozen(id) // durable progress
	require.False(t, db.isIngestFrozen(id), "progress clears recovering")

	db.setIngestFrozen(id, true)
	db.pruneIngestSupervisorState(id) // worker teardown (re-POST / delete)
	require.False(t, db.isIngestFrozen(id), "teardown clears recovering")
}
