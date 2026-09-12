package db

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIngestFrozenFlag_Lifecycle covers the node-local recovering flag: set on
// freeze-exit, cleared on durable progress (db.clearIngestFrozen's setFrozen
// half), and cleared on worker teardown (prune) so a fresh worker isn't
// inherited as recovering.
func TestIngestFrozenFlag_Lifecycle(t *testing.T) {
	s := newIngestSupervisor(0, 0, 0)
	const id = "ing"

	require.False(t, s.isFrozen(id), "unset by default")

	s.setFrozen(id, true)
	require.True(t, s.isFrozen(id))

	s.setFrozen(id, false) // durable progress
	require.False(t, s.isFrozen(id), "progress clears recovering")

	s.setFrozen(id, true)
	s.prune(id) // worker teardown (re-POST / delete)
	require.False(t, s.isFrozen(id), "teardown clears recovering")
}
