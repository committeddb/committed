package sql_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	sql "github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// TestRefreshSnapshotEpoch pins the fix for the refresh-epoch reset: a fresh full
// snapshot must stamp STRICTLY ABOVE the delete-surviving topic highwater, so a
// same-topic recreate (whose cleared position reset the checkpoint epoch to 0)
// does not re-emit at epoch 1 and leave a vacuous "generation < 1" sweep that
// strands every stale sink row forever.
func TestRefreshSnapshotEpoch(t *testing.T) {
	cases := []struct {
		name            string
		checkpointEpoch uint64
		floor           uint64
		want            uint64
	}{
		{"first creation, no history", 0, 0, 1},
		{"recreate: cleared checkpoint, sink holds up to gen 3", 0, 3, 4},
		{"recreate: sink holds up to gen 1 (baseline)", 0, 1, 2},
		{"recreate: large highwater", 0, 42, 43},
		// A surviving checkpoint at the highwater still bumps (this helper only
		// runs for a FRESH full snapshot, i.e. a re-refresh) — never re-stamps at
		// or below a generation already on the sink.
		{"checkpoint present, equals floor", 5, 5, 6},
		{"checkpoint above a lagging floor", 7, 3, 8},
		{"floor above a lagging checkpoint", 3, 7, 8},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, sql.RefreshSnapshotEpoch(tc.checkpointEpoch, tc.floor),
				"a fresh snapshot must stamp strictly above max(checkpoint, sink highwater)")
		})
	}
}
