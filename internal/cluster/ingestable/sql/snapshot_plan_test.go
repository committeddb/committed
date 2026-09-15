package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
)

// TestPlanSnapshot pins the four shared decisions' epoch rules and progress
// shapes — the arithmetic that used to be scattered across three dialects.
func TestPlanSnapshot(t *testing.T) {
	tables := []string{"a", "b", "c"}

	t.Run("cold: first snapshot starts at 1, a recreate stamps strictly above the highwater", func(t *testing.T) {
		p := PlanSnapshot(SnapshotCold, nil, 0, 0, tables, nil)
		require.Equal(t, uint64(1), p.Epoch)
		require.True(t, p.Marker)
		require.Empty(t, p.Progress.CompletedTables)
		require.False(t, p.Progress.PartialBackfill)
		require.Equal(t, uint64(4), PlanSnapshot(SnapshotCold, nil, 0, 3, tables, nil).Epoch, "cleared position, sink at 3 → 4")
	})

	t.Run("resume keeps the in-progress epoch and its shape", func(t *testing.T) {
		seed := &dialectpb.SnapshotProgress{CompletedTables: []string{"a"}, LastPkByTable: map[string]string{"b": "k"}}
		p := PlanSnapshot(SnapshotResume, seed, 5, 9, tables, nil)
		require.Equal(t, uint64(5), p.Epoch, "never bumped mid-snapshot, whatever the floor")
		require.True(t, p.Marker)
		require.Equal(t, []string{"a"}, p.Progress.CompletedTables)
		require.NotSame(t, seed, p.Progress, "a clone: the live cursor is independent of the checkpoint")
		partial := PlanSnapshot(SnapshotResume, &dialectpb.SnapshotProgress{PartialBackfill: true}, 0, 0, tables, nil)
		require.Equal(t, uint64(1), partial.Epoch)
		require.False(t, partial.Marker, "a resumed partial backfill still closes without a sweep")
	})

	t.Run("backfill pre-seeds siblings, floors the epoch, never sweeps", func(t *testing.T) {
		p := PlanSnapshot(SnapshotBackfill, nil, 2, 3, tables, []string{"c"})
		require.Equal(t, uint64(3), p.Epoch)
		require.False(t, p.Marker)
		require.True(t, p.Progress.PartialBackfill)
		require.Equal(t, []string{"a", "b"}, p.Progress.CompletedTables, "only the added table scans")
		require.Equal(t, uint64(1), PlanSnapshot(SnapshotBackfill, nil, 0, 0, tables, []string{"c"}).Epoch, "a pre-feature checkpoint backfills at 1, never 0")
	})

	t.Run("gap bumps above everything seen", func(t *testing.T) {
		require.Equal(t, uint64(4), PlanSnapshot(SnapshotGap, nil, 3, 0, tables, nil).Epoch)
		require.Equal(t, uint64(6), PlanSnapshot(SnapshotGap, nil, 3, 5, tables, nil).Epoch)
		require.Equal(t, uint64(2), PlanSnapshot(SnapshotGap, nil, 0, 0, tables, nil).Epoch)
		require.True(t, PlanSnapshot(SnapshotGap, nil, 0, 0, tables, nil).Marker)
	})
}

func TestFloorEpoch(t *testing.T) {
	require.Equal(t, uint64(1), FloorEpoch(0, 0))
	require.Equal(t, uint64(3), FloorEpoch(1, 3))
	require.Equal(t, uint64(7), FloorEpoch(7, 3))
}

// TestCompleteSnapshot pins the closing sequence: one marker per topic at the
// epoch when the plan carries one, none otherwise, and the completion
// checkpoint in both cases.
func TestCompleteSnapshot(t *testing.T) {
	cfg := &Config{Topics: []TopicSpec{{Type: &cluster.Type{ID: "t1"}}, {Type: &cluster.Type{ID: "t2"}}, {}}}
	pr := make(chan *cluster.Proposal, 4)
	po := make(chan cluster.Position, 1)

	require.NoError(t, CompleteSnapshot(context.Background(), cfg, true, 7, []byte("ckpt"), pr, po))
	require.Len(t, pr, 2, "one marker per typed topic")
	for i := 0; i < 2; i++ {
		p := <-pr
		require.Len(t, p.Entities, 1, "one marker per proposal keeps proposals homogeneous")
		require.Equal(t, uint64(7), p.Entities[0].Generation)
	}
	require.Equal(t, cluster.Position("ckpt"), <-po)

	require.NoError(t, CompleteSnapshot(context.Background(), cfg, false, 7, []byte("ckpt2"), pr, po))
	require.Empty(t, pr, "a partial backfill closes without a sweep")
	require.Equal(t, cluster.Position("ckpt2"), <-po, "but still checkpoints")
}
