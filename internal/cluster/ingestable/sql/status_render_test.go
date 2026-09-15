package sql

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
)

// TestRenderStatus pins the skeleton the three dialect Status methods share:
// the empty-position pending gate (no probe), the snapshot phase from
// in-flight progress (no probe), the streaming phase with the probe's
// source-side fill, and decode errors passed through untouched.
func TestRenderStatus(t *testing.T) {
	cfg := &Config{Tables: []string{"orders"}, Topics: []TopicSpec{{Type: &cluster.Type{ID: "orders"}, Tables: []string{"orders"}}}}
	probed := 0
	probe := func(_ context.Context, st *cluster.IngestableStatus) {
		probed++
		lag := uint64(3)
		st.Lag, st.LagUnit, st.CaughtUp = &lag, cluster.LagUnitTransactions, false
	}
	decodeOK := func(progress *dialectpb.SnapshotProgress) func(cluster.Position) (StatusInputs, error) {
		return func(cluster.Position) (StatusInputs, error) {
			return StatusInputs{Position: "coord:7", Progress: progress}, nil
		}
	}

	st, err := RenderStatus(context.Background(), cfg, nil, decodeOK(nil), probe)
	require.NoError(t, err)
	require.Equal(t, "pending", st.Phase)
	require.Zero(t, probed, "the pending gate never queries the source")

	st, err = RenderStatus(context.Background(), cfg, cluster.Position("x"), decodeOK(&dialectpb.SnapshotProgress{}), probe)
	require.NoError(t, err)
	require.Equal(t, "snapshot", st.Phase)
	require.Equal(t, "coord:7", st.Position)
	require.Zero(t, probed, "the snapshot phase renders from replicated state alone")

	st, err = RenderStatus(context.Background(), cfg, cluster.Position("x"), decodeOK(nil), probe)
	require.NoError(t, err)
	require.Equal(t, "streaming", st.Phase)
	require.Equal(t, 1, probed)
	require.Equal(t, uint64(3), *st.Lag)
	require.Equal(t, cluster.LagUnitTransactions, st.LagUnit)

	boom := errors.New("decode position: bad")
	_, err = RenderStatus(context.Background(), cfg, cluster.Position("x"), func(cluster.Position) (StatusInputs, error) { return StatusInputs{}, boom }, probe)
	require.ErrorIs(t, err, boom, "decode errors pass through with the dialect's wrapping")
}
