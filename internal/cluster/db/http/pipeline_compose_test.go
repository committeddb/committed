package http

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// Fail-loud composition legs of the pipeline view — the shapes a live
// single-node fixture cannot produce (its workers block alive and its
// progress reads succeed), pinned against the extracted composition
// functions. The composed happy paths run for real in pipeline_test.go.

// TestComposePipelineProducer: a not-running producer is NAMED with its
// error rather than dropped, and forces the pipeline out of rest; a
// readable producer contributes its own caughtUp.
func TestComposePipelineProducer(t *testing.T) {
	ingest, ingestErr, caughtUp := composePipelineProducer(cluster.IngestableStatus{}, cluster.ErrIngestableNotRunning)
	require.Nil(t, ingest)
	require.Contains(t, ingestErr, "no ingestable worker is running")
	require.False(t, caughtUp, "an unknown producer can't be at rest")

	lag := uint64(0)
	ingest, ingestErr, caughtUp = composePipelineProducer(cluster.IngestableStatus{
		WorkerState: cluster.WorkerStateRunning, Phase: "streaming",
		Lag: &lag, LagUnit: cluster.LagUnitBytes, CaughtUp: true,
	}, nil)
	require.NotNil(t, ingest)
	require.Empty(t, ingestErr)
	require.True(t, caughtUp)
}

// TestComposePipelineConsumer: the fail-loud and redaction legs — a failed
// progress read lists the consumer WITH its error (never dropped, never at
// rest), and a wrapped RedactedError exposes only its PII-free message.
func TestComposePipelineConsumer(t *testing.T) {
	t.Run("progress error is listed, redacted, and never at rest", func(t *testing.T) {
		wrapped := fmt.Errorf("read progress: %w",
			testRedactedError{full: "dial tcp user:hunter2@10.0.0.5: refused", safe: "destination unreachable"})
		row, caughtUp := composePipelineConsumer("rec-1", cluster.SyncableStuck{}, false, 0, 10, wrapped)
		require.Equal(t, "rec-1", row.ID)
		require.NotEmpty(t, row.Error, "a failing consumer is listed, not dropped")
		require.NotContains(t, row.Error, "hunter2", "progress errors may echo connection identity — redact")
		require.Contains(t, row.Error, "destination unreachable")
		require.False(t, caughtUp)
	})

	t.Run("parked worker state rides the row", func(t *testing.T) {
		row, caughtUp := composePipelineConsumer("rec-1",
			cluster.SyncableStuck{Parked: true}, true, 5, 10, nil)
		require.Equal(t, cluster.WorkerStateParked, row.WorkerState)
		require.False(t, row.Stuck, "parked is terminal, not a transient stall")
		require.Equal(t, uint64(5), row.Lag)
		require.False(t, caughtUp)
	})

	t.Run("caught up at the head", func(t *testing.T) {
		row, caughtUp := composePipelineConsumer("rec-1", cluster.SyncableStuck{}, false, 10, 10, nil)
		require.True(t, row.CaughtUp)
		require.True(t, caughtUp)
		require.Zero(t, row.Lag)
	})
}

// testRedactedError implements cluster.RedactedError: the full text may
// echo PII (connection identity), the redacted message is sink-safe.
type testRedactedError struct{ full, safe string }

func (e testRedactedError) Error() string           { return e.full }
func (e testRedactedError) RedactedMessage() string { return e.safe }
