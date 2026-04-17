package metrics_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/philborlin/committed/internal/cluster/metrics"
)

func setupTest(t *testing.T) (*metrics.Metrics, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { provider.Shutdown(context.Background()) })
	m := metrics.New(provider.Meter("test"))
	return m, reader
}

func collect(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	err := reader.Collect(context.Background(), &rm)
	require.NoError(t, err)
	return rm
}

func findMetric(rm metricdata.ResourceMetrics, name string) *metricdata.Metrics {
	for _, sm := range rm.ScopeMetrics {
		for i := range sm.Metrics {
			if sm.Metrics[i].Name == name {
				return &sm.Metrics[i]
			}
		}
	}
	return nil
}

func TestNew_CreatesAllInstruments(t *testing.T) {
	m, reader := setupTest(t)

	// Exercise every domain method to ensure instruments are created.
	m.ProposalSubmitted("user")
	m.ProposalApplied(10 * time.Millisecond)
	m.SetLeader(true)
	m.EntryApplied(42, 1*time.Millisecond)
	m.SetIndexRange(1, 100)
	m.SyncCompleted("test-sync", 5*time.Millisecond)
	m.SetWorkerRunning("sync", "s1", true)
	m.WorkerReplaced("sync", "s1")

	rm := collect(t, reader)

	expected := []string{
		"committed.proposals",
		"committed.propose.duration",
		"committed.apply.index",
		"committed.first.index",
		"committed.last.index",
		"committed.leader",
		"committed.apply.duration",
		"committed.sync.duration",
		"committed.worker.running",
		"committed.worker.replaces",
	}

	for _, name := range expected {
		require.NotNil(t, findMetric(rm, name), "missing metric: %s", name)
	}
}

func TestProposalSubmitted_IncrementsCounter(t *testing.T) {
	m, reader := setupTest(t)

	m.ProposalSubmitted("user")
	m.ProposalSubmitted("user")
	m.ProposalSubmitted("config")

	rm := collect(t, reader)
	metric := findMetric(rm, "committed.proposals")
	require.NotNil(t, metric)

	sum, ok := metric.Data.(metricdata.Sum[int64])
	require.True(t, ok)

	counts := make(map[string]int64)
	for _, dp := range sum.DataPoints {
		for _, attr := range dp.Attributes.ToSlice() {
			if string(attr.Key) == "kind" {
				counts[attr.Value.AsString()] = dp.Value
			}
		}
	}

	require.Equal(t, int64(2), counts["user"])
	require.Equal(t, int64(1), counts["config"])
}

func TestProposalApplied_RecordsDuration(t *testing.T) {
	m, reader := setupTest(t)

	m.ProposalApplied(50 * time.Millisecond)

	rm := collect(t, reader)
	metric := findMetric(rm, "committed.propose.duration")
	require.NotNil(t, metric)

	hist, ok := metric.Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	require.Len(t, hist.DataPoints, 1)
	require.Equal(t, uint64(1), hist.DataPoints[0].Count)
	require.Greater(t, hist.DataPoints[0].Sum, 0.0)
}

func TestSetLeader(t *testing.T) {
	m, reader := setupTest(t)

	m.SetLeader(true)

	rm := collect(t, reader)
	metric := findMetric(rm, "committed.leader")
	require.NotNil(t, metric)

	gauge, ok := metric.Data.(metricdata.Gauge[float64])
	require.True(t, ok)
	require.Len(t, gauge.DataPoints, 1)
	require.Equal(t, 1.0, gauge.DataPoints[0].Value)
}
