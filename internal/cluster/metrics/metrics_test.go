package metrics_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/committeddb/committed/internal/cluster/metrics"
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
	m.SetDiskFree(1<<30, 42.5)
	m.SetDiskState("warn")

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
		"committed.disk.free_bytes",
		"committed.disk.free_percent",
		"committed.disk.state",
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

func TestSetDiskFree(t *testing.T) {
	m, reader := setupTest(t)

	m.SetDiskFree(2048, 12.5)

	rm := collect(t, reader)

	bytesM := findMetric(rm, "committed.disk.free_bytes")
	require.NotNil(t, bytesM)
	bytesG, ok := bytesM.Data.(metricdata.Gauge[float64])
	require.True(t, ok)
	require.Len(t, bytesG.DataPoints, 1)
	require.Equal(t, 2048.0, bytesG.DataPoints[0].Value)

	pctM := findMetric(rm, "committed.disk.free_percent")
	require.NotNil(t, pctM)
	pctG, ok := pctM.Data.(metricdata.Gauge[float64])
	require.True(t, ok)
	require.Len(t, pctG.DataPoints, 1)
	require.Equal(t, 12.5, pctG.DataPoints[0].Value)
}

// TestSetDiskState_MutuallyExclusive asserts the active level reads 1 and every
// other level reads 0 — so a dashboard alerting on committed_disk_state{level=
// "full"} never sees a stale 1 left on a level the node has since left.
func TestSetDiskState_MutuallyExclusive(t *testing.T) {
	m, reader := setupTest(t)

	m.SetDiskState("critical")

	rm := collect(t, reader)
	metric := findMetric(rm, "committed.disk.state")
	require.NotNil(t, metric)
	gauge, ok := metric.Data.(metricdata.Gauge[float64])
	require.True(t, ok)

	got := make(map[string]float64)
	for _, dp := range gauge.DataPoints {
		for _, attr := range dp.Attributes.ToSlice() {
			if string(attr.Key) == "level" {
				got[attr.Value.AsString()] = dp.Value
			}
		}
	}
	require.Equal(t, map[string]float64{"ok": 0, "warn": 0, "critical": 1, "full": 0}, got)
}

// TestSetDiskClusterState_MutuallyExclusive mirrors the node-local disk-state
// gauge contract for the cluster-effective level the admission gate enforces.
func TestSetDiskClusterState_MutuallyExclusive(t *testing.T) {
	m, reader := setupTest(t)

	m.SetDiskClusterState("full")

	rm := collect(t, reader)
	metric := findMetric(rm, "committed.disk.cluster_state")
	require.NotNil(t, metric)
	gauge, ok := metric.Data.(metricdata.Gauge[float64])
	require.True(t, ok)

	got := make(map[string]float64)
	for _, dp := range gauge.DataPoints {
		for _, attr := range dp.Attributes.ToSlice() {
			if string(attr.Key) == "level" {
				got[attr.Value.AsString()] = dp.Value
			}
		}
	}
	require.Equal(t, map[string]float64{"ok": 0, "warn": 0, "critical": 0, "full": 1}, got)
}

// TestSetWriteAdmission asserts both halves of the admission signal: the
// plain 1/0 admitted gauge, and the mutually-exclusive reason gauges where
// exactly the active cause reads 1.
func TestSetWriteAdmission(t *testing.T) {
	m, reader := setupTest(t)

	m.SetWriteAdmission(false, "quorum_at_risk")

	rm := collect(t, reader)

	admitted := findMetric(rm, "committed.write.admitted")
	require.NotNil(t, admitted)
	ag, ok := admitted.Data.(metricdata.Gauge[float64])
	require.True(t, ok)
	require.Len(t, ag.DataPoints, 1)
	require.Equal(t, 0.0, ag.DataPoints[0].Value)

	reason := findMetric(rm, "committed.write.admission_reason")
	require.NotNil(t, reason)
	rg, ok := reason.Data.(metricdata.Gauge[float64])
	require.True(t, ok)

	got := make(map[string]float64)
	for _, dp := range rg.DataPoints {
		for _, attr := range dp.Attributes.ToSlice() {
			if string(attr.Key) == "reason" {
				got[attr.Value.AsString()] = dp.Value
			}
		}
	}
	require.Equal(t, map[string]float64{
		"ok": 0, "leader_disk": 0, "quorum_at_risk": 1, "cluster_reject": 0, "local_fallback": 0,
	}, got)

	// Recovery flips both: admitted reads 1 and the reason moves to ok.
	m.SetWriteAdmission(true, "ok")
	rm = collect(t, reader)
	admitted = findMetric(rm, "committed.write.admitted")
	ag, ok = admitted.Data.(metricdata.Gauge[float64])
	require.True(t, ok)
	require.Equal(t, 1.0, ag.DataPoints[0].Value)
}

// TestDiskLeadershipTransfer counts disk-pressure transfers.
func TestDiskLeadershipTransfer(t *testing.T) {
	m, reader := setupTest(t)

	m.DiskLeadershipTransfer()
	m.DiskLeadershipTransfer()

	rm := collect(t, reader)
	metric := findMetric(rm, "committed.disk.leadership_transfers")
	require.NotNil(t, metric)
	sum, ok := metric.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 1)
	require.Equal(t, int64(2), sum.DataPoints[0].Value)
}
