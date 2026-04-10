package db_test

import (
	"context"
	"testing"

	"github.com/philborlin/committed/internal/cluster/db"
	parser "github.com/philborlin/committed/internal/cluster/db/parser"
	"github.com/philborlin/committed/internal/cluster/metrics"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestPropose_IncrementsMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { provider.Shutdown(context.Background()) })

	m := metrics.New(provider.Meter("test"))
	s := NewMemoryStorage()
	id := uint64(1)
	peers := make(db.Peers)
	peers[id] = ""

	d := db.New(id, peers, s, parser.New(), nil, nil,
		db.WithTickInterval(testTickInterval),
		db.WithMetrics(m),
	)
	defer d.Close()

	ps := createProposals([][]string{{"hello"}, {"world"}})
	for _, p := range ps {
		err := d.Propose(testCtx(t), p)
		require.NoError(t, err)
	}

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	// Check proposals counter incremented for "user" kind.
	proposalMetric := findMetric(rm, "committed.proposals")
	require.NotNil(t, proposalMetric)
	sum, ok := proposalMetric.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	var userCount int64
	for _, dp := range sum.DataPoints {
		for _, attr := range dp.Attributes.ToSlice() {
			if string(attr.Key) == "kind" && attr.Value.AsString() == "user" {
				userCount = dp.Value
			}
		}
	}
	require.Equal(t, int64(2), userCount)

	// Check propose duration histogram has observations.
	durationMetric := findMetric(rm, "committed.propose.duration")
	require.NotNil(t, durationMetric)
	hist, ok := durationMetric.Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	require.Len(t, hist.DataPoints, 1)
	require.Equal(t, uint64(2), hist.DataPoints[0].Count)

	// Check apply index is non-zero.
	applyMetric := findMetric(rm, "committed.apply.index")
	require.NotNil(t, applyMetric)
	gauge, ok := applyMetric.Data.(metricdata.Gauge[float64])
	require.True(t, ok)
	require.Greater(t, gauge.DataPoints[0].Value, 0.0)

	// Check leader gauge is 1 (single-node cluster).
	leaderMetric := findMetric(rm, "committed.leader")
	require.NotNil(t, leaderMetric)
	leaderGauge, ok := leaderMetric.Data.(metricdata.Gauge[float64])
	require.True(t, ok)
	require.Equal(t, 1.0, leaderGauge.DataPoints[0].Value)
}

func TestProposeType_IncrementsConfigCounter(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { provider.Shutdown(context.Background()) })

	m := metrics.New(provider.Meter("test"))
	s := NewMemoryStorage()
	id := uint64(1)
	peers := make(db.Peers)
	peers[id] = ""

	d := db.New(id, peers, s, parser.New(), nil, nil,
		db.WithTickInterval(testTickInterval),
		db.WithMetrics(m),
	)
	defer d.Close()

	tp := createType("test-type")
	err := d.ProposeType(testCtx(t), tp.config)
	require.NoError(t, err)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	proposalMetric := findMetric(rm, "committed.proposals")
	require.NotNil(t, proposalMetric)
	sum, ok := proposalMetric.Data.(metricdata.Sum[int64])
	require.True(t, ok)

	var configCount int64
	for _, dp := range sum.DataPoints {
		for _, attr := range dp.Attributes.ToSlice() {
			if string(attr.Key) == "kind" && attr.Value.AsString() == "config" {
				configCount = dp.Value
			}
		}
	}
	require.Equal(t, int64(1), configCount)
}

// findMetric searches collected metrics by name.
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
