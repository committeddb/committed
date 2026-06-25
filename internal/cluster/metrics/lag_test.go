package metrics_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/committeddb/committed/internal/cluster/metrics"
)

type fakeLagProvider struct {
	sync   map[string]uint64
	ingest map[string]*uint64
}

func (f fakeLagProvider) SyncableLags() map[string]uint64    { return f.sync }
func (f fakeLagProvider) IngestableLags() map[string]*uint64 { return f.ingest }

func u64(v uint64) *uint64 { return &v }

// collectLag registers the lag gauges against a fresh manual reader, then
// collects once (reuses the package test's collect helper).
func collectLag(t *testing.T, p metrics.LagProvider) metricdata.ResourceMetrics {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	require.NoError(t, metrics.RegisterLagGauges(provider.Meter("test"), p))
	return collect(t, reader)
}

// gaugeByID flattens a float64 observable gauge to {attribute-value: value}.
func gaugeByID(t *testing.T, rm metricdata.ResourceMetrics, name string) map[string]float64 {
	t.Helper()
	m := findMetric(rm, name)
	require.NotNil(t, m, "gauge %s not found", name)
	g, ok := m.Data.(metricdata.Gauge[float64])
	require.True(t, ok, "%s is not a float64 gauge", name)
	out := map[string]float64{}
	for _, dp := range g.DataPoints {
		it := dp.Attributes.Iter()
		for it.Next() {
			out[it.Attribute().Value.AsString()] = dp.Value
		}
	}
	return out
}

// The gauges read the provider at collection (scrape) time: every syncable is
// observed (including a caught-up 0), and an ingestable's lag is observed only
// when known — a nil (MySQL / mid-snapshot / unreachable) is absent, not 0.
func TestLagGauges_ObserveAtCollect(t *testing.T) {
	rm := collectLag(t, fakeLagProvider{
		sync:   map[string]uint64{"s1": 7, "s2": 0},
		ingest: map[string]*uint64{"i1": u64(4096), "i2": nil},
	})

	sync := gaugeByID(t, rm, "committed.sync.lag")
	require.Equal(t, 7.0, sync["s1"])
	require.Equal(t, 0.0, sync["s2"], "a caught-up syncable reports lag 0")

	ingest := gaugeByID(t, rm, "committed.ingest.lag")
	require.Equal(t, 4096.0, ingest["i1"])
	_, present := ingest["i2"]
	require.False(t, present, "an unknown (nil) ingest lag must be absent, not 0")
}

// A second provider with a large value confirms the gauge reports the provider's
// current value at collection — the point of an observable gauge: a syncable
// that fell behind shows its grown lag at the next scrape with no push.
func TestLagGauges_TrackCurrentValue(t *testing.T) {
	rm := collectLag(t, fakeLagProvider{sync: map[string]uint64{"s1": 12000}})
	require.Equal(t, 12000.0, gaugeByID(t, rm, "committed.sync.lag")["s1"])
}
