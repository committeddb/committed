package db

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/metrics"
)

// newTestWatcher builds a diskWatcher with the production default thresholds
// (warn 20 / critical 10 / full 3), a no-op logger, and an injected usage
// probe driven by the returned setter. onState appends every published state
// to *states so a test can assert the transition sequence.
func newTestWatcher(states *[]diskState) (*diskWatcher, func(free, total uint64)) {
	w := &diskWatcher{
		path:        "/data",
		interval:    time.Hour,
		warnPct:     DefaultDiskWarnPercent,
		criticalPct: DefaultDiskCriticalPercent,
		fullPct:     DefaultDiskFullPercent,
		onState:     func(s diskState) { *states = append(*states, s) },
		logger:      zap.NewNop(),
	}
	var free, total uint64
	w.usage = func(string) (uint64, uint64, error) { return free, total, nil }
	set := func(f, t uint64) { free, total = f, t }
	return w, set
}

// TestNewDiskWatcher_RejectsNonDescendingThresholds pins the descending-order
// guard: the bands must be warn > critical > full (or a band is unreachable —
// classify() reaches the lower band first). A non-descending set — each value
// individually valid, so per-value validation misses it — warns and falls back to
// the defaults, matching the per-value fallback policy documented in
// disk-limits.md; a valid descending set is preserved as-is.
func TestNewDiskWatcher_RejectsNonDescendingThresholds(t *testing.T) {
	tests := map[string]struct {
		warn, critical, full float64
		wantDefaulted        bool
	}{
		"valid descending custom": {50, 25, 10, false},
		"warn below critical":     {5, 10, 3, true}, // the ticket's misconfig
		"warn equals critical":    {10, 10, 3, true},
		"critical equals full":    {20, 5, 5, true},
		"fully ascending":         {3, 10, 20, true},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			core, observed := observer.New(zap.WarnLevel)
			w := newDiskWatcher(
				DiskWatcherConfig{Path: "/data", WarnPercent: tt.warn, CriticalPercent: tt.critical, FullPercent: tt.full},
				func(diskState) {}, zap.New(core), nil,
			)

			if tt.wantDefaulted {
				require.Equal(t, DefaultDiskWarnPercent, w.warnPct)
				require.Equal(t, DefaultDiskCriticalPercent, w.criticalPct)
				require.Equal(t, DefaultDiskFullPercent, w.fullPct)
				require.Len(t, observed.FilterMessageSnippet("must be descending").All(), 1,
					"a non-descending set warns and falls back to defaults")
			} else {
				require.Equal(t, tt.warn, w.warnPct)
				require.Equal(t, tt.critical, w.criticalPct)
				require.Equal(t, tt.full, w.fullPct)
				require.Empty(t, observed.All(), "a valid descending set is accepted silently")
			}
		})
	}
}

func TestDiskWatcher_Classify(t *testing.T) {
	w := &diskWatcher{warnPct: 20, criticalPct: 10, fullPct: 3}
	tests := []struct {
		freePct float64
		want    diskState
	}{
		{100, diskOK},
		{20.1, diskOK},
		{20, diskWarn}, // inclusive at the threshold
		{15, diskWarn},
		{10, diskCritical}, // inclusive
		{5, diskCritical},
		{3, diskFull}, // inclusive
		{0, diskFull},
	}
	for _, tc := range tests {
		require.Equalf(t, tc.want, w.classify(tc.freePct), "classify(%v)", tc.freePct)
	}
}

func TestDiskWatcher_FirstSampleAlwaysPublishes(t *testing.T) {
	var states []diskState
	w, set := newTestWatcher(&states)

	// Plenty of free space → ok. Even though ok is the zero value, the first
	// sample must publish so the gate/gauges are initialized.
	set(900, 1000)
	require.NoError(t, w.sample())
	require.Equal(t, []diskState{diskOK}, states)
}

func TestDiskWatcher_DebouncesUnchangedState(t *testing.T) {
	var states []diskState
	w, set := newTestWatcher(&states)

	set(900, 1000) // ok
	require.NoError(t, w.sample())
	require.NoError(t, w.sample()) // still ok — no second publish
	require.NoError(t, w.sample())
	require.Equal(t, []diskState{diskOK}, states)
}

func TestDiskWatcher_TransitionsThroughLevelsAndRecovers(t *testing.T) {
	var states []diskState
	w, set := newTestWatcher(&states)

	set(500, 1000) // 50% → ok
	require.NoError(t, w.sample())
	set(150, 1000) // 15% → warn
	require.NoError(t, w.sample())
	set(80, 1000) // 8% → critical
	require.NoError(t, w.sample())
	set(20, 1000) // 2% → full
	require.NoError(t, w.sample())
	set(500, 1000) // recover straight to ok
	require.NoError(t, w.sample())

	require.Equal(t, []diskState{diskOK, diskWarn, diskCritical, diskFull, diskOK}, states)
}

func TestDiskWatcher_ProbeErrorRetainsState(t *testing.T) {
	var states []diskState
	w, set := newTestWatcher(&states)

	set(80, 1000) // critical
	require.NoError(t, w.sample())

	// A transient probe failure must not change the published state.
	boom := errors.New("statfs: temporary failure")
	w.usage = func(string) (uint64, uint64, error) { return 0, 0, boom }
	require.ErrorIs(t, w.sample(), boom)
	require.Equal(t, []diskState{diskCritical}, states, "probe error must not publish a new state")
}

func TestDiskWatcher_RunDisablesOnUnsupportedPlatform(t *testing.T) {
	var states []diskState
	w, _ := newTestWatcher(&states)
	w.usage = func(string) (uint64, uint64, error) { return 0, 0, errDiskWatchUnsupported }

	// run must return promptly (not block on the ticker) when the platform
	// can't probe disk usage, and must publish nothing.
	done := make(chan struct{})
	go func() { w.run(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run did not exit on unsupported-platform probe error")
	}
	require.Empty(t, states)
}

func TestDiskWatcher_RunStopsOnContextCancel(t *testing.T) {
	var states []diskState
	w, set := newTestWatcher(&states)
	w.interval = time.Millisecond
	set(900, 1000)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.run(ctx); close(done) }()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run did not exit on context cancel")
	}
}

func TestDiskRejection_PolicyMatrix(t *testing.T) {
	kinds := []string{"user", "config", "index", "position"}

	// ok + warn: everything flows.
	for _, state := range []diskState{diskOK, diskWarn} {
		for _, kind := range kinds {
			require.NoErrorf(t, diskRejection(state, kind), "state=%s kind=%s", state, kind)
		}
	}

	// critical: only user-data proposals are rejected.
	require.ErrorIs(t, diskRejection(diskCritical, "user"), cluster.ErrInsufficientStorage)
	for _, kind := range []string{"config", "index", "position"} {
		require.NoErrorf(t, diskRejection(diskCritical, kind), "critical should still accept %s", kind)
	}

	// full: user data AND config are frozen, but checkpoints (index/position)
	// still flow so syncables keep delivering + checkpointing rather than
	// re-delivering in a loop.
	require.ErrorIs(t, diskRejection(diskFull, "user"), cluster.ErrInsufficientStorage)
	require.ErrorIs(t, diskRejection(diskFull, "config"), cluster.ErrInsufficientStorage)
	for _, kind := range []string{"index", "position"} {
		require.NoErrorf(t, diskRejection(diskFull, kind), "full should still accept checkpoint kind %s", kind)
	}
}

func TestDiskWatcher_EmitsGauges(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	var states []diskState
	w, set := newTestWatcher(&states)
	w.metrics = metrics.New(provider.Meter("test"))

	set(80, 1000) // 8% free → critical
	require.NoError(t, w.sample())

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	require.Equal(t, 80.0, lastFloatGauge(t, rm, "committed.disk.free_bytes"))
	require.Equal(t, 8.0, lastFloatGauge(t, rm, "committed.disk.free_percent"))

	// disk.state is a mutually-exclusive set: exactly critical=1, rest 0.
	levels := floatGaugeByAttr(t, rm, "committed.disk.state", "level")
	require.Equal(t, 1.0, levels["critical"])
	require.Equal(t, 0.0, levels["ok"])
	require.Equal(t, 0.0, levels["warn"])
	require.Equal(t, 0.0, levels["full"])
}

func lastFloatGauge(t *testing.T, rm metricdata.ResourceMetrics, name string) float64 {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[float64])
			require.Truef(t, ok, "%s is not a float64 gauge", name)
			require.NotEmpty(t, g.DataPoints)
			return g.DataPoints[len(g.DataPoints)-1].Value
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}

func floatGaugeByAttr(t *testing.T, rm metricdata.ResourceMetrics, name, attrKey string) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[float64])
			require.Truef(t, ok, "%s is not a float64 gauge", name)
			for _, dp := range g.DataPoints {
				if v, ok := dp.Attributes.Value(attribute.Key(attrKey)); ok {
					out[v.AsString()] = dp.Value
				}
			}
		}
	}
	require.NotEmptyf(t, out, "metric %s not found", name)
	return out
}
