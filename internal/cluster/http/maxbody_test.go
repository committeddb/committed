package http_test

import (
	"bytes"
	"context"
	nethttp "net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/http"
	cmetrics "github.com/committeddb/committed/internal/cluster/metrics"
)

// TestMaxBodyBytes is the OOM-DoS regression: an over-cap request body is
// rejected with 413 (bounded by the maxBytes middleware's MaxBytesReader) before
// it can be buffered into memory. The rejection is attributable — the route is
// surfaced in the response/log and counted on committed.http.request_too_large —
// so an operator can watch for the cap false-rejecting legitimate proposals.
func TestMaxBodyBytes(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m := cmetrics.New(provider.Meter("test"))

	h := http.New(&clusterfakes.FakeCluster{}, http.WithMaxBodyBytes(64), http.WithMetrics(m))

	// Over the cap → 413 request_too_large, with the route in the message.
	req := httptest.NewRequest(nethttp.MethodPost, "http://localhost/v1/proposal",
		bytes.NewReader(bytes.Repeat([]byte("a"), 200)))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, nethttp.StatusRequestEntityTooLarge, w.Code)
	require.Contains(t, w.Body.String(), "request_too_large")
	require.Contains(t, w.Body.String(), "/v1/proposal", "the route is surfaced so a false rejection is attributable")

	// The rejection is counted, labeled by route.
	require.EqualValues(t, 1, requestTooLargeCount(t, reader, "/v1/proposal"),
		"committed.http.request_too_large must increment for the route")

	// Under the cap → not rejected by the cap (reaches the handler; never 413).
	req2 := httptest.NewRequest(nethttp.MethodPost, "http://localhost/v1/proposal",
		bytes.NewReader([]byte(`{"entities":[]}`)))
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, req2)
	require.NotEqual(t, nethttp.StatusRequestEntityTooLarge, w2.Code)
}

func requestTooLargeCount(t *testing.T, reader sdkmetric.Reader, route string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if md.Name != "committed.http.request_too_large" {
				continue
			}
			sum, ok := md.Data.(metricdata.Sum[int64])
			if !ok {
				return 0
			}
			for _, dp := range sum.DataPoints {
				for _, attr := range dp.Attributes.ToSlice() {
					if string(attr.Key) == "route" && attr.Value.AsString() == route {
						return dp.Value
					}
				}
			}
		}
	}
	return 0
}
