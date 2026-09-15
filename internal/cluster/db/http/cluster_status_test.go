package http_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db/http"
)

// TestClusterStatus_Healthy: a healthy real cluster returns an empty
// (non-null) parkedWorkers array — the []-not-null guarantee lets clients
// iterate without a nil check. (The populated rendering is pinned by the
// parkedWorkersResponse table in status_mapping_test.go — parking a worker
// for real means tripping the circuit breaker.)
func TestClusterStatus_Healthy(t *testing.T) {
	e := newEngine(t)

	w := e.doEmpty(t, "GET", "/v1/cluster/status")
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Equal(t, "application/json", w.Result().Header.Get("Content-Type"))

	var body http.ClusterStatusResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.NotNil(t, body.ParkedWorkers, "parkedWorkers must be [] not null")
	require.Empty(t, body.ParkedWorkers)
}
