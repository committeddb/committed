package http_test

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/http"
)

func doClusterStatus(t *testing.T, h *http.HTTP) (int, http.ClusterStatusResponse) {
	t.Helper()
	req := httptest.NewRequest("GET", "http://localhost/v1/cluster/status", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	bs, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var body http.ClusterStatusResponse
	require.NoError(t, json.Unmarshal(bs, &body))
	return resp.StatusCode, body
}

// TestClusterStatus_Healthy verifies a healthy cluster returns an empty
// (non-null) parkedWorkers array — the []-not-null guarantee lets clients
// iterate without a nil check.
func TestClusterStatus_Healthy(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	h := http.New(fake)

	status, body := doClusterStatus(t, h)

	require.Equal(t, 200, status)
	require.NotNil(t, body.ParkedWorkers, "parkedWorkers must be [] not null")
	require.Empty(t, body.ParkedWorkers)
}

// TestClusterStatus_ParkedWorkers verifies /cluster/status lists terminally-parked
// workers (sync and ingest) from replicated state — the cluster-wide summary that
// reads the same from any node.
func TestClusterStatus_ParkedWorkers(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.ParkedWorkersReturns([]cluster.ParkedWorker{
		{Kind: "sync", ID: "orders-sync"},
		{Kind: "ingest", ID: "catalog-ingest"},
	}, nil)
	h := http.New(fake)

	status, body := doClusterStatus(t, h)

	require.Equal(t, 200, status)
	require.Equal(t, []http.ParkedWorkerResponse{
		{Kind: "sync", ID: "orders-sync"},
		{Kind: "ingest", ID: "catalog-ingest"},
	}, body.ParkedWorkers)
}
