package http_test

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	"github.com/philborlin/committed/internal/cluster/http"
)

func doNodeStatus(t *testing.T, h *http.HTTP) (int, http.NodeStatusResponse) {
	t.Helper()
	req := httptest.NewRequest("GET", "http://localhost/v1/node/status", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	bs, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var body http.NodeStatusResponse
	require.NoError(t, json.Unmarshal(bs, &body))
	return resp.StatusCode, body
}

// TestNodeStatus_Healthy verifies a node with no degraded configs reports
// its raft identity and an empty (non-null) degradedConfigs array. The
// empty-array guarantee matters: a JSON null would force every client to
// special-case it.
func TestNodeStatus_Healthy(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.IDReturns(2)
	fake.LeaderReturns(1)
	fake.AppliedIndexReturns(42)
	// ConfigBuildErrors defaults to returning nil — a healthy node.
	h := http.New(fake)

	status, body := doNodeStatus(t, h)

	require.Equal(t, 200, status)
	require.Equal(t, uint64(2), body.Node)
	require.Equal(t, uint64(1), body.Leader)
	require.Equal(t, uint64(42), body.AppliedIndex)
	require.NotNil(t, body.DegradedConfigs, "degradedConfigs must be [] not null")
	require.Empty(t, body.DegradedConfigs)
}

// TestNodeStatus_Degraded verifies a node with a config it could not build
// lists it (kind, id, error) and that the error names the missing ${VAR}.
// node identifies the answering node so an operator behind a load balancer
// knows which node is degraded.
func TestNodeStatus_Degraded(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.IDReturns(3)
	fake.LeaderReturns(1)
	fake.AppliedIndexReturns(99)
	fake.ConfigBuildErrorsReturns([]cluster.ConfigBuildError{
		{Kind: "database", ID: "orders-warehouse", Error: "missing environment variable ${WAREHOUSE_PW}"},
	})
	h := http.New(fake)

	status, body := doNodeStatus(t, h)

	require.Equal(t, 200, status)
	require.Equal(t, uint64(3), body.Node)
	require.Len(t, body.DegradedConfigs, 1)
	require.Equal(t, http.DegradedConfigResponse{
		Kind:  "database",
		ID:    "orders-warehouse",
		Error: "missing environment variable ${WAREHOUSE_PW}",
	}, body.DegradedConfigs[0])
	require.Contains(t, body.DegradedConfigs[0].Error, "${WAREHOUSE_PW}")
}

// TestReady_StaysReadyWhenConfigDegraded locks in the invariant from the
// node-status ticket: a degraded config must NOT make /ready return 503.
// Flipping /ready to unready over a node-local env gap would make the
// orchestrator pull the node from rotation — re-introducing the exact
// availability hit config-apply-decouple removed by degrading instead of
// crashing. /ready gates only on leader + applied index, never on the
// degraded set, so the diagnosis lives on the authenticated /node/status.
func TestReady_StaysReadyWhenConfigDegraded(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.LeaderReturns(1)
	fake.AppliedIndexReturns(7)
	fake.ConfigBuildErrorsReturns([]cluster.ConfigBuildError{
		{Kind: "database", ID: "orders-warehouse", Error: "missing environment variable ${WAREHOUSE_PW}"},
	})
	h := http.New(fake)

	req := httptest.NewRequest("GET", "http://localhost/ready", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode, "a degraded config must not make the node unready")

	bs, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var body http.ReadyResponse
	require.NoError(t, json.Unmarshal(bs, &body))
	require.Equal(t, "ok", body.Status)

	// /ready must not consult the degraded set at all.
	require.Equal(t, 0, fake.ConfigBuildErrorsCallCount())
}
