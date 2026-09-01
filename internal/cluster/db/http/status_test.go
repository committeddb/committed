package http_test

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/http"
)

func doNodeStatus(t *testing.T, e *engine) (int, http.NodeStatusResponse) {
	t.Helper()
	w := e.doEmpty(t, "GET", "/v1/node/status")
	require.Equal(t, "application/json", w.Result().Header.Get("Content-Type"))
	var body http.NodeStatusResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	return w.Code, body
}

// TestNodeStatus_Healthy: a real, healthy single-node engine reports its
// raft identity, an empty (non-null) degradedConfigs array, a resting scrub
// block, and a disk block. The empty-array guarantee matters: a JSON null
// would force every client to special-case it.
func TestNodeStatus_Healthy(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos") // advance the applied index past boot

	status, body := doNodeStatus(t, e)

	require.Equal(t, 200, status)
	require.Equal(t, uint64(1), body.Node)
	require.Equal(t, uint64(1), body.Leader)
	require.NotZero(t, body.AppliedIndex)
	require.NotNil(t, body.DegradedConfigs, "degradedConfigs must be [] not null")
	require.Empty(t, body.DegradedConfigs)
	require.False(t, body.SafeMode, "a normal boot must not report safe mode")
	require.Zero(t, body.Scrub.PendingDeleteKeyErasures, "nothing pending on a fresh log")
	require.NotEmpty(t, body.Disk.Admission.State, "the disk admission block is always present")
}

// TestNodeStatus_SafeMode: a node booted with safe mode reports it — the
// operator's confirmation that workers are deliberately held, not
// mysteriously absent.
func TestNodeStatus_SafeMode(t *testing.T) {
	e := newEngineOpts(t, db.WithSafeMode())

	status, body := doNodeStatus(t, e)

	require.Equal(t, 200, status)
	require.True(t, body.SafeMode)
}

// TestReady_StaysReadyWhenConfigDegraded locks in the invariant from the
// node-status ticket: a degraded config must NOT make /ready return 503.
// Flipping /ready to unready over a node-local env gap would make the
// orchestrator pull the node from rotation — re-introducing the exact
// availability hit config-apply-decouple removed by degrading instead of
// crashing. /ready gates only on leader + applied index, never on the
// degraded set, so the diagnosis lives on the authenticated /node/status.
// (health.go is not yet engine-backed, so this drives the fake directly.)
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
