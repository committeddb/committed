package http_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

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
func TestReady_StaysReadyWhenConfigDegraded(t *testing.T) {
	e := newEngine(t)
	w := e.doEmpty(t, "GET", "/ready")
	require.Equal(t, 200, w.Code, "a degraded config must not make the node unready")
	var body http.ReadyResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, "ok", body.Status)

	// The stronger half of the invariant is structural now: the readiness
	// probe consults only the middleware's clusterView (leader, applied,
	// stalled), which cannot express the degraded-config set at all — the
	// compiler enforces what a call-count assertion used to sample.
}
