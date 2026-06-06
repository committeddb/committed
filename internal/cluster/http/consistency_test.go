package http_test

import (
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/http"
)

// doGET issues a GET against a fresh handler and returns the recorder plus the
// fake so callers can assert both the response and what the handler called.
func doGET(t *testing.T, path string, setup func(*clusterfakes.FakeCluster)) (*httptest.ResponseRecorder, *clusterfakes.FakeCluster) {
	t.Helper()
	h, fake := setupTest()
	if setup != nil {
		setup(fake)
	}
	req := httptest.NewRequest("GET", "http://localhost"+path, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w, fake
}

// TestLinearize_DefaultRunsReadIndex: a GET with no consistency parameter runs
// a linearizable read before serving, and succeeds when it confirms.
func TestLinearize_DefaultRunsReadIndex(t *testing.T) {
	w, fake := doGET(t, "/v1/database", nil)
	require.Equal(t, 200, w.Result().StatusCode)
	require.Equal(t, 1, fake.LinearizableReadCallCount(),
		"a default GET must confirm a linearizable read before serving")
	require.Equal(t, 1, fake.DatabasesCallCount())
}

// TestLinearize_StaleSkipsReadIndex: ?consistency=stale serves local state with
// no quorum round-trip.
func TestLinearize_StaleSkipsReadIndex(t *testing.T) {
	w, fake := doGET(t, "/v1/database?consistency=stale", nil)
	require.Equal(t, 200, w.Result().StatusCode)
	require.Equal(t, 0, fake.LinearizableReadCallCount(),
		"?consistency=stale must skip the linearizable read")
	require.Equal(t, 1, fake.DatabasesCallCount(), "a stale read still serves")
}

// TestLinearize_LinearizableExplicit: the explicit default value behaves like
// the absent default.
func TestLinearize_LinearizableExplicit(t *testing.T) {
	w, fake := doGET(t, "/v1/database?consistency=linearizable", nil)
	require.Equal(t, 200, w.Result().StatusCode)
	require.Equal(t, 1, fake.LinearizableReadCallCount())
}

// TestLinearize_ReadIndexFailureReturns503: when the node can't confirm a
// linearizable read (no quorum / partitioned), the GET returns 503 and never
// reads local state.
func TestLinearize_ReadIndexFailureReturns503(t *testing.T) {
	w, fake := doGET(t, "/v1/database", func(f *clusterfakes.FakeCluster) {
		f.LinearizableReadReturns(errors.New("no quorum confirmed"))
	})
	resp := w.Result()
	require.Equal(t, 503, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	var er http.ErrorResponse
	require.NoError(t, json.Unmarshal(body, &er))
	require.Equal(t, "not_linearizable", er.Code)
	require.Equal(t, 0, fake.DatabasesCallCount(),
		"a failed linearizable read must not fall through to a local read")
}

// TestLinearize_BadConsistencyValueReturns400: an unrecognized consistency
// value is rejected before any read happens.
func TestLinearize_BadConsistencyValueReturns400(t *testing.T) {
	w, fake := doGET(t, "/v1/database?consistency=eventually", nil)
	resp := w.Result()
	require.Equal(t, 400, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	var er http.ErrorResponse
	require.NoError(t, json.Unmarshal(body, &er))
	require.Equal(t, "invalid_consistency", er.Code)
	require.Equal(t, 0, fake.LinearizableReadCallCount())
	require.Equal(t, 0, fake.DatabasesCallCount())
}

// TestLinearize_AppliesToVersionAndSyncableReads: the gate isn't database-
// specific — version history and syncable status reads honour it too.
func TestLinearize_AppliesToVersionAndSyncableReads(t *testing.T) {
	t.Run("versions default runs read-index", func(t *testing.T) {
		w, fake := doGET(t, "/v1/database/db-1/versions", nil)
		require.Equal(t, 200, w.Result().StatusCode)
		require.Equal(t, 1, fake.LinearizableReadCallCount())
	})

	t.Run("syncable status stale skips read-index", func(t *testing.T) {
		w, fake := doGET(t, "/v1/syncable/s1/status?consistency=stale", nil)
		require.Equal(t, 200, w.Result().StatusCode)
		require.Equal(t, 0, fake.LinearizableReadCallCount())
	})
}
