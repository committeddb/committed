package http_test

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/http"
)

// TestDeleteIngestable threads the id through to the cluster and returns 200. The
// route is leader-pinned (leaderRead), so the fake reports itself as the leader
// to serve the delete locally (where the source teardown runs).
func TestDeleteIngestable(t *testing.T) {
	h, fake := setupTest()
	fake.IDReturns(1)
	fake.LeaderReturns(1)

	req := httptest.NewRequest("DELETE", "http://localhost/v1/ingestable/ing-1", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, 200, w.Code)
	require.Equal(t, 1, fake.DeleteIngestableCallCount())
	_, gotID := fake.DeleteIngestableArgsForCall(0)
	require.Equal(t, "ing-1", gotID)

	var body struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, "ing-1", body.ID)
}

// An empty id is a 400 and never reaches the cluster. (Chi won't route a bare
// /v1/ingestable/ with no id to this handler, but the guard is defensive.)
func TestDeleteIngestable_EmptyID(t *testing.T) {
	h, fake := setupTest()
	fake.IDReturns(1)
	fake.LeaderReturns(1)

	// A trailing-slash id is empty at the handler.
	req := httptest.NewRequest("DELETE", "http://localhost/v1/ingestable/", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.NotEqual(t, 200, w.Code)
	require.Equal(t, 0, fake.DeleteIngestableCallCount())
}

// The ingest twin of the syncable degraded-workerState fix: a config that
// exists but failed to BUILD on this node has no worker, so status answers
// ingestable_not_running — honest, but the generic message hides the cause.
// When the answering node's config-error record explains the absence, the
// 404 body must carry the (redacted) build error so the operator learns
// what to fix without hunting the node-status surface.
func TestGetIngestableStatus_NotRunningNamesDegradedCause(t *testing.T) {
	h, fake := setupTest()
	fake.IngestableStatusReturns(cluster.IngestableStatus{}, cluster.ErrIngestableNotRunning)
	fake.ConfigBuildErrorsReturns([]cluster.ConfigBuildError{
		{Kind: "syncable", ID: "ing-1", Error: "wrong kind — must not match"},
		{Kind: "ingestable", ID: "ing-1", Error: "interpolate: variable SOURCE_DSN not set"},
	})

	req := httptest.NewRequest("GET", "http://localhost/v1/ingestable/ing-1/status", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, 404, w.Code)
	require.Contains(t, w.Body.String(), "ingestable_not_running")
	require.Contains(t, w.Body.String(), "interpolate: variable SOURCE_DSN not set",
		"the not-running answer must name the degraded-build cause")

	// Without a degraded record the generic message stands.
	fake.ConfigBuildErrorsReturns(nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "http://localhost/v1/ingestable/ing-1/status", nil))
	require.Equal(t, 404, w.Code)
	require.Contains(t, w.Body.String(), "no ingestable worker is running")
}

// lagUnit names the scale lag is on — load-bearing now that MySQL reports
// transactions under GTID positioning but bytes under the file:pos fallback:
// present next to a non-null lag, omitted with a null one.
func TestGetIngestableStatus_LagUnit(t *testing.T) {
	h, fake := setupTest()
	lag := uint64(8192)
	fake.IngestableStatusReturns(cluster.IngestableStatus{
		WorkerState: cluster.WorkerStateRunning,
		Phase:       "streaming",
		Lag:         &lag,
		LagUnit:     cluster.LagUnitBytes,
	}, nil)

	req := httptest.NewRequest("GET", "http://localhost/v1/ingestable/ing-1/status", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
	require.Contains(t, w.Body.String(), `"lagUnit":"bytes"`)

	// Unknown lag: null lag, no unit — a unit without a number is noise.
	fake.IngestableStatusReturns(cluster.IngestableStatus{
		WorkerState: cluster.WorkerStateRunning,
		Phase:       "streaming",
	}, nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "http://localhost/v1/ingestable/ing-1/status", nil))
	require.Equal(t, 200, w.Code)
	require.Contains(t, w.Body.String(), `"lag":null`)
	require.False(t, strings.Contains(w.Body.String(), "lagUnit"),
		"lagUnit must be omitted when lag is null")
}

// TestGetIngestableStatus_Census pins the census section of the status
// payload: shapes in first-seen order, the derived path view, and a rendered
// draft schema — and its absence when no census has been published.
func TestGetIngestableStatus_Census(t *testing.T) {
	h, fake := setupTest()
	fake.IngestableStatusReturns(cluster.IngestableStatus{
		WorkerState: cluster.WorkerStateRunning,
		Phase:       "streaming",
	}, nil)

	tc := &cluster.TopicCensus{}
	require.NoError(t, tc.Fold([]byte(`{"caption":"a","size":1}`), cluster.CensusOptions{}))
	require.NoError(t, tc.Fold([]byte(`{"caption":"b","ai":{"m":"x"}}`), cluster.CensusOptions{}))
	fake.IngestableCensusReturns(&cluster.IngestableCensus{
		ID: "ing-1", RefreshEpoch: 1,
		Topics: map[string]*cluster.TopicCensus{"photos": tc},
	}, true)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "http://localhost/v1/ingestable/ing-1/status", nil))
	require.Equal(t, 200, w.Code)

	var resp http.IngestableStatusResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	photos := resp.Census["photos"]
	require.NotNil(t, photos)
	require.Equal(t, uint64(2), photos.Rows)
	require.Len(t, photos.Shapes, 2)
	require.LessOrEqual(t, photos.Shapes[0].FirstRow, photos.Shapes[1].FirstRow, "shapes render in first-seen order")
	require.NotEmpty(t, photos.Paths)
	require.Contains(t, photos.DraftSchema, `"additionalProperties": false`)
	require.Contains(t, photos.DraftSchema, `"ai"`)

	// No census published → the field is omitted entirely.
	fake.IngestableCensusReturns(nil, false)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "http://localhost/v1/ingestable/ing-1/status", nil))
	require.Equal(t, 200, w.Code)
	require.False(t, strings.Contains(w.Body.String(), "census"), "no census, no field")
}
