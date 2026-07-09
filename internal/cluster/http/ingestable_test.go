package http_test

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
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
