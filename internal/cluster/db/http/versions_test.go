package http_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
)

// --- GetVersions ---

func TestGetVersions_Success(t *testing.T) {
	versions := []cluster.VersionInfo{
		{Version: 1, Current: false},
		{Version: 2, Current: true},
	}

	tests := []struct {
		name    string
		path    string
		setupFn func(fake *clusterfakes.FakeCluster)
	}{
		{
			name:    "type",
			path:    "/v1/type/type-1/versions",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.TypeVersionsReturns(versions, nil) },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, fake := setupTest()
			tc.setupFn(fake)

			req := httptest.NewRequest("GET", "http://localhost"+tc.path, nil)
			w := httptest.NewRecorder()

			h.ServeHTTP(w, req)

			resp := w.Result()
			require.Equal(t, 200, resp.StatusCode)
			require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

			body, err := io.ReadAll(resp.Body)
			require.Nil(t, err)

			var result []cluster.VersionInfo
			err = json.Unmarshal(body, &result)
			require.Nil(t, err)
			require.Equal(t, 2, len(result))
			require.Equal(t, uint64(1), result[0].Version)
			require.False(t, result[0].Current)
			require.Equal(t, uint64(2), result[1].Version)
			require.True(t, result[1].Current)
		})
	}
}

func TestGetVersions_ResourceNotFound(t *testing.T) {
	tests := []struct {
		name         string
		path         string
		setupFn      func(fake *clusterfakes.FakeCluster)
		expectedCode string
	}{
		{
			name:         "type",
			path:         "/v1/type/missing/versions",
			setupFn:      func(fake *clusterfakes.FakeCluster) { fake.TypeVersionsReturns(nil, cluster.ErrResourceNotFound) },
			expectedCode: "type_not_found",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, fake := setupTest()
			tc.setupFn(fake)

			req := httptest.NewRequest("GET", "http://localhost"+tc.path, nil)
			w := httptest.NewRecorder()

			h.ServeHTTP(w, req)

			resp := w.Result()
			require.Equal(t, 404, resp.StatusCode)
			requireErrorResponse(t, resp, tc.expectedCode)
		})
	}
}

func TestGetVersions_InternalError(t *testing.T) {
	h, fake := setupTest()
	fake.TypeVersionsReturns(nil, fmt.Errorf("disk failure"))

	req := httptest.NewRequest("GET", "http://localhost/v1/type/type-1/versions", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 500, resp.StatusCode)
	requireErrorResponse(t, resp, "internal_error")
}

// --- GetVersion (specific version) ---

func TestGetVersion_Success(t *testing.T) {
	cfg := &cluster.Configuration{ID: "res-1", MimeType: "text/toml", Data: []byte("data-v1")}

	tests := []struct {
		name    string
		path    string
		setupFn func(fake *clusterfakes.FakeCluster)
	}{
		{
			name:    "type",
			path:    "/v1/type/res-1/versions/1",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.TypeVersionReturns(cfg, nil) },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, fake := setupTest()
			tc.setupFn(fake)

			req := httptest.NewRequest("GET", "http://localhost"+tc.path, nil)
			w := httptest.NewRecorder()

			h.ServeHTTP(w, req)

			resp := w.Result()
			require.Equal(t, 200, resp.StatusCode)

			body, err := io.ReadAll(resp.Body)
			require.Nil(t, err)

			var result configResponse
			err = json.Unmarshal(body, &result)
			require.Nil(t, err)
			require.Equal(t, "res-1", result.ID)
			require.Equal(t, "data-v1", result.Data)
		})
	}
}

// TestGetVersion_VersionNotFound: a real ingestable has one version; asking
// for 99 is the version-level 404, distinct from the resource-level one.
func TestGetVersion_VersionNotFound(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	e.addRecorderIngestable(t, "ing-1", "photos")
	requireEnvelope(t, e.doEmpty(t, "GET", "/v1/ingestable/ing-1/versions/99"), 404, "version_not_found")
}

func TestGetVersion_InvalidVersionParam(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{name: "non-numeric", path: "/v1/ingestable/ingest-1/versions/abc"},
		{name: "zero", path: "/v1/ingestable/ingest-1/versions/0"},
		{name: "negative", path: "/v1/ingestable/ingest-1/versions/-1"},
	}

	e := newEngine(t)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			requireEnvelope(t, e.doEmpty(t, "GET", tc.path), 400, "invalid_version")
		})
	}
}

// --- Rollback ---

// TestRollback_VersionNotFound: rolling a real ingestable back to a version
// that never existed is the version-level 404 through the real read.
func TestRollback_VersionNotFound(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	e.addRecorderIngestable(t, "ing-1", "photos")
	requireEnvelope(t, e.doEmpty(t, "POST", "/v1/ingestable/ing-1/rollback?to=99"), 404, "version_not_found")
}

func TestRollback_MissingToParam(t *testing.T) {
	h, _ := setupTest()

	req := httptest.NewRequest("POST", "http://localhost/v1/ingestable/ingest-1/rollback", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 400, resp.StatusCode)
	requireErrorResponse(t, resp, "missing_parameter")
}

func TestRollback_InvalidToParam(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{name: "non-numeric", path: "/v1/database/db-1/rollback?to=abc"},
		{name: "zero", path: "/v1/database/db-1/rollback?to=0"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := setupTest()

			req := httptest.NewRequest("POST", "http://localhost"+tc.path, nil)
			w := httptest.NewRecorder()

			h.ServeHTTP(w, req)

			resp := w.Result()
			require.Equal(t, 400, resp.StatusCode)
			requireErrorResponse(t, resp, "invalid_version")
		})
	}
}

// --- Type has no rollback endpoint ---

func TestTypeRollback_NoRoute(t *testing.T) {
	h, _ := setupTest()

	req := httptest.NewRequest("POST", "http://localhost/v1/type/type-1/rollback?to=1", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	// Chi returns 404 for unmatched routes (no POST /type/{id}/rollback registered).
	require.Equal(t, 404, resp.StatusCode)
}

// configResponse mirrors http.ConfigurationResponse for test unmarshaling.
type configResponse struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	MimeType string `json:"mimeType"`
	Data     string `json:"data"`
}
