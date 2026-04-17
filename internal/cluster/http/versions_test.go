package http_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/clusterfakes"
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
			name:    "database",
			path:    "/database/db-1/versions",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.DatabaseVersionsReturns(versions, nil) },
		},
		{
			name:    "ingestable",
			path:    "/ingestable/ingest-1/versions",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.IngestableVersionsReturns(versions, nil) },
		},
		{
			name:    "syncable",
			path:    "/syncable/sync-1/versions",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.SyncableVersionsReturns(versions, nil) },
		},
		{
			name:    "type",
			path:    "/type/type-1/versions",
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
			name:         "database",
			path:         "/database/missing/versions",
			setupFn:      func(fake *clusterfakes.FakeCluster) { fake.DatabaseVersionsReturns(nil, cluster.ErrResourceNotFound) },
			expectedCode: "database_not_found",
		},
		{
			name:         "ingestable",
			path:         "/ingestable/missing/versions",
			setupFn:      func(fake *clusterfakes.FakeCluster) { fake.IngestableVersionsReturns(nil, cluster.ErrResourceNotFound) },
			expectedCode: "ingestable_not_found",
		},
		{
			name:         "syncable",
			path:         "/syncable/missing/versions",
			setupFn:      func(fake *clusterfakes.FakeCluster) { fake.SyncableVersionsReturns(nil, cluster.ErrResourceNotFound) },
			expectedCode: "syncable_not_found",
		},
		{
			name:         "type",
			path:         "/type/missing/versions",
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
	fake.DatabaseVersionsReturns(nil, fmt.Errorf("disk failure"))

	req := httptest.NewRequest("GET", "http://localhost/database/db-1/versions", nil)
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
			name:    "database",
			path:    "/database/res-1/versions/1",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.DatabaseVersionReturns(cfg, nil) },
		},
		{
			name:    "ingestable",
			path:    "/ingestable/res-1/versions/1",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.IngestableVersionReturns(cfg, nil) },
		},
		{
			name:    "syncable",
			path:    "/syncable/res-1/versions/1",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.SyncableVersionReturns(cfg, nil) },
		},
		{
			name:    "type",
			path:    "/type/res-1/versions/1",
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

			var result []configResponse
			err = json.Unmarshal(body, &result)
			require.Nil(t, err)
			require.Equal(t, 1, len(result))
			require.Equal(t, "res-1", result[0].ID)
			require.Equal(t, "data-v1", result[0].Data)
		})
	}
}

func TestGetVersion_VersionNotFound(t *testing.T) {
	h, fake := setupTest()
	fake.IngestableVersionReturns(nil, cluster.ErrVersionNotFound)

	req := httptest.NewRequest("GET", "http://localhost/ingestable/ingest-1/versions/99", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 404, resp.StatusCode)
	requireErrorResponse(t, resp, "version_not_found")
}

func TestGetVersion_InvalidVersionParam(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{name: "non-numeric", path: "/ingestable/ingest-1/versions/abc"},
		{name: "zero", path: "/ingestable/ingest-1/versions/0"},
		{name: "negative", path: "/ingestable/ingest-1/versions/-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := setupTest()

			req := httptest.NewRequest("GET", "http://localhost"+tc.path, nil)
			w := httptest.NewRecorder()

			h.ServeHTTP(w, req)

			resp := w.Result()
			require.Equal(t, 400, resp.StatusCode)
			requireErrorResponse(t, resp, "invalid_version")
		})
	}
}

// --- Rollback ---

func TestRollback_Success(t *testing.T) {
	cfg := &cluster.Configuration{ID: "res-1", MimeType: "text/toml", Data: []byte("old-config")}

	tests := []struct {
		name     string
		path     string
		setupFn  func(fake *clusterfakes.FakeCluster)
		verifyFn func(fake *clusterfakes.FakeCluster)
	}{
		{
			name: "database",
			path: "/database/res-1/rollback?to=1",
			setupFn: func(fake *clusterfakes.FakeCluster) {
				fake.DatabaseVersionReturns(cfg, nil)
			},
			verifyFn: func(fake *clusterfakes.FakeCluster) {
				require.Equal(t, 1, fake.ProposeDatabaseCallCount())
				_, proposed := fake.ProposeDatabaseArgsForCall(0)
				require.Equal(t, cfg, proposed)
			},
		},
		{
			name: "ingestable",
			path: "/ingestable/res-1/rollback?to=1",
			setupFn: func(fake *clusterfakes.FakeCluster) {
				fake.IngestableVersionReturns(cfg, nil)
			},
			verifyFn: func(fake *clusterfakes.FakeCluster) {
				require.Equal(t, 1, fake.ProposeIngestableCallCount())
				_, proposed := fake.ProposeIngestableArgsForCall(0)
				require.Equal(t, cfg, proposed)
			},
		},
		{
			name: "syncable",
			path: "/syncable/res-1/rollback?to=1",
			setupFn: func(fake *clusterfakes.FakeCluster) {
				fake.SyncableVersionReturns(cfg, nil)
			},
			verifyFn: func(fake *clusterfakes.FakeCluster) {
				require.Equal(t, 1, fake.ProposeSyncableCallCount())
				_, proposed := fake.ProposeSyncableArgsForCall(0)
				require.Equal(t, cfg, proposed)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, fake := setupTest()
			tc.setupFn(fake)

			req := httptest.NewRequest("POST", "http://localhost"+tc.path, nil)
			w := httptest.NewRecorder()

			h.ServeHTTP(w, req)

			resp := w.Result()
			require.Equal(t, 200, resp.StatusCode)

			body, err := io.ReadAll(resp.Body)
			require.Nil(t, err)
			require.Equal(t, "res-1", string(body))

			tc.verifyFn(fake)
		})
	}
}

func TestRollback_VersionNotFound(t *testing.T) {
	h, fake := setupTest()
	fake.IngestableVersionReturns(nil, cluster.ErrVersionNotFound)

	req := httptest.NewRequest("POST", "http://localhost/ingestable/ingest-1/rollback?to=99", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 404, resp.StatusCode)
	requireErrorResponse(t, resp, "version_not_found")
}

func TestRollback_ProposeError(t *testing.T) {
	cfg := &cluster.Configuration{ID: "sync-1", MimeType: "text/toml", Data: []byte("config")}

	h, fake := setupTest()
	fake.SyncableVersionReturns(cfg, nil)
	fake.ProposeSyncableReturns(fmt.Errorf("raft unavailable"))

	req := httptest.NewRequest("POST", "http://localhost/syncable/sync-1/rollback?to=1", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 500, resp.StatusCode)
	requireErrorResponse(t, resp, "internal_error")
}

func TestRollback_ProposeConfigError(t *testing.T) {
	cfg := &cluster.Configuration{ID: "db-1", MimeType: "text/toml", Data: []byte("config")}
	configErr := &cluster.ConfigError{Err: fmt.Errorf("bad config")}

	h, fake := setupTest()
	fake.DatabaseVersionReturns(cfg, nil)
	fake.ProposeDatabaseReturns(configErr)

	req := httptest.NewRequest("POST", "http://localhost/database/db-1/rollback?to=1", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 400, resp.StatusCode)
	requireErrorResponse(t, resp, "invalid_database_config")
}

func TestRollback_MissingToParam(t *testing.T) {
	h, _ := setupTest()

	req := httptest.NewRequest("POST", "http://localhost/ingestable/ingest-1/rollback", nil)
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
		{name: "non-numeric", path: "/database/db-1/rollback?to=abc"},
		{name: "zero", path: "/database/db-1/rollback?to=0"},
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

	req := httptest.NewRequest("POST", "http://localhost/type/type-1/rollback?to=1", nil)
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
