package http_test

import (
	"encoding/json"
	"fmt"
	"io"
	httpgo "net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/db/http"
)

func setupTest() (*http.HTTP, *clusterfakes.FakeCluster) {
	fake := &clusterfakes.FakeCluster{}
	// The status/errors endpoints 404 unknown ids via the existence oracle;
	// default the fake to "exists" so every test not ABOUT the 404 gate keeps
	// its focus (the gate's own tests stub false explicitly).
	fake.IngestableExistsReturns(true, nil)
	h := http.New(fake)
	return h, fake
}

// --- Add Configuration (table-driven for Database, Syncable, Ingestable, Type) ---

func TestAddConfiguration_Success(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		verifyFn func(fake *clusterfakes.FakeCluster) (int, *cluster.Configuration)
	}{
		{
			name: "database",
			path: "/v1/database/db-1",
			verifyFn: func(fake *clusterfakes.FakeCluster) (int, *cluster.Configuration) {
				_, cfg := fake.ProposeDatabaseArgsForCall(0)
				return fake.ProposeDatabaseCallCount(), cfg
			},
		},
		{
			name: "ingestable",
			path: "/v1/ingestable/ingest-1",
			verifyFn: func(fake *clusterfakes.FakeCluster) (int, *cluster.Configuration) {
				_, cfg := fake.ProposeIngestableArgsForCall(0)
				return fake.ProposeIngestableCallCount(), cfg
			},
		},
		{
			name: "type",
			path: "/v1/type/type-1",
			verifyFn: func(fake *clusterfakes.FakeCluster) (int, *cluster.Configuration) {
				_, cfg, _ := fake.ProposeTypeArgsForCall(0)
				return fake.ProposeTypeCallCount(), cfg
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, fake := setupTest()

			body := `[config]
name = "test"
type = "sql"`

			req := httptest.NewRequest("POST", "http://localhost"+tc.path, strings.NewReader(body))
			req.Header["Content-Type"] = []string{"text/toml"}
			w := httptest.NewRecorder()

			h.ServeHTTP(w, req)

			resp := w.Result()
			require.Equal(t, 200, resp.StatusCode)

			respBody, err := io.ReadAll(resp.Body)
			require.Nil(t, err)

			// ID is the last segment of the path
			parts := strings.Split(tc.path, "/")
			expectedID := parts[len(parts)-1]
			require.JSONEq(t, `{"id":"`+expectedID+`"}`, string(respBody))

			callCount, cfg := tc.verifyFn(fake)
			require.Equal(t, 1, callCount)
			require.Equal(t, expectedID, cfg.ID)
			require.Equal(t, "text/toml", cfg.MimeType)
			require.Equal(t, body, string(cfg.Data))
		})
	}
}

func TestAddConfiguration_ClusterError(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		setupFn func(fake *clusterfakes.FakeCluster)
	}{
		{
			name:    "database",
			path:    "/v1/database/db-1",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.ProposeDatabaseReturns(fmt.Errorf("fail")) },
		},
		{
			name:    "ingestable",
			path:    "/v1/ingestable/ingest-1",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.ProposeIngestableReturns(fmt.Errorf("fail")) },
		},
		{
			name:    "type",
			path:    "/v1/type/type-1",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.ProposeTypeReturns(fmt.Errorf("fail")) },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, fake := setupTest()
			tc.setupFn(fake)

			req := httptest.NewRequest("POST", "http://localhost"+tc.path, strings.NewReader("body"))
			w := httptest.NewRecorder()

			h.ServeHTTP(w, req)

			resp := w.Result()
			require.Equal(t, 500, resp.StatusCode)
			requireErrorResponse(t, resp, "internal_error")
		})
	}
}

func TestAddConfiguration_ConfigError(t *testing.T) {
	configErr := &cluster.ConfigError{Err: fmt.Errorf("bad toml")}

	tests := []struct {
		name         string
		path         string
		setupFn      func(fake *clusterfakes.FakeCluster)
		expectedCode string
	}{
		{
			name:         "database",
			path:         "/v1/database/db-1",
			setupFn:      func(fake *clusterfakes.FakeCluster) { fake.ProposeDatabaseReturns(configErr) },
			expectedCode: "invalid_database_config",
		},
		{
			name:         "ingestable",
			path:         "/v1/ingestable/ingest-1",
			setupFn:      func(fake *clusterfakes.FakeCluster) { fake.ProposeIngestableReturns(configErr) },
			expectedCode: "invalid_ingestable_config",
		},
		{
			name:         "type",
			path:         "/v1/type/type-1",
			setupFn:      func(fake *clusterfakes.FakeCluster) { fake.ProposeTypeReturns(configErr) },
			expectedCode: "invalid_type_config",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, fake := setupTest()
			tc.setupFn(fake)

			req := httptest.NewRequest("POST", "http://localhost"+tc.path, strings.NewReader("body"))
			w := httptest.NewRecorder()

			h.ServeHTTP(w, req)

			resp := w.Result()
			require.Equal(t, 400, resp.StatusCode)
			errResp := requireErrorResponse(t, resp, tc.expectedCode)
			require.Contains(t, errResp.Message, "bad toml", "response should include the underlying parse error")
		})
	}
}

func TestAddConfiguration_EmptyBody(t *testing.T) {
	h, _ := setupTest()

	req := httptest.NewRequest("POST", "http://localhost/v1/database/db-1", strings.NewReader(""))
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	require.Equal(t, 200, w.Result().StatusCode)
}

func TestAddConfiguration_ContentTypeHandling(t *testing.T) {
	t.Run("default mime type is text/toml", func(t *testing.T) {
		h, fake := setupTest()

		req := httptest.NewRequest("POST", "http://localhost/v1/database/db-1", strings.NewReader("data"))
		// No Content-Type header set
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		require.Equal(t, 200, w.Result().StatusCode)
		_, cfg := fake.ProposeDatabaseArgsForCall(0)
		require.Equal(t, "text/toml", cfg.MimeType)
	})

	t.Run("explicit application/json", func(t *testing.T) {
		h, fake := setupTest()

		req := httptest.NewRequest("POST", "http://localhost/v1/database/db-1", strings.NewReader("{}"))
		req.Header["Content-Type"] = []string{"application/json"}
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		require.Equal(t, 200, w.Result().StatusCode)
		_, cfg := fake.ProposeDatabaseArgsForCall(0)
		require.Equal(t, "application/json", cfg.MimeType)
	})
}

// --- Get Configurations (table-driven) ---

func TestGetConfigurations_Success(t *testing.T) {
	cfgs := []*cluster.Configuration{
		{ID: "id-1", Name: "name-1", MimeType: "text/toml", Data: []byte("data1")},
		{ID: "id-2", Name: "name-2", MimeType: "application/json", Data: []byte("data2")},
	}

	tests := []struct {
		name    string
		path    string
		setupFn func(fake *clusterfakes.FakeCluster)
	}{
		{
			name:    "database",
			path:    "/v1/database",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.DatabasesReturns(cfgs, nil) },
		},
		{
			name:    "ingestable",
			path:    "/v1/ingestable",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.IngestablesReturns(cfgs, nil) },
		},
		{
			name:    "type",
			path:    "/v1/type",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.TypesReturns(cfgs, nil) },
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

			var result []http.ConfigurationResponse
			err = json.Unmarshal(body, &result)
			require.Nil(t, err)
			require.Equal(t, 2, len(result))
			require.Equal(t, "id-1", result[0].ID)
			require.Equal(t, "name-1", result[0].Name)
			require.Equal(t, "data1", result[0].Data)
			require.Equal(t, "id-2", result[1].ID)
			require.Equal(t, "name-2", result[1].Name)
		})
	}
}

func TestGetConfigurations_Empty(t *testing.T) {
	h, fake := setupTest()
	fake.DatabasesReturns(nil, nil)

	req := httptest.NewRequest("GET", "http://localhost/v1/database", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.Nil(t, err)
	require.Equal(t, "[]", string(body))
}

func TestGetConfigurations_Error(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		setupFn func(fake *clusterfakes.FakeCluster)
	}{
		{
			name:    "database",
			path:    "/v1/database",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.DatabasesReturns(nil, fmt.Errorf("fail")) },
		},
		{
			name:    "syncable",
			path:    "/v1/syncable",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.SyncablesReturns(nil, fmt.Errorf("fail")) },
		},
		{
			name:    "ingestable",
			path:    "/v1/ingestable",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.IngestablesReturns(nil, fmt.Errorf("fail")) },
		},
		{
			name:    "type",
			path:    "/v1/type",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.TypesReturns(nil, fmt.Errorf("fail")) },
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
			require.Equal(t, 500, resp.StatusCode)
			requireErrorResponse(t, resp, "internal_error")
		})
	}
}

// --- AddProposal ---

// --- Error response helpers & shape test ---

// requireErrorResponse reads the response body, unmarshals it as an
// ErrorResponse, and asserts the code field matches expectedCode.
func requireErrorResponse(t *testing.T, resp *httpgo.Response, expectedCode string) http.ErrorResponse {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	require.Nil(t, err)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	var errResp http.ErrorResponse
	err = json.Unmarshal(body, &errResp)
	require.Nil(t, err, "response body is not valid ErrorResponse JSON: %s", string(body))
	require.Equal(t, expectedCode, errResp.Code)
	require.NotEmpty(t, errResp.Message)
	return errResp
}

func TestErrorResponse_JSONShape(t *testing.T) {
	h, fake := setupTest()
	fake.DatabaseVersionsReturns(nil, cluster.ErrResourceNotFound)

	req := httptest.NewRequest("GET", "http://localhost/v1/database/missing/versions", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 404, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	respBody, err := io.ReadAll(resp.Body)
	require.Nil(t, err)

	// Verify the JSON shape has exactly the expected fields
	var raw map[string]any
	err = json.Unmarshal(respBody, &raw)
	require.Nil(t, err)

	require.Contains(t, raw, "code")
	require.Contains(t, raw, "message")
	require.Equal(t, "database_not_found", raw["code"])
	require.IsType(t, "", raw["message"])
	require.NotEmpty(t, raw["message"])
}
