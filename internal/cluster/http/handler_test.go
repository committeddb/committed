package http_test

import (
	"encoding/json"
	"fmt"
	"io"
	httpgo "net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	"github.com/philborlin/committed/internal/cluster/http"
	"github.com/stretchr/testify/require"
)

func setupTest() (*http.HTTP, *clusterfakes.FakeCluster) {
	fake := &clusterfakes.FakeCluster{}
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
			path: "/database/db-1",
			verifyFn: func(fake *clusterfakes.FakeCluster) (int, *cluster.Configuration) {
				_, cfg := fake.ProposeDatabaseArgsForCall(0)
				return fake.ProposeDatabaseCallCount(), cfg
			},
		},
		{
			name: "syncable",
			path: "/syncable/sync-1",
			verifyFn: func(fake *clusterfakes.FakeCluster) (int, *cluster.Configuration) {
				_, cfg := fake.ProposeSyncableArgsForCall(0)
				return fake.ProposeSyncableCallCount(), cfg
			},
		},
		{
			name: "ingestable",
			path: "/ingestable/ingest-1",
			verifyFn: func(fake *clusterfakes.FakeCluster) (int, *cluster.Configuration) {
				_, cfg := fake.ProposeIngestableArgsForCall(0)
				return fake.ProposeIngestableCallCount(), cfg
			},
		},
		{
			name: "type",
			path: "/type/type-1",
			verifyFn: func(fake *clusterfakes.FakeCluster) (int, *cluster.Configuration) {
				_, cfg := fake.ProposeTypeArgsForCall(0)
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
			require.Equal(t, expectedID, string(respBody))

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
			path:    "/database/db-1",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.ProposeDatabaseReturns(fmt.Errorf("fail")) },
		},
		{
			name:    "syncable",
			path:    "/syncable/sync-1",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.ProposeSyncableReturns(fmt.Errorf("fail")) },
		},
		{
			name:    "ingestable",
			path:    "/ingestable/ingest-1",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.ProposeIngestableReturns(fmt.Errorf("fail")) },
		},
		{
			name:    "type",
			path:    "/type/type-1",
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

func TestAddConfiguration_EmptyBody(t *testing.T) {
	h, _ := setupTest()

	req := httptest.NewRequest("POST", "http://localhost/database/db-1", strings.NewReader(""))
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	require.Equal(t, 200, w.Result().StatusCode)
}

func TestAddConfiguration_ContentTypeHandling(t *testing.T) {
	t.Run("default mime type is text/toml", func(t *testing.T) {
		h, fake := setupTest()

		req := httptest.NewRequest("POST", "http://localhost/database/db-1", strings.NewReader("data"))
		// No Content-Type header set
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		require.Equal(t, 200, w.Result().StatusCode)
		_, cfg := fake.ProposeDatabaseArgsForCall(0)
		require.Equal(t, "text/toml", cfg.MimeType)
	})

	t.Run("explicit application/json", func(t *testing.T) {
		h, fake := setupTest()

		req := httptest.NewRequest("POST", "http://localhost/database/db-1", strings.NewReader("{}"))
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
		{ID: "id-1", MimeType: "text/toml", Data: []byte("data1")},
		{ID: "id-2", MimeType: "application/json", Data: []byte("data2")},
	}

	tests := []struct {
		name    string
		path    string
		setupFn func(fake *clusterfakes.FakeCluster)
	}{
		{
			name:    "database",
			path:    "/database",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.DatabasesReturns(cfgs, nil) },
		},
		{
			name:    "syncable",
			path:    "/syncable",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.SyncablesReturns(cfgs, nil) },
		},
		{
			name:    "ingestable",
			path:    "/ingestable",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.IngestablesReturns(cfgs, nil) },
		},
		{
			name:    "type",
			path:    "/type",
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
			require.Equal(t, "data1", result[0].Data)
			require.Equal(t, "id-2", result[1].ID)
		})
	}
}

func TestGetConfigurations_Empty(t *testing.T) {
	h, fake := setupTest()
	fake.DatabasesReturns(nil, nil)

	req := httptest.NewRequest("GET", "http://localhost/database", nil)
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
			path:    "/database",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.DatabasesReturns(nil, fmt.Errorf("fail")) },
		},
		{
			name:    "syncable",
			path:    "/syncable",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.SyncablesReturns(nil, fmt.Errorf("fail")) },
		},
		{
			name:    "ingestable",
			path:    "/ingestable",
			setupFn: func(fake *clusterfakes.FakeCluster) { fake.IngestablesReturns(nil, fmt.Errorf("fail")) },
		},
		{
			name:    "type",
			path:    "/type",
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

func TestAddProposal_Success(t *testing.T) {
	h, fake := setupTest()

	fake.TypeReturns(&cluster.Type{ID: "t1", Name: "TestType"}, nil)

	body := `{"entities": [{"typeId": "t1", "key": "k1", "data": {"foo": "bar"}}]}`
	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	require.Equal(t, 200, w.Result().StatusCode)
	require.Equal(t, 1, fake.TypeCallCount())
	require.Equal(t, "t1", fake.TypeArgsForCall(0))
	require.Equal(t, 1, fake.ProposeCallCount())

	_, p := fake.ProposeArgsForCall(0)
	require.Equal(t, 1, len(p.Entities))
	require.Equal(t, "t1", p.Entities[0].Type.ID)
	require.Equal(t, "k1", string(p.Entities[0].Key))
	require.JSONEq(t, `{"foo": "bar"}`, string(p.Entities[0].Data))
}

func TestAddProposal_MultipleEntities(t *testing.T) {
	h, fake := setupTest()

	fake.TypeReturnsOnCall(0, &cluster.Type{ID: "t1", Name: "Type1"}, nil)
	fake.TypeReturnsOnCall(1, &cluster.Type{ID: "t2", Name: "Type2"}, nil)

	body := `{"entities": [
		{"typeId": "t1", "key": "k1", "data": {"a": 1}},
		{"typeId": "t2", "key": "k2", "data": {"b": 2}}
	]}`
	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	require.Equal(t, 200, w.Result().StatusCode)
	require.Equal(t, 2, fake.TypeCallCount())
	require.Equal(t, 1, fake.ProposeCallCount())
	_, p := fake.ProposeArgsForCall(0)
	require.Equal(t, 2, len(p.Entities))
}

func TestAddProposal_BadJSON(t *testing.T) {
	h, _ := setupTest()

	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader("not json"))
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 400, resp.StatusCode)
	requireErrorResponse(t, resp, "invalid_json")
}

func TestAddProposal_TypeNotFound(t *testing.T) {
	h, fake := setupTest()
	fake.TypeReturns(nil, fmt.Errorf("type not found"))

	body := `{"entities": [{"typeId": "missing", "key": "k1", "data": {}}]}`
	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 400, resp.StatusCode)
	require.Equal(t, 0, fake.ProposeCallCount())
	requireErrorResponse(t, resp, "type_not_found")
}

func TestAddProposal_ProposeError(t *testing.T) {
	h, fake := setupTest()
	fake.TypeReturns(&cluster.Type{ID: "t1", Name: "T"}, nil)
	fake.ProposeReturns(fmt.Errorf("raft failed"))

	body := `{"entities": [{"typeId": "t1", "key": "k1", "data": {}}]}`
	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 500, resp.StatusCode)
	requireErrorResponse(t, resp, "internal_error")
}

// --- GetProposals ---

func TestGetProposals_Success(t *testing.T) {
	h, fake := setupTest()

	proposals := []*cluster.Proposal{
		{Entities: []*cluster.Entity{
			{Type: &cluster.Type{ID: "t1", Name: "Type1"}, Key: []byte("k1"), Data: []byte(`{"a":1}`)},
		}},
	}
	fake.ProposalsReturns(proposals, nil)

	req := httptest.NewRequest("GET", "http://localhost/proposal?type=Type1&number=5", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.Nil(t, err)

	var result []http.GetProposalResponse
	err = json.Unmarshal(body, &result)
	require.Nil(t, err)
	require.Equal(t, 1, len(result))
	require.Equal(t, 1, len(result[0].Entities))
	require.Equal(t, "t1", result[0].Entities[0].TypeID)
	require.Equal(t, "Type1", result[0].Entities[0].TypeName)
	require.Equal(t, "k1", result[0].Entities[0].Key)

	// Verify correct args passed to Proposals
	n, types := fake.ProposalsArgsForCall(0)
	require.Equal(t, uint64(5), n)
	require.Equal(t, []string{"Type1"}, types)
}

func TestGetProposals_DefaultNumber(t *testing.T) {
	h, fake := setupTest()
	fake.ProposalsReturns(nil, nil)

	req := httptest.NewRequest("GET", "http://localhost/proposal", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	require.Equal(t, 200, w.Result().StatusCode)
	n, _ := fake.ProposalsArgsForCall(0)
	require.Equal(t, uint64(10), n)
}

func TestGetProposals_WithTypeFilter(t *testing.T) {
	h, fake := setupTest()
	fake.ProposalsReturns(nil, nil)

	req := httptest.NewRequest("GET", "http://localhost/proposal?type=MyType", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	require.Equal(t, 200, w.Result().StatusCode)
	_, types := fake.ProposalsArgsForCall(0)
	require.Equal(t, []string{"MyType"}, types)
}

func TestGetProposals_InvalidNumber(t *testing.T) {
	h, fake := setupTest()
	fake.ProposalsReturns(nil, nil)

	req := httptest.NewRequest("GET", "http://localhost/proposal?number=abc", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 400, resp.StatusCode)
	require.Equal(t, 0, fake.ProposalsCallCount(), "Proposals should not be called with invalid number")
	requireErrorResponse(t, resp, "invalid_parameter")
}

func TestGetProposals_ClusterError(t *testing.T) {
	h, fake := setupTest()
	fake.ProposalsReturns(nil, fmt.Errorf("storage error"))

	req := httptest.NewRequest("GET", "http://localhost/proposal", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 500, resp.StatusCode)
	requireErrorResponse(t, resp, "internal_error")
}

// --- GetType (graph) ---

func TestGetType_Success(t *testing.T) {
	h, fake := setupTest()

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	points := []cluster.TimePoint{
		{Start: start, End: end, Value: 42},
	}
	fake.TypeGraphReturns(points, nil)

	req := httptest.NewRequest("GET", fmt.Sprintf(
		"http://localhost/type/mytype?start=%s&end=%s",
		start.Format("2006-01-02T15:04:05Z0700"),
		end.Format("2006-01-02T15:04:05Z0700"),
	), nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.Nil(t, err)

	var result []http.GetTypeGraphResponse
	err = json.Unmarshal(body, &result)
	require.Nil(t, err)
	require.Equal(t, 1, len(result))
	require.Equal(t, uint64(42), result[0].Value)

	// Verify args
	id, s, e := fake.TypeGraphArgsForCall(0)
	require.Equal(t, "mytype", id)
	require.Equal(t, start, s)
	require.Equal(t, end, e)
}

func TestGetType_MissingStartParam(t *testing.T) {
	h, _ := setupTest()

	req := httptest.NewRequest("GET", "http://localhost/type/mytype?end=2024-01-02T00:00:00Z", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 400, resp.StatusCode)
	requireErrorResponse(t, resp, "invalid_parameter")
}

func TestGetType_BadEndFormat(t *testing.T) {
	h, _ := setupTest()

	req := httptest.NewRequest("GET", "http://localhost/type/mytype?start=2024-01-01T00:00:00Z&end=not-a-date", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 400, resp.StatusCode)
	requireErrorResponse(t, resp, "invalid_parameter")
}

func TestGetType_ClusterError(t *testing.T) {
	h, fake := setupTest()
	fake.TypeGraphReturns(nil, fmt.Errorf("graph error"))

	req := httptest.NewRequest("GET", "http://localhost/type/mytype?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 500, resp.StatusCode)
	requireErrorResponse(t, resp, "internal_error")
}

func TestGetType_EmptyResult(t *testing.T) {
	h, fake := setupTest()
	fake.TypeGraphReturns(nil, nil)

	req := httptest.NewRequest("GET", "http://localhost/type/mytype?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.Nil(t, err)
	require.Equal(t, "[]", string(body))
}

// --- Error response helpers & shape test ---

// requireErrorResponse reads the response body, unmarshals it as an
// ErrorResponse, and asserts the code field matches expectedCode.
func requireErrorResponse(t *testing.T, resp *httpgo.Response, expectedCode string) {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	require.Nil(t, err)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	var errResp http.ErrorResponse
	err = json.Unmarshal(body, &errResp)
	require.Nil(t, err, "response body is not valid ErrorResponse JSON: %s", string(body))
	require.Equal(t, expectedCode, errResp.Code)
	require.NotEmpty(t, errResp.Message)
}

func TestErrorResponse_JSONShape(t *testing.T) {
	h, fake := setupTest()
	fake.TypeReturns(nil, fmt.Errorf("not found"))

	body := `{"entities": [{"typeId": "missing", "key": "k1", "data": {}}]}`
	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 400, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	respBody, err := io.ReadAll(resp.Body)
	require.Nil(t, err)

	// Verify the JSON shape has exactly the expected fields
	var raw map[string]any
	err = json.Unmarshal(respBody, &raw)
	require.Nil(t, err)

	require.Contains(t, raw, "code")
	require.Contains(t, raw, "message")
	require.Equal(t, "type_not_found", raw["code"])
	require.IsType(t, "", raw["message"])
	require.NotEmpty(t, raw["message"])
}
