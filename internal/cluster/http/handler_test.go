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
			path:         "/database/db-1",
			setupFn:      func(fake *clusterfakes.FakeCluster) { fake.ProposeDatabaseReturns(configErr) },
			expectedCode: "invalid_database_config",
		},
		{
			name:         "syncable",
			path:         "/syncable/sync-1",
			setupFn:      func(fake *clusterfakes.FakeCluster) { fake.ProposeSyncableReturns(configErr) },
			expectedCode: "invalid_syncable_config",
		},
		{
			name:         "ingestable",
			path:         "/ingestable/ingest-1",
			setupFn:      func(fake *clusterfakes.FakeCluster) { fake.ProposeIngestableReturns(configErr) },
			expectedCode: "invalid_ingestable_config",
		},
		{
			name:         "type",
			path:         "/type/type-1",
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

	fake.ResolveTypeReturns(&cluster.Type{ID: "t1", Name: "TestType"}, nil)

	body := `{"entities": [{"typeId": "t1", "key": "k1", "data": {"foo": "bar"}}]}`
	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	require.Equal(t, 200, w.Result().StatusCode)
	require.Equal(t, 1, fake.ResolveTypeCallCount())
	require.Equal(t, cluster.LatestTypeRef("t1"), fake.ResolveTypeArgsForCall(0))
	require.Equal(t, 1, fake.ProposeCallCount())

	_, p := fake.ProposeArgsForCall(0)
	require.Equal(t, 1, len(p.Entities))
	require.Equal(t, "t1", p.Entities[0].Type.ID)
	require.Equal(t, "k1", string(p.Entities[0].Key))
	require.JSONEq(t, `{"foo": "bar"}`, string(p.Entities[0].Data))
}

func TestAddProposal_MultipleEntities(t *testing.T) {
	h, fake := setupTest()

	fake.ResolveTypeReturnsOnCall(0, &cluster.Type{ID: "t1", Name: "Type1"}, nil)
	fake.ResolveTypeReturnsOnCall(1, &cluster.Type{ID: "t2", Name: "Type2"}, nil)

	body := `{"entities": [
		{"typeId": "t1", "key": "k1", "data": {"a": 1}},
		{"typeId": "t2", "key": "k2", "data": {"b": 2}}
	]}`
	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	require.Equal(t, 200, w.Result().StatusCode)
	require.Equal(t, 2, fake.ResolveTypeCallCount())
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
	fake.ResolveTypeReturns(nil, fmt.Errorf("type not found"))

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
	fake.ResolveTypeReturns(&cluster.Type{ID: "t1", Name: "T"}, nil)
	fake.ProposeReturns(fmt.Errorf("raft failed"))

	body := `{"entities": [{"typeId": "t1", "key": "k1", "data": {}}]}`
	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 500, resp.StatusCode)
	requireErrorResponse(t, resp, "internal_error")
}

// --- AddProposal schema validation ---

func TestAddProposal_SchemaValidation_Valid(t *testing.T) {
	h, fake := setupTest()

	schema := []byte(`{
		"type": "object",
		"properties": {
			"name": {"type": "string"},
			"age":  {"type": "integer"}
		},
		"required": ["name"]
	}`)

	fake.ResolveTypeReturns(&cluster.Type{
		ID:         "t1",
		Name:       "Person",
		SchemaType: "JSONSchema",
		Schema:     schema,
		Validate:   cluster.ValidateSchema,
	}, nil)

	body := `{"entities": [{"typeId": "t1", "key": "k1", "data": {"name": "Alice", "age": 30}}]}`
	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	require.Equal(t, 200, w.Result().StatusCode)
	require.Equal(t, 1, fake.ProposeCallCount())
}

func TestAddProposal_SchemaValidation_Invalid(t *testing.T) {
	h, fake := setupTest()

	schema := []byte(`{
		"type": "object",
		"properties": {
			"name": {"type": "string"},
			"age":  {"type": "integer"}
		},
		"required": ["name"]
	}`)

	fake.ResolveTypeReturns(&cluster.Type{
		ID:         "t1",
		Name:       "Person",
		SchemaType: "JSONSchema",
		Schema:     schema,
		Validate:   cluster.ValidateSchema,
	}, nil)

	// Missing required "name" field
	body := `{"entities": [{"typeId": "t1", "key": "k1", "data": {"age": 30}}]}`
	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 400, resp.StatusCode)
	require.Equal(t, 0, fake.ProposeCallCount())
	errResp := requireErrorResponse(t, resp, "schema_validation_failed")
	require.NotNil(t, errResp.Details)
}

func TestAddProposal_SchemaValidation_WrongType(t *testing.T) {
	h, fake := setupTest()

	schema := []byte(`{
		"type": "object",
		"properties": {
			"age": {"type": "integer"}
		}
	}`)

	fake.ResolveTypeReturns(&cluster.Type{
		ID:         "t1",
		Name:       "Person",
		SchemaType: "JSONSchema",
		Schema:     schema,
		Validate:   cluster.ValidateSchema,
	}, nil)

	// "age" should be integer, not string
	body := `{"entities": [{"typeId": "t1", "key": "k1", "data": {"age": "not a number"}}]}`
	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 400, resp.StatusCode)
	require.Equal(t, 0, fake.ProposeCallCount())
	requireErrorResponse(t, resp, "schema_validation_failed")
}

func TestAddProposal_SchemaValidation_NoValidation(t *testing.T) {
	h, fake := setupTest()

	// Type has a schema but Validate == NoValidation — should skip validation
	schema := []byte(`{"type": "object", "required": ["name"]}`)
	fake.ResolveTypeReturns(&cluster.Type{
		ID:         "t1",
		Name:       "Loose",
		SchemaType: "JSONSchema",
		Schema:     schema,
		Validate:   cluster.NoValidation,
	}, nil)

	// Missing required "name" — should still pass because validation is off
	body := `{"entities": [{"typeId": "t1", "key": "k1", "data": {"other": 1}}]}`
	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	require.Equal(t, 200, w.Result().StatusCode)
	require.Equal(t, 1, fake.ProposeCallCount())
}

// --- Protobuf validation ---

// protoSourcePerson is a minimal .proto source used by the Protobuf
// validation tests. The message name (`Person`) matches the Type.Name,
// which is what compileProtobufValidator uses to pick the root message
// when a .proto file declares multiple messages.
const protoSourcePerson = `syntax = "proto3";
message Person {
    string name = 1;
    int32 age = 2;
}
`

func TestAddProposal_ProtobufValidation_Valid(t *testing.T) {
	h, fake := setupTest()

	fake.ResolveTypeReturns(&cluster.Type{
		ID:         "t1",
		Name:       "Person",
		SchemaType: "Protobuf",
		Schema:     []byte(protoSourcePerson),
		Validate:   cluster.ValidateSchema,
	}, nil)

	body := `{"entities": [{"typeId": "t1", "key": "k1", "data": {"name": "Alice", "age": 30}}]}`
	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, 200, w.Result().StatusCode)
	require.Equal(t, 1, fake.ProposeCallCount())
}

func TestAddProposal_ProtobufValidation_WrongFieldType(t *testing.T) {
	h, fake := setupTest()

	fake.ResolveTypeReturns(&cluster.Type{
		ID:         "t1",
		Name:       "Person",
		SchemaType: "Protobuf",
		Schema:     []byte(protoSourcePerson),
		Validate:   cluster.ValidateSchema,
	}, nil)

	// age is declared int32; a string is a protojson type mismatch.
	body := `{"entities": [{"typeId": "t1", "key": "k1", "data": {"name": "Alice", "age": "thirty"}}]}`
	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 400, resp.StatusCode)
	require.Equal(t, 0, fake.ProposeCallCount())
	requireErrorResponse(t, resp, "schema_validation_failed")
}

func TestAddProposal_ProtobufValidation_UnknownField(t *testing.T) {
	h, fake := setupTest()

	fake.ResolveTypeReturns(&cluster.Type{
		ID:         "t1",
		Name:       "Person",
		SchemaType: "Protobuf",
		Schema:     []byte(protoSourcePerson),
		Validate:   cluster.ValidateSchema,
	}, nil)

	// `email` is not declared on Person. protojson.Unmarshal rejects
	// unknown fields by default (DiscardUnknown: false).
	body := `{"entities": [{"typeId": "t1", "key": "k1", "data": {"name": "Alice", "email": "a@x.com"}}]}`
	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 400, resp.StatusCode)
	require.Equal(t, 0, fake.ProposeCallCount())
	requireErrorResponse(t, resp, "schema_validation_failed")
}

func TestAddProposal_ProtobufValidation_BadProtoSource(t *testing.T) {
	h, fake := setupTest()

	// Schema is not valid .proto syntax. Compilation should fail, which
	// surfaces as a 500 (internal_error) at proposal time.
	fake.ResolveTypeReturns(&cluster.Type{
		ID:         "t1",
		Name:       "Person",
		SchemaType: "Protobuf",
		Schema:     []byte("this is not proto source {{{"),
		Validate:   cluster.ValidateSchema,
	}, nil)

	body := `{"entities": [{"typeId": "t1", "key": "k1", "data": {"name": "Alice"}}]}`
	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 500, resp.StatusCode)
	require.Equal(t, 0, fake.ProposeCallCount())
	requireErrorResponse(t, resp, "internal_error")
}

func TestAddProposal_ProtobufValidation_MissingMessageName(t *testing.T) {
	h, fake := setupTest()

	// .proto compiles fine but declares no `message Person`. The
	// validator surfaces this as a compile failure -> 500.
	fake.ResolveTypeReturns(&cluster.Type{
		ID:         "t1",
		Name:       "Person",
		SchemaType: "Protobuf",
		Schema:     []byte(`syntax = "proto3"; message Other { string x = 1; }`),
		Validate:   cluster.ValidateSchema,
	}, nil)

	body := `{"entities": [{"typeId": "t1", "key": "k1", "data": {"name": "Alice"}}]}`
	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 500, resp.StatusCode)
	require.Equal(t, 0, fake.ProposeCallCount())
	requireErrorResponse(t, resp, "internal_error")
}

// The same (typeID, version) compiles once and is served from cache.
// This asserts that a second proposal against the same type doesn't
// trigger a fresh compile — verified indirectly by re-using a type
// (which returns the cached validator). A direct assertion on the
// compile-count isn't practical without plumbing a counter through the
// compiler, so this test just confirms functional correctness on the
// second call.
// A .proto with a `package` statement qualifies the message name. The
// validator should still find it by the short name in Type.Name.
func TestAddProposal_ProtobufValidation_WithPackage(t *testing.T) {
	h, fake := setupTest()

	src := `syntax = "proto3";
package app.v1;
message Person {
    string name = 1;
}
`
	fake.ResolveTypeReturns(&cluster.Type{
		ID:         "t1",
		Name:       "Person",
		SchemaType: "Protobuf",
		Schema:     []byte(src),
		Validate:   cluster.ValidateSchema,
	}, nil)

	body := `{"entities": [{"typeId": "t1", "key": "k1", "data": {"name": "Alice"}}]}`
	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, 200, w.Result().StatusCode)
}

func TestAddProposal_ProtobufValidation_SameVersionCached(t *testing.T) {
	h, fake := setupTest()

	fake.ResolveTypeReturns(&cluster.Type{
		ID:         "t1",
		Name:       "Person",
		Version:    3,
		SchemaType: "Protobuf",
		Schema:     []byte(protoSourcePerson),
		Validate:   cluster.ValidateSchema,
	}, nil)

	body := `{"entities": [{"typeId": "t1", "key": "k1", "data": {"name": "Alice"}}]}`
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		require.Equal(t, 200, w.Result().StatusCode, "iteration %d", i)
	}
	require.Equal(t, 3, fake.ProposeCallCount())
}

func TestAddProposal_SchemaValidation_UnknownSchemaType(t *testing.T) {
	h, fake := setupTest()

	// An unrecognized SchemaType currently fails-open (the type is
	// proposed without validation) per proposal-validation.md's "do not
	// fail-closed for unknown schema types" guidance. JSONSchema and
	// Protobuf have real validators; Thrift is the unimplemented one
	// that exercises this branch.
	fake.ResolveTypeReturns(&cluster.Type{
		ID:         "t1",
		Name:       "Thing",
		SchemaType: "Thrift",
		Schema:     []byte("not a real schema"),
		Validate:   cluster.ValidateSchema,
	}, nil)

	body := `{"entities": [{"typeId": "t1", "key": "k1", "data": {"anything": true}}]}`
	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	require.Equal(t, 200, w.Result().StatusCode)
	require.Equal(t, 1, fake.ProposeCallCount())
}

func TestAddProposal_SchemaValidation_EmptySchema(t *testing.T) {
	h, fake := setupTest()

	// ValidateSchema + JSONSchema but no actual schema content — should 500
	fake.ResolveTypeReturns(&cluster.Type{
		ID:         "t1",
		Name:       "Bad",
		SchemaType: "JSONSchema",
		Schema:     nil,
		Validate:   cluster.ValidateSchema,
	}, nil)

	body := `{"entities": [{"typeId": "t1", "key": "k1", "data": {"a": 1}}]}`
	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 500, resp.StatusCode)
	require.Equal(t, 0, fake.ProposeCallCount())
	requireErrorResponse(t, resp, "internal_error")
}

func TestAddProposal_SchemaValidation_InvalidSchemaJSON(t *testing.T) {
	h, fake := setupTest()

	// ValidateSchema + JSONSchema but schema is not valid JSON
	fake.ResolveTypeReturns(&cluster.Type{
		ID:         "t1",
		Name:       "Bad",
		SchemaType: "JSONSchema",
		Schema:     []byte("not valid json {{{"),
		Validate:   cluster.ValidateSchema,
	}, nil)

	body := `{"entities": [{"typeId": "t1", "key": "k1", "data": {"a": 1}}]}`
	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 500, resp.StatusCode)
	require.Equal(t, 0, fake.ProposeCallCount())
	requireErrorResponse(t, resp, "internal_error")
}

func TestAddProposal_SchemaValidation_EmptySchemaType(t *testing.T) {
	h, fake := setupTest()

	// ValidateSchema but SchemaType is empty — validation should be skipped
	fake.ResolveTypeReturns(&cluster.Type{
		ID:       "t1",
		Name:     "Legacy",
		Schema:   []byte(`{"type":"object"}`),
		Validate: cluster.ValidateSchema,
	}, nil)

	body := `{"entities": [{"typeId": "t1", "key": "k1", "data": {"a": 1}}]}`
	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	require.Equal(t, 200, w.Result().StatusCode)
	require.Equal(t, 1, fake.ProposeCallCount())
}

// AddProposal attaches the resolved Type (including its Version) to
// every entity. Marshal reads Version off the Type for the wire stamp,
// so covering the attached Type here transitively covers the wire
// behavior too.
func TestAddProposal_AttachesResolvedType(t *testing.T) {
	h, fake := setupTest()
	fake.ResolveTypeReturns(&cluster.Type{ID: "t1", Name: "Person", Version: 7}, nil)

	body := `{"entities": [{"typeId": "t1", "key": "k1", "data": {"a": 1}}]}`
	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, 200, w.Result().StatusCode)
	require.Equal(t, 1, fake.ProposeCallCount())

	_, p := fake.ProposeArgsForCall(0)
	require.Len(t, p.Entities, 1)
	require.Equal(t, 7, p.Entities[0].Type.Version, "entity must carry the resolved Type.Version")
}

func TestAddProposal_SchemaValidation_CacheInvalidatesOnVersionBump(t *testing.T) {
	h, fake := setupTest()

	// First schema: requires "name". Version 1 (the system bumps Version
	// on every schema change).
	schema1 := []byte(`{"type":"object","required":["name"],"properties":{"name":{"type":"string"}}}`)
	fake.ResolveTypeReturnsOnCall(0, &cluster.Type{
		ID: "t1", Name: "Person", Version: 1, SchemaType: "JSONSchema",
		Schema: schema1, Validate: cluster.ValidateSchema,
	}, nil)

	// Proposal with "name" — should pass against schema1
	body := `{"entities": [{"typeId": "t1", "key": "k1", "data": {"name": "Alice"}}]}`
	req := httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, 200, w.Result().StatusCode)

	// Schema evolves: now requires "email" instead of "name". A real
	// ProposeType would bump Version to 2 — the fake mirrors that so the
	// cache key (typeID, version) misses and the new schema is compiled.
	schema2 := []byte(`{"type":"object","required":["email"],"properties":{"email":{"type":"string"}}}`)
	fake.ResolveTypeReturnsOnCall(1, &cluster.Type{
		ID: "t1", Name: "Person", Version: 2, SchemaType: "JSONSchema",
		Schema: schema2, Validate: cluster.ValidateSchema,
	}, nil)

	// Same proposal with "name" but no "email" — must fail against schema2.
	body = `{"entities": [{"typeId": "t1", "key": "k2", "data": {"name": "Bob"}}]}`
	req = httptest.NewRequest("POST", "http://localhost/proposal", strings.NewReader(body))
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, 400, w.Result().StatusCode)
	requireErrorResponse(t, w.Result(), "schema_validation_failed")
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
	fake.ResolveTypeReturns(nil, fmt.Errorf("not found"))

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
