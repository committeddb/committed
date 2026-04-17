package http_test

import (
	"fmt"
	"io"
	httpgo "net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/pb33f/libopenapi"
	validator "github.com/pb33f/libopenapi-validator"
	valerrors "github.com/pb33f/libopenapi-validator/errors"
	"github.com/stretchr/testify/require"

	openapiapi "github.com/philborlin/committed/api"
	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	httppkg "github.com/philborlin/committed/internal/cluster/http"
)

// newValidator parses the embedded spec and constructs a validator. Test
// fails here if the spec is malformed or invalid OpenAPI.
func newValidator(t *testing.T) (libopenapi.Document, validator.Validator) {
	t.Helper()

	doc, err := libopenapi.NewDocument(openapiapi.OpenAPISpec)
	require.NoError(t, err, "spec must parse")

	v, setupErrs := validator.NewValidator(doc)
	require.Empty(t, setupErrs, "validator must build from spec")

	valid, specErrs := v.ValidateDocument()
	if !valid {
		t.Fatalf("spec failed validation: %s", formatValidationErrors(specErrs))
	}
	return doc, v
}

func formatValidationErrors(errs []*valerrors.ValidationError) string {
	if len(errs) == 0 {
		return ""
	}
	var b strings.Builder
	for i, e := range errs {
		if i > 0 {
			b.WriteString("; ")
		}
		fmt.Fprintf(&b, "[%s] %s", e.ValidationType, e.Message)
		if e.Reason != "" {
			fmt.Fprintf(&b, " (%s)", e.Reason)
		}
	}
	return b.String()
}

// sampleConfig returns a non-empty Configuration suitable as a fake
// return value for version/list handlers.
func sampleConfig(id string) *cluster.Configuration {
	return &cluster.Configuration{
		ID:       id,
		MimeType: "text/toml",
		Data:     []byte("[config]\nvalue = \"x\""),
	}
}

// sampleVersions returns a non-empty VersionInfo slice.
func sampleVersions() []cluster.VersionInfo {
	return []cluster.VersionInfo{
		{Version: 2, Current: true},
		{Version: 1, Current: false},
	}
}

// sampleType returns a Type record with schema validation disabled so
// the proposal handler won't try to parse its Schema field.
func sampleType() *cluster.Type {
	return &cluster.Type{
		ID:       "t-1",
		Name:     "TestType",
		Version:  1,
		Validate: cluster.NoValidation,
	}
}

// contractCase is one row of the table. `setup` configures the fake so
// the handler can return a valid response. `body` is the request body
// (only meaningful for POST). `contentType` defaults to application/json
// when empty; set to text/toml for configuration POSTs.
type contractCase struct {
	name        string
	method      string
	path        string
	body        string
	contentType string
	setup       func(fake *clusterfakes.FakeCluster)
}

func TestOpenAPISpec_IsValid(t *testing.T) {
	newValidator(t)
}

func TestOpenAPIContract_SuccessResponses(t *testing.T) {
	_, v := newValidator(t)

	cases := []contractCase{
		{
			name:   "GET /health",
			method: httpgo.MethodGet,
			path:   "/health",
		},
		{
			name:   "GET /ready",
			method: httpgo.MethodGet,
			path:   "/ready",
			setup: func(fake *clusterfakes.FakeCluster) {
				fake.LeaderReturns(1)
				fake.AppliedIndexReturns(1)
			},
		},
		{
			name:   "GET /openapi.yaml",
			method: httpgo.MethodGet,
			path:   "/openapi.yaml",
		},
		{
			name:   "GET /docs",
			method: httpgo.MethodGet,
			path:   "/docs",
		},

		// --- database ---
		{
			name:   "GET /database",
			method: httpgo.MethodGet,
			path:   "/database",
			setup: func(fake *clusterfakes.FakeCluster) {
				fake.DatabasesReturns([]*cluster.Configuration{sampleConfig("db-1")}, nil)
			},
		},
		{
			name:        "POST /database/{id}",
			method:      httpgo.MethodPost,
			path:        "/database/db-1",
			body:        "[config]\nvalue = \"x\"",
			contentType: "text/toml",
		},
		{
			name:   "GET /database/{id}/versions",
			method: httpgo.MethodGet,
			path:   "/database/db-1/versions",
			setup: func(fake *clusterfakes.FakeCluster) {
				fake.DatabaseVersionsReturns(sampleVersions(), nil)
			},
		},
		{
			name:   "GET /database/{id}/versions/{version}",
			method: httpgo.MethodGet,
			path:   "/database/db-1/versions/1",
			setup: func(fake *clusterfakes.FakeCluster) {
				fake.DatabaseVersionReturns(sampleConfig("db-1"), nil)
			},
		},
		{
			name:   "POST /database/{id}/rollback",
			method: httpgo.MethodPost,
			path:   "/database/db-1/rollback?to=1",
			setup: func(fake *clusterfakes.FakeCluster) {
				fake.DatabaseVersionReturns(sampleConfig("db-1"), nil)
			},
		},

		// --- ingestable ---
		{
			name:   "GET /ingestable",
			method: httpgo.MethodGet,
			path:   "/ingestable",
			setup: func(fake *clusterfakes.FakeCluster) {
				fake.IngestablesReturns([]*cluster.Configuration{sampleConfig("ing-1")}, nil)
			},
		},
		{
			name:        "POST /ingestable/{id}",
			method:      httpgo.MethodPost,
			path:        "/ingestable/ing-1",
			body:        "[config]\nvalue = \"x\"",
			contentType: "text/toml",
		},
		{
			name:   "GET /ingestable/{id}/versions",
			method: httpgo.MethodGet,
			path:   "/ingestable/ing-1/versions",
			setup: func(fake *clusterfakes.FakeCluster) {
				fake.IngestableVersionsReturns(sampleVersions(), nil)
			},
		},
		{
			name:   "GET /ingestable/{id}/versions/{version}",
			method: httpgo.MethodGet,
			path:   "/ingestable/ing-1/versions/1",
			setup: func(fake *clusterfakes.FakeCluster) {
				fake.IngestableVersionReturns(sampleConfig("ing-1"), nil)
			},
		},
		{
			name:   "POST /ingestable/{id}/rollback",
			method: httpgo.MethodPost,
			path:   "/ingestable/ing-1/rollback?to=1",
			setup: func(fake *clusterfakes.FakeCluster) {
				fake.IngestableVersionReturns(sampleConfig("ing-1"), nil)
			},
		},

		// --- syncable ---
		{
			name:   "GET /syncable",
			method: httpgo.MethodGet,
			path:   "/syncable",
			setup: func(fake *clusterfakes.FakeCluster) {
				fake.SyncablesReturns([]*cluster.Configuration{sampleConfig("sync-1")}, nil)
			},
		},
		{
			name:        "POST /syncable/{id}",
			method:      httpgo.MethodPost,
			path:        "/syncable/sync-1",
			body:        "[config]\nvalue = \"x\"",
			contentType: "text/toml",
		},
		{
			name:   "GET /syncable/{id}/versions",
			method: httpgo.MethodGet,
			path:   "/syncable/sync-1/versions",
			setup: func(fake *clusterfakes.FakeCluster) {
				fake.SyncableVersionsReturns(sampleVersions(), nil)
			},
		},
		{
			name:   "GET /syncable/{id}/versions/{version}",
			method: httpgo.MethodGet,
			path:   "/syncable/sync-1/versions/1",
			setup: func(fake *clusterfakes.FakeCluster) {
				fake.SyncableVersionReturns(sampleConfig("sync-1"), nil)
			},
		},
		{
			name:   "POST /syncable/{id}/rollback",
			method: httpgo.MethodPost,
			path:   "/syncable/sync-1/rollback?to=1",
			setup: func(fake *clusterfakes.FakeCluster) {
				fake.SyncableVersionReturns(sampleConfig("sync-1"), nil)
			},
		},

		// --- type ---
		{
			name:   "GET /type",
			method: httpgo.MethodGet,
			path:   "/type",
			setup: func(fake *clusterfakes.FakeCluster) {
				fake.TypesReturns([]*cluster.Configuration{sampleConfig("t-1")}, nil)
			},
		},
		{
			name:   "GET /type/{id} (graph)",
			method: httpgo.MethodGet,
			// The handler parses start/end with a format that rejects
			// fractional seconds, so keep the example at whole seconds.
			path: "/type/t-1?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z",
			setup: func(fake *clusterfakes.FakeCluster) {
				fake.TypeGraphReturns([]cluster.TimePoint{
					{End: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC), Value: 3},
				}, nil)
			},
		},
		{
			name:        "POST /type/{id}",
			method:      httpgo.MethodPost,
			path:        "/type/t-1",
			body:        "[config]\nvalue = \"x\"",
			contentType: "text/toml",
		},
		{
			name:   "GET /type/{id}/versions",
			method: httpgo.MethodGet,
			path:   "/type/t-1/versions",
			setup: func(fake *clusterfakes.FakeCluster) {
				fake.TypeVersionsReturns(sampleVersions(), nil)
			},
		},
		{
			name:   "GET /type/{id}/versions/{version}",
			method: httpgo.MethodGet,
			path:   "/type/t-1/versions/1",
			setup: func(fake *clusterfakes.FakeCluster) {
				fake.TypeVersionReturns(sampleConfig("t-1"), nil)
			},
		},

		// --- proposal ---
		{
			name:   "GET /proposal",
			method: httpgo.MethodGet,
			path:   "/proposal",
			setup: func(fake *clusterfakes.FakeCluster) {
				fake.ProposalsReturns([]*cluster.Proposal{
					{Entities: []*cluster.Entity{{
						Type: sampleType(),
						Key:  []byte("k"),
						Data: []byte(`{"hello":"world"}`),
					}}},
				}, nil)
			},
		},
		{
			name:        "POST /proposal",
			method:      httpgo.MethodPost,
			path:        "/proposal",
			contentType: "application/json",
			body:        `{"entities":[{"typeId":"t-1","key":"k","data":{"hello":"world"}}]}`,
			setup: func(fake *clusterfakes.FakeCluster) {
				fake.ResolveTypeReturns(sampleType(), nil)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &clusterfakes.FakeCluster{}
			if tc.setup != nil {
				tc.setup(fake)
			}
			h := httppkg.New(fake)

			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			req := httptest.NewRequest(tc.method, "http://localhost"+tc.path, body)
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)

			require.GreaterOrEqual(t, rr.Code, 200, "handler returned an error status unexpectedly")
			require.Less(t, rr.Code, 300, "handler returned %d; body=%s", rr.Code, rr.Body.String())

			valid, errs := v.ValidateHttpResponse(req, rr.Result())
			require.True(t, valid, "response did not match spec for %s: %s", tc.name, formatValidationErrors(errs))
		})
	}
}

// TestOpenAPIContract_ErrorResponses validates that the ErrorResponse
// shape returned by a few representative error paths matches the spec.
// We don't exhaustively cover every handler's error codes — the goal is
// to catch shape drift on the ErrorResponse struct, not to re-test
// error logic already covered in handler_test.go and versions_test.go.
func TestOpenAPIContract_ErrorResponses(t *testing.T) {
	_, v := newValidator(t)

	cases := []struct {
		name       string
		method     string
		path       string
		body       string
		setup      func(*clusterfakes.FakeCluster)
		wantStatus int
	}{
		{
			name:       "POST /proposal with invalid JSON → 400",
			method:     httpgo.MethodPost,
			path:       "/proposal",
			body:       "not-json",
			wantStatus: 400,
		},
		{
			name:       "GET /database/{id}/versions/{version} with bad version → 400",
			method:     httpgo.MethodGet,
			path:       "/database/db-1/versions/abc",
			wantStatus: 400,
		},
		{
			name:   "GET /database/{id}/versions with resource missing → 404",
			method: httpgo.MethodGet,
			path:   "/database/missing/versions",
			setup: func(f *clusterfakes.FakeCluster) {
				f.DatabaseVersionsReturns(nil, cluster.ErrResourceNotFound)
			},
			wantStatus: 404,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &clusterfakes.FakeCluster{}
			if tc.setup != nil {
				tc.setup(fake)
			}
			h := httppkg.New(fake)

			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			req := httptest.NewRequest(tc.method, "http://localhost"+tc.path, body)
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)

			require.Equal(t, tc.wantStatus, rr.Code, "body=%s", rr.Body.String())

			valid, errs := v.ValidateHttpResponse(req, rr.Result())
			require.True(t, valid, "error response did not match spec for %s: %s", tc.name, formatValidationErrors(errs))
		})
	}
}

// TestOpenAPIContract_UnauthorizedShape validates the 401 body shape
// when a bearer token is required but not supplied.
func TestOpenAPIContract_UnauthorizedShape(t *testing.T) {
	_, v := newValidator(t)

	fake := &clusterfakes.FakeCluster{}
	h := httppkg.New(fake, httppkg.WithBearerToken("secret"))

	req := httptest.NewRequest(httpgo.MethodGet, "http://localhost/database", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, 401, rr.Code)
	valid, errs := v.ValidateHttpResponse(req, rr.Result())
	require.True(t, valid, "401 response did not match spec: %s", formatValidationErrors(errs))
}

// TestOpenAPIContract_SpecCoversAllRoutes is a belt-and-suspenders
// check: every route the Chi router serves must have an entry in the
// spec. If someone adds a new route without documenting it, this fails.
// It uses a lightweight, hand-maintained list of router paths because
// chi doesn't expose a stable route walker in this code path and the
// list is short.
func TestOpenAPIContract_SpecCoversAllRoutes(t *testing.T) {
	doc, _ := newValidator(t)

	// The set of routes registered in http.go. Keep in sync when adding
	// new handlers.
	expected := []string{
		"/health",
		"/ready",
		"/openapi.yaml",
		"/docs",
		"/database",
		"/database/{id}",
		"/database/{id}/versions",
		"/database/{id}/versions/{version}",
		"/database/{id}/rollback",
		"/ingestable",
		"/ingestable/{id}",
		"/ingestable/{id}/versions",
		"/ingestable/{id}/versions/{version}",
		"/ingestable/{id}/rollback",
		"/syncable",
		"/syncable/{id}",
		"/syncable/{id}/versions",
		"/syncable/{id}/versions/{version}",
		"/syncable/{id}/rollback",
		"/type",
		"/type/{id}",
		"/type/{id}/versions",
		"/type/{id}/versions/{version}",
		"/proposal",
	}

	model, errs := doc.BuildV3Model()
	require.Empty(t, errs, "must build v3 model from spec")

	specPaths := make(map[string]bool)
	for p := range model.Model.Paths.PathItems.FromOldest() {
		specPaths[p] = true
	}

	var missing []string
	for _, p := range expected {
		if !specPaths[p] {
			missing = append(missing, p)
		}
	}
	sort.Strings(missing)
	require.Empty(t, missing, "routes registered in http.go are missing from api/openapi.yaml")
}
