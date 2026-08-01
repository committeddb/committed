package http_test

import (
	httpgo "net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/http"
)

// getPprof issues GET /debug/pprof/heap against h, optionally with a bearer token.
func getPprof(h *http.HTTP, token string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(httpgo.MethodGet, "http://localhost/debug/pprof/heap", nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

// TestPprofDisabledByDefault: without WithPprof, the profiling routes are not
// mounted at all — a request 404s.
func TestPprofDisabledByDefault(t *testing.T) {
	h := http.New(&clusterfakes.FakeCluster{})
	require.Equal(t, httpgo.StatusNotFound, getPprof(h, "").Code)
}

// TestPprofEnabled: with WithPprof and no auth token, the heap profile is served.
func TestPprofEnabled(t *testing.T) {
	h := http.New(&clusterfakes.FakeCluster{}, http.WithPprof())
	require.Equal(t, httpgo.StatusOK, getPprof(h, "").Code)
}

// TestPprofBehindBearerAuth: with a token configured, the profiling endpoints sit
// inside the authenticated group — no token is 401, the right token is 200.
func TestPprofBehindBearerAuth(t *testing.T) {
	h := http.New(&clusterfakes.FakeCluster{}, http.WithBearerToken("sekret"), http.WithPprof())
	require.Equal(t, httpgo.StatusUnauthorized, getPprof(h, "").Code, "no token must be rejected")
	require.Equal(t, httpgo.StatusOK, getPprof(h, "sekret").Code, "the right token is admitted")
}
