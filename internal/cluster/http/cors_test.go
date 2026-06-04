package http_test

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	"github.com/philborlin/committed/internal/cluster/http"
)

// get issues a GET /health carrying the given Origin header and returns
// the value of the Access-Control-Allow-Origin response header. /health
// is an unauthenticated route that returns 200 without cluster state, so
// it isolates the CORS middleware from everything else.
func corsAllowOrigin(t *testing.T, h *http.HTTP, origin string) string {
	t.Helper()
	req := httptest.NewRequest("GET", "http://localhost/health", nil)
	req.Header.Set("Origin", origin)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, 200, w.Result().StatusCode)
	return w.Result().Header.Get("Access-Control-Allow-Origin")
}

// TestCORS_DisabledByDefault asserts that without WithCORS no
// Access-Control-* headers are emitted even when the request carries an
// Origin — the correct default for a server binary with no in-tree
// browser client.
func TestCORS_DisabledByDefault(t *testing.T) {
	h := http.New(&clusterfakes.FakeCluster{})
	require.Empty(t, corsAllowOrigin(t, h, "https://app.example.com"))
}

// TestCORS_AllowlistedOrigin asserts a configured origin is reflected
// back while any other origin gets no allow header (and is therefore
// blocked by the browser).
func TestCORS_AllowlistedOrigin(t *testing.T) {
	h := http.New(&clusterfakes.FakeCluster{},
		http.WithCORS([]string{"https://app.example.com"}, nil, nil))

	require.Equal(t, "https://app.example.com",
		corsAllowOrigin(t, h, "https://app.example.com"))
	require.Empty(t, corsAllowOrigin(t, h, "https://evil.example.com"))
}

// TestCORS_Wildcard asserts the literal "*" allows any origin.
func TestCORS_Wildcard(t *testing.T) {
	h := http.New(&clusterfakes.FakeCluster{},
		http.WithCORS([]string{"*"}, nil, nil))

	require.Equal(t, "*", corsAllowOrigin(t, h, "https://anything.example.com"))
}

// TestCORS_Preflight asserts an OPTIONS preflight for an allowlisted
// origin advertises the default methods and headers, and that a custom
// methods/headers list overrides those defaults.
func TestCORS_Preflight(t *testing.T) {
	preflight := func(h *http.HTTP, reqMethod string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("OPTIONS", "http://localhost/v1/proposal", nil)
		req.Header.Set("Origin", "https://app.example.com")
		req.Header.Set("Access-Control-Request-Method", reqMethod)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w
	}

	t.Run("defaults", func(t *testing.T) {
		h := http.New(&clusterfakes.FakeCluster{},
			http.WithCORS([]string{"https://app.example.com"}, nil, nil))
		w := preflight(h, "POST")
		require.Equal(t, "https://app.example.com",
			w.Result().Header.Get("Access-Control-Allow-Origin"))
		require.Contains(t,
			w.Result().Header.Get("Access-Control-Allow-Methods"), "POST")
	})

	t.Run("custom methods exclude others", func(t *testing.T) {
		h := http.New(&clusterfakes.FakeCluster{},
			http.WithCORS([]string{"https://app.example.com"},
				[]string{"GET"}, []string{"Content-Type"}))
		w := preflight(h, "DELETE")
		// DELETE is not in the custom allowlist, so the preflight must
		// not advertise it.
		require.NotContains(t,
			w.Result().Header.Get("Access-Control-Allow-Methods"), "DELETE")
	})
}
