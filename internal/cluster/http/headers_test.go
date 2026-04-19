package http_test

import (
	"crypto/tls"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	"github.com/philborlin/committed/internal/cluster/http"
)

// TestSecurityHeaders_APIRoute asserts the defense-in-depth headers the
// securityHeaders middleware sets on every response. /health is a
// convenient unauthenticated API route that returns 200 without any
// cluster state, so it's a clean signal that the middleware ran.
func TestSecurityHeaders_APIRoute(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	h := http.New(fake)

	req := httptest.NewRequest("GET", "http://localhost/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)

	require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
	require.Equal(t, "DENY", resp.Header.Get("X-Frame-Options"))
	require.Equal(t, "no-referrer", resp.Header.Get("Referrer-Policy"))
	require.Equal(t, "camera=(), microphone=(), geolocation=()", resp.Header.Get("Permissions-Policy"))
	require.Equal(t, "default-src 'none'; frame-ancestors 'none'", resp.Header.Get("Content-Security-Policy"))

	// Plaintext request must NOT carry HSTS — it's a no-op over http
	// and we don't want operators to assume their config is TLS-safe.
	require.Empty(t, resp.Header.Get("Strict-Transport-Security"))
}

// TestSecurityHeaders_DocsRouteOverridesCSP asserts that /docs swaps
// the strict API default-src 'none' CSP for one that actually permits
// the Swagger UI assets it loads from unpkg.com. The other headers
// (nosniff, frame-ancestors, etc.) must still be present — the per-
// route override targets CSP only.
func TestSecurityHeaders_DocsRouteOverridesCSP(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	h := http.New(fake)

	req := httptest.NewRequest("GET", "http://localhost/docs", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)

	csp := resp.Header.Get("Content-Security-Policy")
	require.NotEqual(t, "default-src 'none'; frame-ancestors 'none'", csp,
		"/docs must override the strict API CSP or Swagger UI won't load")
	require.Contains(t, csp, "https://unpkg.com", "CSP must allow the unpkg CDN Swagger UI loads from")
	require.Contains(t, csp, "script-src")
	require.Contains(t, csp, "style-src")
	require.Contains(t, csp, "frame-ancestors 'none'",
		"clickjacking defense must survive the override")

	require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
	require.Equal(t, "DENY", resp.Header.Get("X-Frame-Options"))
	require.Equal(t, "no-referrer", resp.Header.Get("Referrer-Policy"))
}

// TestSecurityHeaders_HSTSOnlyOverTLS brings a real TLS server up and
// asserts the HSTS header appears on an https:// request. The
// TestSecurityHeaders_APIRoute test already covers the negative case
// (plaintext → no HSTS); this is the positive side.
func TestSecurityHeaders_HSTSOnlyOverTLS(t *testing.T) {
	pki := newTLSTestPKI(t)
	serverCert, serverKey := pki.issueCert(t, "server", extServer)

	fake := &clusterfakes.FakeCluster{}
	h := http.New(fake)

	pair, err := tls.LoadX509KeyPair(serverCert, serverKey)
	require.NoError(t, err)
	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{pair},
		MinVersion:   tls.VersionTLS12,
	}

	addr := pickFreeLoopbackAddr(t)
	srv := h.NewServer(addr, http.WithTLSConfig(serverTLS))

	doneC := make(chan error, 1)
	go func() { doneC <- srv.ListenAndServeTLS("", "") }()
	t.Cleanup(func() {
		_ = srv.Close()
		<-doneC
	})
	waitListeningTLS(t, addr)

	client := httpsClient(t, pki.caPEM, nil, nil)
	resp, err := client.Get("https://" + addr + "/health")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	hsts := resp.Header.Get("Strict-Transport-Security")
	require.NotEmpty(t, hsts, "HSTS must be set on TLS responses")
	require.Contains(t, hsts, "max-age=")
	require.Contains(t, hsts, "includeSubDomains")
}

// TestSecurityHeaders_OpenAPISpecKeepsStrictCSP asserts the spec
// endpoint (static YAML) retains the default strict API CSP. Only
// /docs should relax the policy.
func TestSecurityHeaders_OpenAPISpecKeepsStrictCSP(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	h := http.New(fake)

	req := httptest.NewRequest("GET", "http://localhost/openapi.yaml", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "default-src 'none'; frame-ancestors 'none'",
		resp.Header.Get("Content-Security-Policy"))
}

