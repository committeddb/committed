package http_test

import (
	"crypto/tls"
	"crypto/x509"
	nethttp "net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/auth"
	clusterhttp "github.com/committeddb/committed/internal/cluster/db/http"
)

func TestSplitAuthorization(t *testing.T) {
	tokens, err := auth.NewTokens("api", "member", "peer")
	require.NoError(t, err)
	e := newEngineHTTP(t, clusterhttp.WithTokens(tokens), clusterhttp.WithPprof())
	for _, route := range []struct{ method, path, token string }{
		{"GET", "/v1/type", "api"},
		{"GET", "/v1/node/status", "api"},
		{"GET", "/v1/cluster/status", "api"},
		{"POST", "/v1/proposal", "api"},
		{"GET", "/v1/membership", "member"},
		{"POST", "/v1/membership", "member"},
		{"POST", "/v1/membership/0/promote", "member"},
		{"DELETE", "/v1/membership/0", "member"},
		{"GET", "/v1/node/backup", "member"},
		{"GET", "/debug/pprof/", "member"},
		{"GET", "/debug/pprof/goroutine", "member"},
	} {
		t.Run(route.method+route.path, func(t *testing.T) {
			for _, token := range []string{"", "wrong", "api", "member", "peer"} {
				req := httptest.NewRequest(route.method, route.path, nil)
				if token != "" {
					req.Header.Set("Authorization", "Bearer "+token)
				}
				req.Header.Set("X-Committed-Forwarded", "1")
				req.Header.Set("Content-Type", "application/json")
				w := httptest.NewRecorder()
				e.h.ServeHTTP(w, req)
				if token == route.token {
					expected := 200
					if route.method != "GET" {
						expected = 400
					} // invalid bodies/IDs reach validation without mutating membership
					require.Equal(t, expected, w.Code, route.path)
				} else {
					require.Equal(t, 401, w.Code, route.path)
				}
			}
		})
	}
	for _, token := range []string{"api", "member", "peer"} {
		req := httptest.NewRequest("POST", "/v1/node/disk-report", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		e.h.ServeHTTP(w, req)
		if token == "api" {
			require.Equal(t, 404, w.Code)
		} else {
			require.Equal(t, 401, w.Code)
		}
	}
	for _, path := range []string{"/health", "/ready", "/version", "/openapi.yaml", "/docs"} {
		w := httptest.NewRecorder()
		e.h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		require.Equal(t, 200, w.Code, path)
	}
}

func TestSplitAuthorizationWithMTLS(t *testing.T) {
	tokens, err := auth.NewTokens("api", "member", "peer")
	require.NoError(t, err)
	e := newEngineHTTP(t, clusterhttp.WithTokens(tokens))
	pki := newTLSTestPKI(t)
	certFile, keyFile := pki.issueCert(t, "operator", extClient)
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(pki.caPEM))
	server := httptest.NewUnstartedServer(e.h)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	server.StartTLS()
	defer server.Close()

	withoutCert := server.Client()
	req, err := nethttp.NewRequest("GET", server.URL+"/v1/membership", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer member")
	_, err = withoutCert.Do(req)
	require.Error(t, err, "a bearer token cannot bypass required mTLS")

	transport := withoutCert.Transport.(*nethttp.Transport).Clone()
	transport.TLSClientConfig = transport.TLSClientConfig.Clone()
	transport.TLSClientConfig.Certificates = []tls.Certificate{cert}
	defer transport.CloseIdleConnections()
	client := &nethttp.Client{Transport: transport}
	// One operator certificate can exercise either role by choosing its key.
	for _, path := range []string{"/v1/type", "/v1/membership"} {
		for _, token := range []string{"", "api", "member", "peer"} {
			req, err := nethttp.NewRequest("GET", server.URL+path, nil)
			require.NoError(t, err)
			if token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}
			response, err := client.Do(req)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			expected := 401
			if (path == "/v1/type" && token == "api") || (path == "/v1/membership" && token == "member") {
				expected = 200
			}
			require.Equal(t, expected, response.StatusCode)
		}
	}
}
