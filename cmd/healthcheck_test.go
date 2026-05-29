package cmd

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestHealthcheckURL verifies the loopback URL derivation, in particular
// the rewrite of empty/wildcard listen hosts to 127.0.0.1.
func TestHealthcheckURL(t *testing.T) {
	tests := []struct {
		name    string
		addr    string // COMMITTED_API_ADDR; "" means unset
		tlsCert bool   // COMMITTED_HTTP_TLS_CERT_FILE set
		want    string
	}{
		{name: "unset defaults to loopback 8080", addr: "", want: "http://127.0.0.1:8080/ready"},
		{name: "wildcard host rewritten", addr: ":8080", want: "http://127.0.0.1:8080/ready"},
		{name: "0.0.0.0 rewritten", addr: "0.0.0.0:9000", want: "http://127.0.0.1:9000/ready"},
		{name: "ipv6 wildcard rewritten", addr: "[::]:8080", want: "http://127.0.0.1:8080/ready"},
		{name: "explicit host preserved", addr: "10.0.0.5:8080", want: "http://10.0.0.5:8080/ready"},
		{name: "tls cert flips scheme to https", addr: ":8443", tlsCert: true, want: "https://127.0.0.1:8443/ready"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.addr != "" {
				t.Setenv("COMMITTED_API_ADDR", tt.addr)
			}
			if tt.tlsCert {
				t.Setenv("COMMITTED_HTTP_TLS_CERT_FILE", "/some/cert.pem")
			}
			require.Equal(t, tt.want, healthcheckURL())
		})
	}
}

// TestRunHealthcheck drives runHealthcheck against a local server that
// returns the configured status, asserting 200 → nil and anything else
// → error. Pointing COMMITTED_API_ADDR at the httptest listener also
// exercises the real GET path.
func TestRunHealthcheck(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		wantErr bool
	}{
		{name: "ready returns nil", status: http.StatusOK},
		{name: "not ready returns error", status: http.StatusServiceUnavailable, wantErr: true},
		{name: "server error returns error", status: http.StatusInternalServerError, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/ready", r.URL.Path)
				w.WriteHeader(tt.status)
			}))
			defer srv.Close()

			// httptest serves http://127.0.0.1:PORT; feed the host:port
			// back through the env the command reads.
			t.Setenv("COMMITTED_API_ADDR", strings.TrimPrefix(srv.URL, "http://"))

			err := runHealthcheck()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestRunHealthcheck_Unreachable verifies a connection failure (nothing
// listening) is reported as an error rather than a false-healthy exit 0.
func TestRunHealthcheck_Unreachable(t *testing.T) {
	// Port 1 has nothing listening; the dial fails fast.
	t.Setenv("COMMITTED_API_ADDR", "127.0.0.1:1")
	require.Error(t, runHealthcheck())
}
