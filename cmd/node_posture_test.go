package cmd

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsLoopbackBind(t *testing.T) {
	for _, tc := range []struct {
		addr string
		want bool
	}{
		{":8080", false},        // empty host = all interfaces = reachable off-host
		{"0.0.0.0:8080", false}, // all interfaces
		{"192.168.1.5:8080", false},
		{"example.com:8080", false}, // unresolved hostname -> safe default (non-loopback)
		{"127.0.0.1:8080", true},
		{"localhost:8080", true},
		{"[::1]:8080", true},
	} {
		t.Run(tc.addr, func(t *testing.T) {
			require.Equal(t, tc.want, isLoopbackBind(tc.addr))
		})
	}
}

func TestApiUnauthenticatedOffHost(t *testing.T) {
	mtls := &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert} //nolint:gosec // test-only, no MinVersion needed
	serverOnlyTLS := &tls.Config{}                                  //nolint:gosec // test-only

	for _, tc := range []struct {
		name     string
		addr     string
		token    string
		tls      *tls.Config
		wantLoud bool
	}{
		{"off-host, no auth, plaintext", ":8080", "", nil, true},
		{"off-host, no auth, server-only TLS (encrypted but unauthenticated)", ":8080", "", serverOnlyTLS, true},
		{"off-host, bearer token", ":8080", "secret", nil, false},
		{"off-host, mTLS", ":8080", "", mtls, false},
		{"loopback, no auth", "127.0.0.1:8080", "", nil, false},
		{"loopback ipv6, no auth", "[::1]:8080", "", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.wantLoud, apiUnauthenticatedOffHost(tc.addr, tc.token, tc.tls))
		})
	}
}
