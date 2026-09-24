package httptransport

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// A trusted certificate establishes the connection, but never substitutes for
// the peer token on any of the three peer routes.
func TestPeerAuthorizationWithMTLS(t *testing.T) {
	pki := newTestPKI(t)
	cert, key := pki.issueNodeCert(t, "server")
	clientCert, clientKey := pki.issueNodeCert(t, "operator")
	info := &transport.TLSInfo{TrustedCAFile: pki.caFile, CertFile: cert, KeyFile: key, ClientCertAuth: true}
	serverTLS, err := info.ServerConfig()
	require.NoError(t, err)
	tr := New(1, nil, zap.NewNop(), &fakeRaft{}, nil, info, "peer")
	defer tr.Stop()
	tr.SetDiskReporter(func(uint64, string) (cluster.DiskVerdict, error) {
		return cluster.DiskVerdict{State: "ok", LeaderID: 1}, nil
	})
	server := httptest.NewUnstartedServer(tr.handler())
	server.TLS = serverTLS
	server.StartTLS()
	defer server.Close()
	for _, withCert := range []bool{false, true} {
		cfg := &tls.Config{RootCAs: loadCAPool(t, pki.caFile), MinVersion: tls.VersionTLS12}
		if withCert {
			cfg = clientTLSConfig(t, pki.caFile, clientCert, clientKey)
		}
		client := &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}}
		defer client.CloseIdleConnections()
		for _, route := range []struct {
			method, path, body string
			status             int
		}{
			{http.MethodPost, raftMessagePath, "invalid protobuf", 400},
			{http.MethodGet, eventsPath, "", 404}, // no event log in this transport fixture
			{http.MethodPost, diskReportPath, `{"node":2,"state":"ok"}`, 200},
		} {
			for _, token := range []string{"", "api", "membership", "peer"} {
				req, err := http.NewRequest(route.method, server.URL+route.path, strings.NewReader(route.body))
				require.NoError(t, err)
				req.Header.Set(clusterIDHeader, clusterID)
				req.Header.Set(protocolHeader, protocolVersion)
				if token != "" {
					req.Header.Set("Authorization", "Bearer "+token)
				}
				resp, err := client.Do(req)
				if !withCert {
					require.Error(t, err, "certificate required on %s", route.path)
					continue
				}
				require.NoError(t, err)
				require.NoError(t, resp.Body.Close())
				want := http.StatusUnauthorized
				if token == "peer" {
					want = route.status
				}
				require.Equal(t, want, resp.StatusCode, "path=%s token=%q", route.path, token)
			}
		}
	}
}
