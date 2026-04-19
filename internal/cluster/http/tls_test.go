package http_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	httpgo "net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	"github.com/philborlin/committed/internal/cluster/http"
)

// These tests cover the http package's TLS wiring — the WithTLSConfig
// ServerOption and the hand-constructed *http.Server shape that
// cmd/node.go drives via ListenAndServeTLS. Env-var parsing and the
// partial-config startup error live in cmd/node_test.go; that's the
// layer that owns file I/O and fatal paths.

// TestTLS_HTTPS_RoundTrip verifies the happy path: a server configured
// via WithTLSConfig terminates TLS correctly, and a client trusting
// the issuing CA can hit /health and get 200.
func TestTLS_HTTPS_RoundTrip(t *testing.T) {
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

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), "ok")
}

// TestTLS_MinVersion_TLS12 verifies the server refuses a TLS 1.1 client.
// We don't assert the exact error string because it varies across Go
// versions, only that the handshake does not complete.
func TestTLS_MinVersion_TLS12(t *testing.T) {
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

	caPool := x509.NewCertPool()
	require.True(t, caPool.AppendCertsFromPEM(pki.caPEM))
	// Clamp the client to TLS 1.1 — below the server floor. MinVersion
	// and MaxVersion both pinned so Go doesn't upgrade under us.
	conn, err := tls.Dial("tcp", addr, &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS10,
		MaxVersion: tls.VersionTLS11,
	})
	if err == nil {
		_ = conn.Close()
		t.Fatal("TLS 1.1 client must be rejected by TLS 1.2 floor")
	}
}

// TestTLS_MTLS_RequiresClientCert verifies the CLIENT_CA_FILE path:
// when ClientAuth = RequireAndVerifyClientCert is set, a client with
// NO cert cannot reach a protected route.
func TestTLS_MTLS_RequiresClientCert(t *testing.T) {
	pki := newTLSTestPKI(t)
	serverCert, serverKey := pki.issueCert(t, "server", extServer)

	pair, err := tls.LoadX509KeyPair(serverCert, serverKey)
	require.NoError(t, err)

	clientCAPool := x509.NewCertPool()
	require.True(t, clientCAPool.AppendCertsFromPEM(pki.caPEM))

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{pair},
		MinVersion:   tls.VersionTLS12,
		ClientCAs:    clientCAPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}

	fake := &clusterfakes.FakeCluster{}
	h := http.New(fake)
	addr := pickFreeLoopbackAddr(t)
	srv := h.NewServer(addr, http.WithTLSConfig(serverTLS))
	doneC := make(chan error, 1)
	go func() { doneC <- srv.ListenAndServeTLS("", "") }()
	t.Cleanup(func() {
		_ = srv.Close()
		<-doneC
	})
	waitListeningTLS(t, addr)

	// Case 1: client with no cert is rejected.
	client := httpsClient(t, pki.caPEM, nil, nil)
	_, err = client.Get("https://" + addr + "/health")
	require.Error(t, err, "mTLS server must reject a client with no cert")

	// Case 2: client with CA-signed cert succeeds.
	clientCert, clientKey := pki.issueCert(t, "client", extClient)
	clientCertPEM, err := os.ReadFile(clientCert)
	require.NoError(t, err)
	clientKeyPEM, err := os.ReadFile(clientKey)
	require.NoError(t, err)
	client2 := httpsClient(t, pki.caPEM, clientCertPEM, clientKeyPEM)
	resp, err := client2.Get("https://" + addr + "/health")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
}

// TestTLS_WithTLSConfig_SetsOnServer verifies the ServerOption wires
// TLSConfig onto the returned *http.Server. Pure plumbing check — no
// network I/O — so it runs fast and would fail closed if someone
// refactored NewServer and dropped the option.
func TestTLS_WithTLSConfig_SetsOnServer(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	h := http.New(fake)

	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	s := h.NewServer(":0", http.WithTLSConfig(cfg))
	require.Same(t, cfg, s.TLSConfig)
}

// --- test helpers ---

type certExt int

const (
	extServer certExt = iota
	extClient
)

// tlsTestPKI is a throwaway CA + cert factory for these tests. It
// parallels internal/cluster/db/httptransport/certs_test.go:testPKI —
// we duplicate rather than share because that helper lives in an
// internal test package and exporting it would create a cross-package
// test dependency.
type tlsTestPKI struct {
	dir    string
	caPEM  []byte
	caCert *x509.Certificate
	caKey  *rsa.PrivateKey
}

func newTLSTestPKI(t *testing.T) *tlsTestPKI {
	t.Helper()
	dir := t.TempDir()

	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "committed-http-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})

	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	return &tlsTestPKI{dir: dir, caPEM: caPEM, caCert: caCert, caKey: caKey}
}

func (p *tlsTestPKI) issueCert(t *testing.T, name string, ext certExt) (certFile, keyFile string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
	}
	switch ext {
	case extServer:
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}
		tmpl.DNSNames = []string{"localhost"}
	case extClient:
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.caCert, &key.PublicKey, p.caKey)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})

	certFile = filepath.Join(p.dir, name+".pem")
	keyFile = filepath.Join(p.dir, name+".key")
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))
	return certFile, keyFile
}

// httpsClient returns an http.Client configured to trust caPEM. If
// clientCertPEM + clientKeyPEM are non-nil they're presented during
// the handshake (mTLS client role).
func httpsClient(t *testing.T, caPEM, clientCertPEM, clientKeyPEM []byte) *httpgo.Client {
	t.Helper()
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(caPEM))

	cfg := &tls.Config{
		RootCAs:    pool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
	}
	if clientCertPEM != nil && clientKeyPEM != nil {
		pair, err := tls.X509KeyPair(clientCertPEM, clientKeyPEM)
		require.NoError(t, err)
		cfg.Certificates = []tls.Certificate{pair}
	}
	return &httpgo.Client{
		Transport: &httpgo.Transport{TLSClientConfig: cfg},
		Timeout:   3 * time.Second,
	}
}

func pickFreeLoopbackAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

// waitListeningTLS polls the address until a TCP connection can be
// made. Used in tests that spawn ListenAndServeTLS on a goroutine —
// the listener isn't guaranteed to be bound by the time the test
// dials, and a bare Sleep hides race failures.
func waitListeningTLS(t *testing.T, addr string) {
	t.Helper()
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err != nil {
			return false
		}
		_ = c.Close()
		return true
	}, 2*time.Second, 10*time.Millisecond, "TLS server never bound to %s", addr)
}
