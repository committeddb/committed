package cmd

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster/db"
	"github.com/philborlin/committed/internal/cluster/db/parser"
	dbtest "github.com/philborlin/committed/internal/cluster/db/testing"
	clusterhttp "github.com/philborlin/committed/internal/cluster/http"
)

// newTestDB builds a single-node db.DB backed by MemoryStorage. Mirrors
// the shape of dbtest.CreateDB but returns the underlying *db.DB so
// runNode can consume d.ErrorC directly.
func newTestDB(t *testing.T) *db.DB {
	t.Helper()
	s := dbtest.NewMemoryStorage()
	p := parser.New()
	peers := make(db.Peers)
	peers[1] = "" // empty URL skips peer-transport listener
	d := db.New(1, peers, s, p, nil, nil, db.WithTickInterval(1*time.Millisecond))
	d.EatCommitC()
	return d
}

// pickFreeAddr allocates a loopback port, closes it immediately, and
// returns "127.0.0.1:<port>" for the caller to re-bind. Same pattern as
// internal/cluster/db/httptransport/mtls_test.go:pickFreeAddr.
func pickFreeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

// waitListening polls addr until it accepts a TCP connection.
func waitListening(t *testing.T, addr string) {
	t.Helper()
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err != nil {
			return false
		}
		_ = c.Close()
		return true
	}, 2*time.Second, 10*time.Millisecond, "http server never bound to %s", addr)
}

// TestRunNode_SIGINT starts runNode, sends SIGINT to the test process,
// and asserts runNode returns 0 after the HTTP server has been
// gracefully shut down. The signal path is the one operators will
// exercise via `kill`, `systemctl stop`, or a Kubernetes rolling
// restart — the in-process signal is the only way to drive it from a
// test.
func TestRunNode_SIGINT(t *testing.T) {
	d := newTestDB(t)
	h := clusterhttp.New(d)
	addr := pickFreeAddr(t)
	srv := h.NewServer(addr)

	done := make(chan int, 1)
	go func() { done <- runNode(d, srv) }()

	waitListening(t, addr)

	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGINT))

	select {
	case code := <-done:
		require.Equal(t, 0, code)
	case <-time.After(5 * time.Second):
		t.Fatal("runNode did not return after SIGINT")
	}

	// After graceful shutdown the listener should be gone.
	_, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	require.Error(t, err, "http server still accepting connections after shutdown")
}

// TestGracefulShutdown_ClosesHTTPAndDB verifies the drain path
// directly, without going through the signal handler. A dial after
// shutdown must fail (httpServer.Shutdown actually closed the
// listener), and calling d.Close a second time must be safe (proves
// the first call inside gracefulShutdown completed).
func TestGracefulShutdown_ClosesHTTPAndDB(t *testing.T) {
	d := newTestDB(t)
	h := clusterhttp.New(d)
	addr := pickFreeAddr(t)
	srv := h.NewServer(addr)

	serveErrC := make(chan error, 1)
	go func() { serveErrC <- srv.ListenAndServe() }()
	waitListening(t, addr)

	code := gracefulShutdown(d, srv)
	require.Equal(t, 0, code)

	select {
	case err := <-serveErrC:
		require.ErrorIs(t, err, http.ErrServerClosed)
	case <-time.After(2 * time.Second):
		t.Fatal("ListenAndServe did not return after Shutdown")
	}

	_, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	require.Error(t, err, "http server still accepting connections after shutdown")

	require.NoError(t, d.Close(), "db.Close must be idempotent after gracefulShutdown")
}

// TestShutdownTimeout_Default covers env-var parsing: unset returns
// the default, a valid duration is honored, and a garbage value falls
// back to the default rather than silently disabling the graceful
// path.
func TestShutdownTimeout(t *testing.T) {
	orig, hadOrig := os.LookupEnv("COMMITTED_SHUTDOWN_TIMEOUT")
	t.Cleanup(func() {
		if hadOrig {
			_ = os.Setenv("COMMITTED_SHUTDOWN_TIMEOUT", orig)
		} else {
			_ = os.Unsetenv("COMMITTED_SHUTDOWN_TIMEOUT")
		}
	})

	_ = os.Unsetenv("COMMITTED_SHUTDOWN_TIMEOUT")
	require.Equal(t, defaultShutdownTimeout, shutdownTimeout())

	_ = os.Setenv("COMMITTED_SHUTDOWN_TIMEOUT", "45s")
	require.Equal(t, 45*time.Second, shutdownTimeout())

	_ = os.Setenv("COMMITTED_SHUTDOWN_TIMEOUT", "not-a-duration")
	require.Equal(t, defaultShutdownTimeout, shutdownTimeout())

	_ = os.Setenv("COMMITTED_SHUTDOWN_TIMEOUT", "0s")
	require.Equal(t, defaultShutdownTimeout, shutdownTimeout())
}

// TestLoadAPITLSConfig covers the env-var parsing for the client-facing
// HTTP TLS config. The startup-fatal path in nodeCmd.Run wraps this
// helper, so covering the helper covers the fatal rule by extension.
func TestLoadAPITLSConfig(t *testing.T) {
	// Snapshot + restore the three env vars; tests in the same process
	// must not leak state.
	vars := []string{
		"COMMITTED_HTTP_TLS_CERT_FILE",
		"COMMITTED_HTTP_TLS_KEY_FILE",
		"COMMITTED_HTTP_TLS_CLIENT_CA_FILE",
	}
	saved := map[string]string{}
	hadVar := map[string]bool{}
	for _, v := range vars {
		val, ok := os.LookupEnv(v)
		saved[v] = val
		hadVar[v] = ok
	}
	t.Cleanup(func() {
		for _, v := range vars {
			if hadVar[v] {
				_ = os.Setenv(v, saved[v])
			} else {
				_ = os.Unsetenv(v)
			}
		}
	})

	clearEnv := func() {
		for _, v := range vars {
			_ = os.Unsetenv(v)
		}
	}

	pki := newAPITLSTestPKI(t)
	certFile, keyFile := pki.issueServerCert(t)

	t.Run("unset returns nil config and no error", func(t *testing.T) {
		clearEnv()
		cfg, err := loadAPITLSConfig()
		require.NoError(t, err)
		require.Nil(t, cfg)
	})

	t.Run("cert without key is a hard error", func(t *testing.T) {
		clearEnv()
		_ = os.Setenv("COMMITTED_HTTP_TLS_CERT_FILE", certFile)
		cfg, err := loadAPITLSConfig()
		require.Error(t, err)
		require.Nil(t, cfg)
	})

	t.Run("key without cert is a hard error", func(t *testing.T) {
		clearEnv()
		_ = os.Setenv("COMMITTED_HTTP_TLS_KEY_FILE", keyFile)
		cfg, err := loadAPITLSConfig()
		require.Error(t, err)
		require.Nil(t, cfg)
	})

	t.Run("client CA alone is a hard error", func(t *testing.T) {
		clearEnv()
		_ = os.Setenv("COMMITTED_HTTP_TLS_CLIENT_CA_FILE", pki.caFile)
		cfg, err := loadAPITLSConfig()
		require.Error(t, err)
		require.Nil(t, cfg)
	})

	t.Run("cert + key enables TLS without client auth", func(t *testing.T) {
		clearEnv()
		_ = os.Setenv("COMMITTED_HTTP_TLS_CERT_FILE", certFile)
		_ = os.Setenv("COMMITTED_HTTP_TLS_KEY_FILE", keyFile)
		cfg, err := loadAPITLSConfig()
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Len(t, cfg.Certificates, 1)
		require.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
		require.Equal(t, tls.NoClientCert, cfg.ClientAuth)
		require.Nil(t, cfg.ClientCAs)
	})

	t.Run("cert + key + client CA enables mTLS", func(t *testing.T) {
		clearEnv()
		_ = os.Setenv("COMMITTED_HTTP_TLS_CERT_FILE", certFile)
		_ = os.Setenv("COMMITTED_HTTP_TLS_KEY_FILE", keyFile)
		_ = os.Setenv("COMMITTED_HTTP_TLS_CLIENT_CA_FILE", pki.caFile)
		cfg, err := loadAPITLSConfig()
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, tls.RequireAndVerifyClientCert, cfg.ClientAuth)
		require.NotNil(t, cfg.ClientCAs)
	})

	t.Run("bad cert path surfaces the load error", func(t *testing.T) {
		clearEnv()
		_ = os.Setenv("COMMITTED_HTTP_TLS_CERT_FILE", filepath.Join(t.TempDir(), "missing.pem"))
		_ = os.Setenv("COMMITTED_HTTP_TLS_KEY_FILE", keyFile)
		cfg, err := loadAPITLSConfig()
		require.Error(t, err)
		require.Nil(t, cfg)
	})

	t.Run("empty client CA file is a hard error", func(t *testing.T) {
		clearEnv()
		empty := filepath.Join(t.TempDir(), "empty.pem")
		require.NoError(t, os.WriteFile(empty, []byte("not a cert"), 0o600))
		_ = os.Setenv("COMMITTED_HTTP_TLS_CERT_FILE", certFile)
		_ = os.Setenv("COMMITTED_HTTP_TLS_KEY_FILE", keyFile)
		_ = os.Setenv("COMMITTED_HTTP_TLS_CLIENT_CA_FILE", empty)
		cfg, err := loadAPITLSConfig()
		require.Error(t, err)
		require.Nil(t, cfg)
	})
}

// apiTLSTestPKI is a throwaway CA for loadAPITLSConfig tests. We can't
// reuse the one in internal/cluster/http/tls_test.go (different
// package, test-only) so we inline a small copy here.
type apiTLSTestPKI struct {
	caFile string
	caCert *x509.Certificate
	caKey  *rsa.PrivateKey
	dir    string
}

func newAPITLSTestPKI(t *testing.T) *apiTLSTestPKI {
	t.Helper()
	dir := t.TempDir()

	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "committed-cmd-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	caFile := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caFile, caPEM, 0o600))
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	return &apiTLSTestPKI{caFile: caFile, caCert: caCert, caKey: caKey, dir: dir}
}

func (p *apiTLSTestPKI) issueServerCert(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "server"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.caCert, &key.PublicKey, p.caKey)
	require.NoError(t, err)
	certFile = filepath.Join(p.dir, "server.pem")
	keyFile = filepath.Join(p.dir, "server.key")
	require.NoError(t, os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0o600))
	return certFile, keyFile
}
