package httptransport

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.etcd.io/etcd/raft/v3"
	"go.uber.org/zap"
)

// TestTransport_MTLS_AcceptsAuthorizedClient verifies the positive
// path: a client that presents a cert signed by the configured CA
// completes the TLS handshake against a transport started with
// matching mTLS config. This is the invariant the Phase 2 of
// http-authentication.md exists to produce — a CA-signed client cert
// is both necessary and sufficient to be allowed to talk to a peer.
//
// We don't drive a full raft message through the transport here;
// that's covered indirectly by every existing multi-node test. What
// this test guards is the wiring: TLS termination sits in front of
// the handler, a good client can handshake, and the bytes after the
// handshake land on the rafthttp handler.
func TestTransport_MTLS_AcceptsAuthorizedClient(t *testing.T) {
	pki := newTestPKI(t)
	serverCert, serverKey := pki.issueNodeCert(t, "server")
	clientCert, clientKey := pki.issueNodeCert(t, "client")

	addr := pickFreeAddr(t)

	tlsInfo := &transport.TLSInfo{
		TrustedCAFile:  pki.caFile,
		CertFile:       serverCert,
		KeyFile:        serverKey,
		ClientCertAuth: true,
	}

	stopC, doneC := startTLSTransport(t, addr, tlsInfo)
	defer func() {
		close(stopC)
		<-doneC
	}()
	waitUntilListening(t, addr)

	clientTLS := clientTLSConfig(t, pki.caFile, clientCert, clientKey)
	conn, err := tls.Dial("tcp", addr, clientTLS)
	require.NoError(t, err, "authorized client must complete TLS handshake")
	require.NoError(t, conn.Handshake())
	_ = conn.Close()
}

// TestTransport_MTLS_RejectsClientWithoutCert verifies that a client
// that does NOT present any certificate at all is rejected at the
// TLS layer. Without this, an attacker could skip the client-auth
// step and still reach the raft handler on plaintext bytes.
func TestTransport_MTLS_RejectsClientWithoutCert(t *testing.T) {
	pki := newTestPKI(t)
	serverCert, serverKey := pki.issueNodeCert(t, "server")

	addr := pickFreeAddr(t)
	tlsInfo := &transport.TLSInfo{
		TrustedCAFile:  pki.caFile,
		CertFile:       serverCert,
		KeyFile:        serverKey,
		ClientCertAuth: true,
	}

	stopC, doneC := startTLSTransport(t, addr, tlsInfo)
	defer func() {
		close(stopC)
		<-doneC
	}()
	waitUntilListening(t, addr)

	// Trust the server's CA but present no client cert.
	caPool := loadCAPool(t, pki.caFile)
	conn, err := tls.Dial("tcp", addr, &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
	})
	if err == nil {
		// Some TLS stacks complete the handshake at Dial and report the
		// cert failure on the first read. Force a read so we don't
		// false-pass on those.
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		_, err = conn.Read(make([]byte, 1))
		_ = conn.Close()
	}
	require.Error(t, err, "client without cert must be rejected")
}

// TestTransport_MTLS_RejectsClientSignedByDifferentCA verifies that a
// client whose cert was signed by a DIFFERENT CA is rejected, even
// though the cert itself is syntactically valid. This is the exact
// "rogue peer" scenario mTLS exists to prevent.
func TestTransport_MTLS_RejectsClientSignedByDifferentCA(t *testing.T) {
	pki := newTestPKI(t)
	serverCert, serverKey := pki.issueNodeCert(t, "server")

	// Second, UNRELATED CA — this is the attacker's PKI.
	rogue := newTestPKI(t)
	rogueCert, rogueKey := rogue.issueNodeCert(t, "rogue")

	addr := pickFreeAddr(t)
	tlsInfo := &transport.TLSInfo{
		TrustedCAFile:  pki.caFile,
		CertFile:       serverCert,
		KeyFile:        serverKey,
		ClientCertAuth: true,
	}

	stopC, doneC := startTLSTransport(t, addr, tlsInfo)
	defer func() {
		close(stopC)
		<-doneC
	}()
	waitUntilListening(t, addr)

	// Client presents a cert — but from the rogue CA the server doesn't
	// trust. Server should reject during handshake.
	clientTLS := clientTLSConfig(t, pki.caFile, rogueCert, rogueKey)
	conn, err := tls.Dial("tcp", addr, clientTLS)
	if err == nil {
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		_, err = conn.Read(make([]byte, 1))
		_ = conn.Close()
	}
	require.Error(t, err, "client with cert from untrusted CA must be rejected")
}

// startTLSTransport boots a single-peer HttpTransport at the given
// address with the supplied TLSInfo and spins its Start loop on a
// goroutine. Callers receive a stopC they can close to wind the
// transport down, plus a doneC they block on to know it's finished.
func startTLSTransport(t *testing.T, addr string, tlsInfo *transport.TLSInfo) (chan struct{}, chan struct{}) {
	t.Helper()
	peers := []raft.Peer{
		{ID: 1, Context: []byte("https://" + addr)},
	}
	tr := New(1, peers, zap.NewExample(), &fakeRaft{}, tlsInfo)
	stopC := make(chan struct{})
	doneC := make(chan struct{})
	go func() {
		defer close(doneC)
		// Start returns the underlying http.Server.Serve error on
		// shutdown — always non-nil. We swallow it because the test
		// fixture only cares about "the goroutine exited."
		_ = tr.Start(stopC)
	}()
	return stopC, doneC
}

// pickFreeAddr allocates a loopback port, closes it immediately, and
// returns "127.0.0.1:<port>". The caller re-binds to it. The race
// between close and re-bind is real but tiny; the existing
// raft_test.go:pickFreePorts helper uses the same pattern.
func pickFreeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

// waitUntilListening polls for connectivity to addr up to a few
// seconds. Needed because startTLSTransport spawns Start on a
// goroutine — the listener isn't guaranteed to be bound by the time
// we try to dial.
func waitUntilListening(t *testing.T, addr string) {
	t.Helper()
	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return true
		}
		return false
	}, 3*time.Second, 10*time.Millisecond, "transport did not start listening on %s", addr)
}

func clientTLSConfig(t *testing.T, caFile, certFile, keyFile string) *tls.Config {
	t.Helper()
	certPair, err := tls.LoadX509KeyPair(certFile, keyFile)
	require.NoError(t, err)
	return &tls.Config{
		Certificates: []tls.Certificate{certPair},
		RootCAs:      loadCAPool(t, caFile),
		ServerName:   "localhost",
	}
}

func loadCAPool(t *testing.T, caFile string) *x509.CertPool {
	t.Helper()
	pem, err := os.ReadFile(caFile)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(pem), "CA file did not contain any certs")
	return pool
}
