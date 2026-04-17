package httptransport

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// testPKI holds filesystem paths to a test certificate authority and
// two node certs signed by it. Each mTLS test generates its own PKI
// into a t.TempDir() so tests don't share state, and certificate
// expiry dates are set far enough in the future that nothing in CI
// can age past them.
type testPKI struct {
	caFile  string
	caPEM   []byte
	caKey   *rsa.PrivateKey
	caCert  *x509.Certificate
	nodeDir string
}

// newTestPKI generates a fresh CA into t.TempDir() and returns paths
// for downstream tests. Individual node certs are minted via
// issueNodeCert — that split lets the negative-path tests issue a
// cert from a *different* CA to simulate an intruder.
func newTestPKI(t *testing.T) *testPKI {
	t.Helper()
	dir := t.TempDir()

	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "committed-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	caPath := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caPath, caPEM, 0o600))

	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	return &testPKI{
		caFile:  caPath,
		caPEM:   caPEM,
		caKey:   caKey,
		caCert:  caCert,
		nodeDir: dir,
	}
}

// issueNodeCert mints a cert signed by the test CA with the given
// common name. The resulting cert chain is valid for both server and
// client roles — rafthttp uses the same cert on both sides. The cert
// includes loopback SANs (127.0.0.1 + localhost) so the etcd
// TLSInfo.ServerConfig default SAN verification passes when peers
// dial "https://127.0.0.1:...".
func (p *testPKI) issueNodeCert(t *testing.T, name string) (certFile, keyFile string) {
	t.Helper()
	return p.issueNodeCertWithSigner(t, name, p.caCert, p.caKey)
}

// issueNodeCertWithSigner is the flexible variant used by
// negative-path tests that need a cert signed by a *different* CA.
func (p *testPKI) issueNodeCertWithSigner(t *testing.T, name string, signerCert *x509.Certificate, signerKey *rsa.PrivateKey) (certFile, keyFile string) {
	t.Helper()
	nodeKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	nodeTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		DNSNames:     []string{"localhost"},
	}
	nodeDER, err := x509.CreateCertificate(rand.Reader, nodeTmpl, signerCert, &nodeKey.PublicKey, signerKey)
	require.NoError(t, err)

	nodePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: nodeDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(nodeKey)})

	certFile = filepath.Join(p.nodeDir, name+".pem")
	keyFile = filepath.Join(p.nodeDir, name+".key")
	require.NoError(t, os.WriteFile(certFile, nodePEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))
	return certFile, keyFile
}
