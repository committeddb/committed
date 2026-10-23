//go:build multinode

package multinode_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The rest of the peer-address scenarios (see peer_identity_test.go).

// A node added at runtime moves: its address was recorded when it was added,
// not given by anyone's COMMITTED_PEERS, and it still heals on its own.
func TestPeers_AddedNodeMoves(t *testing.T) {
	buildBinary(t)
	nodes := startCluster(t, 3)
	added := &clusterNode{id: 4, dataDir: t.TempDir(), apiPort: freePort(t), raftPort: freePort(t)}
	all := append(append([]*clusterNode(nil), nodes...), added)
	added.env = nodeEnv(4, added.dataDir, added.apiPort, peersOf(all), true)
	added.start(t)
	t.Cleanup(func() { added.kill(t) })
	addMember(t, nodes[0].base(), 4, fmt.Sprintf("http://127.0.0.1:%d", added.raftPort))
	waitReady(t, added.base())
	writeAndCheck(t, all, "before_move")

	added.stopGraceful(t)
	added.raftPort = freePort(t)
	added.env = nodeEnv(4, added.dataDir, added.apiPort, peersOf(all), true)
	added.start(t)
	waitRecorded(t, nodes, 4, fmt.Sprintf("http://127.0.0.1:%d", added.raftPort))
	writeAndCheck(t, all, "after_move")
}

// A lost node is replaced by a fresh one under a new id at the old node's
// address (rebuild.md). Until the old id is removed the leader keeps
// addressing it there, and the new node refuses those messages as not its
// own instead of acting on them.
func TestPeers_ReplacementAtTheOldAddress(t *testing.T) {
	buildBinary(t)
	nodes := startCluster(t, 3)
	writeAndCheck(t, nodes, "before_loss")
	lost := nodes[2]
	lost.kill(t)

	replacement := &clusterNode{id: 4, dataDir: t.TempDir(), apiPort: lost.apiPort, raftPort: lost.raftPort}
	all := append(append([]*clusterNode(nil), nodes...), replacement)
	replacement.env = nodeEnv(4, replacement.dataDir, replacement.apiPort, peersOf(all), true)
	replacement.start(t)
	t.Cleanup(func() { replacement.kill(t) })
	replacement.waitForLog(t, "refused a peer request addressed to another node", 30*time.Second)

	// rebuild.md's flow: a learner (no effect on quorum while it catches up),
	// promoted, and only then the old id removed.
	postJSON(t, http.MethodPost, nodes[0].base()+"/v1/membership",
		fmt.Sprintf(`{"id":4,"url":"http://127.0.0.1:%d","learner":true}`, replacement.raftPort))
	waitReady(t, replacement.base())
	postJSON(t, http.MethodPost, nodes[0].base()+"/v1/membership/4/promote", "")
	postJSON(t, http.MethodDelete, nodes[0].base()+"/v1/membership/3", "")
	writeAndCheck(t, []*clusterNode{nodes[0], nodes[1], replacement}, "after_replacement")
}

// A restore started beside its still-running source, even one pointed at
// the source's addresses, can't exchange a message with it: the source
// refuses it and keeps committing, and the restored node never forms.
func TestPeers_RestoreBesideTheSourceCantReachIt(t *testing.T) {
	buildBinary(t)
	nodes := startCluster(t, 3)
	writeAndCheck(t, nodes, "before_backup")
	nodes[0].stopGraceful(t)
	archive := filepath.Join(t.TempDir(), "cluster.tar.gz")
	runCommitted(t, "backup", "--data", nodes[0].dataDir, "--to", archive, "--node-id", "1")
	nodes[0].start(t)
	waitReady(t, nodes[0].base())

	restored := &clusterNode{id: 1, dataDir: filepath.Join(t.TempDir(), "restored"), apiPort: freePort(t), raftPort: freePort(t)}
	runCommitted(t, "restore", "--from", archive, "--data", restored.dataDir, "--node-id", "1", "--new-cluster")
	aimed := []*clusterNode{restored, nodes[1], nodes[2]}
	restored.env = nodeEnv(1, restored.dataDir, restored.apiPort, peersOf(aimed), false)
	restored.start(t)
	t.Cleanup(func() { restored.kill(t) })

	refused := func() bool {
		return nodes[1].logs.contains("refused a peer request from another cluster") ||
			nodes[2].logs.contains("refused a peer request from another cluster")
	}
	deadline := time.Now().Add(30 * time.Second)
	for !refused() && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	require.True(t, refused(), "the source refuses the restored node")
	writeAndCheck(t, nodes, "source_keeps_committing")
	require.False(t, statusOK(restored.base()+"/ready"), "the restored node never forms with the source")
}

// Switching a running cluster's peers to https (authentication.md): stop
// every node, start each with peer mTLS and every COMMITTED_PEERS entry
// changed to https. The changed entries win over the recorded http
// addresses.
func TestPeers_SwitchToHTTPS(t *testing.T) {
	buildBinary(t)
	nodes := startCluster(t, 3)
	writeAndCheck(t, nodes, "on_http")
	for _, n := range nodes {
		n.stopGraceful(t)
	}

	ca, cert, key := peerPKI(t)
	pairs := make([]string, len(nodes))
	for i, n := range nodes {
		pairs[i] = fmt.Sprintf("%d=https://127.0.0.1:%d", n.id, n.raftPort)
	}
	peers := strings.Join(pairs, ",")
	for _, n := range nodes {
		n.env = nodeEnv(n.id, n.dataDir, n.apiPort, peers, false,
			"COMMITTED_PEER_TLS_CA_FILE="+ca, "COMMITTED_PEER_TLS_CERT_FILE="+cert, "COMMITTED_PEER_TLS_KEY_FILE="+key)
		n.start(t)
	}
	for _, n := range nodes {
		waitReady(t, n.base())
	}
	requireTypeListed(t, nodes[2].base(), "on_http")
	// Peer TLS refuses plaintext, so committing at all proves the peers
	// dial each other's https addresses.
	writeAndCheck(t, nodes, "on_https")

	// A later restart with the same env still forms: nothing goes back to
	// the old http addresses.
	for _, n := range nodes {
		n.stopGraceful(t)
	}
	for _, n := range nodes {
		n.start(t)
	}
	for _, n := range nodes {
		waitReady(t, n.base())
	}
	writeAndCheck(t, nodes, "after_restart_on_https")
}

// peerPKI writes a CA and one loopback cert, good for server and client,
// that every node of the test shares.
func peerPKI(t *testing.T) (caFile, certFile, keyFile string) {
	t.Helper()
	dir := t.TempDir()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "committed-test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	nodeKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	nodeTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "committed-test-node"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, DNSNames: []string{"localhost"},
	}
	nodeDER, err := x509.CreateCertificate(rand.Reader, nodeTmpl, caCert, &nodeKey.PublicKey, caKey)
	require.NoError(t, err)

	caFile, certFile, keyFile = filepath.Join(dir, "ca.pem"), filepath.Join(dir, "node.pem"), filepath.Join(dir, "node.key")
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600))
	require.NoError(t, os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: nodeDER}), 0o600))
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(nodeKey)}), 0o600))
	return caFile, certFile, keyFile
}
