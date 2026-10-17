//go:build multinode

package multinode_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Peer identity and addresses, at process level: real binaries, real data
// directories, real restarts. Each test is a scenario that never formed
// before the fix (see the peer-identity-and-addresses ticket).

// moveTo gives every node fresh ports and the COMMITTED_PEERS naming them.
func moveTo(t *testing.T, nodes []*clusterNode) {
	t.Helper()
	for _, n := range nodes {
		n.apiPort, n.raftPort = freePort(t), freePort(t)
	}
	peers := peersOf(nodes)
	for _, n := range nodes {
		n.env = nodeEnv(n.id, n.dataDir, n.apiPort, peers, false)
	}
}

// writeAndCheck proves the cluster commits: a new type is accepted and every
// node lists it.
func writeAndCheck(t *testing.T, nodes []*clusterNode, typ string) {
	t.Helper()
	postType(t, nodes[0].base(), typ)
	for _, n := range nodes {
		requireTypeListed(t, n.base(), typ)
	}
}

// Every node restarts on a new address with COMMITTED_PEERS updated, data
// intact. Before the fix the recorded first-boot addresses won and the
// cluster never elected a leader.
func TestPeers_EveryNodeMovesAddress(t *testing.T) {
	buildBinary(t)
	nodes := startCluster(t, 3)
	writeAndCheck(t, nodes, "before_move")

	for _, n := range nodes {
		n.stopGraceful(t)
	}
	moveTo(t, nodes)
	for _, n := range nodes {
		n.start(t)
	}
	for _, n := range nodes {
		waitReady(t, n.base())
	}
	requireTypeListed(t, nodes[2].base(), "before_move")
	writeAndCheck(t, nodes, "after_move")
}

// A total-loss restore onto new machines: every node's data directory is
// gone, each is restored from a backup into a fresh directory, and the
// cluster starts on new addresses. Before the fix it never formed.
func TestPeers_RestoreOntoNewAddresses(t *testing.T) {
	buildBinary(t)
	nodes := startCluster(t, 3)
	writeAndCheck(t, nodes, "before_loss")
	for _, n := range nodes {
		n.stopGraceful(t)
	}

	archives := t.TempDir()
	for _, n := range nodes {
		archive := filepath.Join(archives, fmt.Sprintf("node%d.tar.gz", n.id))
		runCommitted(t, "backup", "--data", n.dataDir, "--to", archive, "--node-id", fmt.Sprint(n.id))
		restored := filepath.Join(t.TempDir(), "restored")
		runCommitted(t, "restore", "--from", archive, "--data", restored)
		require.NoError(t, os.RemoveAll(n.dataDir), "the original machine is gone")
		n.dataDir = restored
	}
	moveTo(t, nodes)
	for _, n := range nodes {
		n.start(t)
	}
	for _, n := range nodes {
		waitReady(t, n.base())
	}
	requireTypeListed(t, nodes[1].base(), "before_loss")
	writeAndCheck(t, nodes, "after_restore")
}

// Every node of a new cluster derives the same cluster id at its first boot.
func TestPeers_ClusterIDAgreed(t *testing.T) {
	buildBinary(t)
	nodes := startCluster(t, 3)
	ids := map[string]bool{}
	for _, n := range nodes {
		raw, err := os.ReadFile(filepath.Join(n.dataDir, "cluster-id"))
		require.NoError(t, err)
		ids[strings.TrimSpace(string(raw))] = true
	}
	require.Len(t, ids, 1, "every node has the same cluster id: %v", ids)
}

// A node from another cluster — here node 3, bootstrapped with a different
// COMMITTED_CLUSTER_NAME, so it derived a different id — is refused at the
// wire. The other two form a cluster and commit; neither exchanges a raft
// message with it.
func TestPeers_NodeFromAnotherClusterIsRefused(t *testing.T) {
	buildBinary(t)
	nodes := make([]*clusterNode, 3)
	for i := range nodes {
		nodes[i] = &clusterNode{id: uint64(i + 1), dataDir: t.TempDir(), apiPort: freePort(t), raftPort: freePort(t)}
	}
	peers := peersOf(nodes)
	for _, n := range nodes {
		var extra []string
		if n.id == 3 {
			extra = append(extra, "COMMITTED_CLUSTER_NAME=someone-else")
		}
		n.env = nodeEnv(n.id, n.dataDir, n.apiPort, peers, false, extra...)
		n.start(t)
		t.Cleanup(func() { n.kill(t) })
	}
	waitReady(t, nodes[0].base())
	waitReady(t, nodes[1].base())
	writeAndCheck(t, nodes[:2], "two_of_three")

	refused := func() bool {
		return nodes[0].logs.contains("refused a peer request from another cluster") ||
			nodes[1].logs.contains("refused a peer request from another cluster")
	}
	deadline := time.Now().Add(20 * time.Second)
	for !refused() && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	require.True(t, refused(), "the cluster refuses node 3's messages, loudly")
	nodes[2].waitForLog(t, "refused a peer request from another cluster", 20*time.Second)
	require.False(t, statusOK(nodes[2].base()+"/ready"), "node 3 never joins the cluster")
}

// runCommitted runs the committed binary with args and fails the test on error.
func runCommitted(t *testing.T, args ...string) {
	t.Helper()
	out, err := exec.Command(binPath, args...).CombinedOutput() //nolint:gosec // G204: the test's own binary
	require.NoError(t, err, "committed %s: %s", strings.Join(args, " "), out)
}
