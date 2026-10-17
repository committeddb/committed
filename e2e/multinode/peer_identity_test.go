//go:build multinode

package multinode_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

// recordedPeerURL is node id's recorded peer address as base reports it.
func recordedPeerURL(t *testing.T, base string, id uint64) string {
	t.Helper()
	resp, err := http.Get(base + "/v1/membership") //nolint:gosec // G107: fixed loopback URL
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	var m struct {
		Members []struct {
			ID      uint64 `json:"id"`
			PeerURL string `json:"peerUrl"`
		} `json:"members"`
	}
	if json.NewDecoder(resp.Body).Decode(&m) != nil {
		return ""
	}
	for _, mem := range m.Members {
		if mem.ID == id {
			return mem.PeerURL
		}
	}
	return ""
}

func waitRecorded(t *testing.T, nodes []*clusterNode, id uint64, want string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, n := range nodes {
			if recordedPeerURL(t, n.base(), id) != want {
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("node %d's address %s was not recorded on every node within 30s", id, want)
}

// One node moves and nobody touches the others: it rejoins on its own (its
// election attempts carry its new address; the others switch once their old
// address for it stops answering) and the leader records the new address
// cluster-wide. Restarting the others with their old, unedited
// COMMITTED_PEERS then keeps the recorded address. Before the fix the moved
// node stayed cut off and the cluster silently ran without it.
func TestPeers_OneNodeMovesAndHealsOnItsOwn(t *testing.T) {
	buildBinary(t)
	nodes := startCluster(t, 3)
	writeAndCheck(t, nodes, "before_move")
	oldPeers := peersOf(nodes)

	moved := nodes[2]
	moved.stopGraceful(t)
	moved.raftPort = freePort(t)
	moved.env = nodeEnv(moved.id, moved.dataDir, moved.apiPort, peersOf(nodes), false)
	moved.start(t)
	writeAndCheck(t, nodes, "after_move")

	newAddr := fmt.Sprintf("http://127.0.0.1:%d", moved.raftPort)
	waitRecorded(t, nodes, moved.id, newAddr)

	for _, n := range nodes[:2] {
		n.stopGraceful(t)
		n.env = nodeEnv(n.id, n.dataDir, n.apiPort, oldPeers, false) // still names node 3's old address
		n.start(t)
		waitReady(t, n.base())
	}
	writeAndCheck(t, nodes, "after_restarting_the_others")
}

// A learner never campaigns, so a moved learner can't announce itself; the
// operator records its new address and it catches up there.
func TestPeers_MovedLearnerViaSetAddress(t *testing.T) {
	buildBinary(t)
	nodes := startCluster(t, 3)
	learner := &clusterNode{id: 4, dataDir: t.TempDir(), apiPort: freePort(t), raftPort: freePort(t)}
	withLearner := append(append([]*clusterNode(nil), nodes...), learner)
	learner.env = nodeEnv(4, learner.dataDir, learner.apiPort, peersOf(withLearner), true)
	learner.start(t)
	t.Cleanup(func() { learner.kill(t) })
	postJSON(t, http.MethodPost, nodes[0].base()+"/v1/membership",
		fmt.Sprintf(`{"id":4,"url":"http://127.0.0.1:%d","learner":true}`, learner.raftPort))
	writeAndCheck(t, withLearner, "learner_caught_up")

	learner.stopGraceful(t)
	learner.raftPort = freePort(t)
	learner.env = nodeEnv(4, learner.dataDir, learner.apiPort, peersOf(withLearner), true)
	learner.start(t)
	newAddr := fmt.Sprintf("http://127.0.0.1:%d", learner.raftPort)
	postJSON(t, http.MethodPut, nodes[1].base()+"/v1/membership/4/address", fmt.Sprintf(`{"url":%q}`, newAddr))
	waitRecorded(t, nodes, 4, newAddr)
	writeAndCheck(t, withLearner, "after_learner_moved")
}

// The membership API refuses an add-learner naming a voter (it would demote
// it) and the re-add of a removed id (a removed node rejoins under a new id).
func TestPeers_RemovedIDAndVoterAsLearnerRefused(t *testing.T) {
	buildBinary(t)
	nodes := startCluster(t, 3)
	learner := &clusterNode{id: 4, dataDir: t.TempDir(), apiPort: freePort(t), raftPort: freePort(t)}
	withLearner := append(append([]*clusterNode(nil), nodes...), learner)
	learner.env = nodeEnv(4, learner.dataDir, learner.apiPort, peersOf(withLearner), true)
	learner.start(t)
	t.Cleanup(func() { learner.kill(t) })
	addLearner := fmt.Sprintf(`{"id":4,"url":"http://127.0.0.1:%d","learner":true}`, learner.raftPort)
	postJSON(t, http.MethodPost, nodes[0].base()+"/v1/membership", addLearner)
	writeAndCheck(t, withLearner, "learner_caught_up")

	status, body := requestJSON(t, http.MethodPost, nodes[1].base()+"/v1/membership",
		fmt.Sprintf(`{"id":2,"url":"http://127.0.0.1:%d","learner":true}`, nodes[1].raftPort))
	require.Equal(t, http.StatusBadRequest, status, body)
	require.Contains(t, body, "would demote it")

	postJSON(t, http.MethodDelete, nodes[0].base()+"/v1/membership/4", "")
	status, body = requestJSON(t, http.MethodPost, nodes[2].base()+"/v1/membership", addLearner)
	require.Equal(t, http.StatusBadRequest, status, body)
	require.Contains(t, body, "was removed from this cluster")
	writeAndCheck(t, nodes, "after_refusals")
}

func postJSON(t *testing.T, method, url, body string) {
	t.Helper()
	status, out := requestJSON(t, method, url, body)
	require.Lessf(t, status, 300, "%s %s: %d %s", method, url, status, out)
}

func requestJSON(t *testing.T, method, url, body string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(out)
}
