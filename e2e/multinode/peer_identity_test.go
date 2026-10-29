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

// restoreAll restores every node from one archive into a fresh directory,
// as the documented total-loss procedure does: the first with --new-cluster,
// the rest with the seed it printed. The original directories are gone.
func restoreAll(t *testing.T, nodes []*clusterNode, archive string) string {
	t.Helper()
	var seed string
	for _, n := range nodes {
		restored := filepath.Join(t.TempDir(), "restored")
		args := []string{"restore", "--from", archive, "--data", restored, "--node-id", fmt.Sprint(n.id)}
		if seed == "" {
			out := runCommitted(t, append(args, "--new-cluster")...)
			seed = seedFrom(t, out)
		} else {
			runCommitted(t, append(args, "--cluster-id", seed)...)
		}
		require.NoError(t, os.RemoveAll(n.dataDir), "the original machine is gone")
		n.dataDir = restored
	}
	return seed
}

// seedFrom is the cluster seed restore --new-cluster printed.
func seedFrom(t *testing.T, out string) string {
	t.Helper()
	const marker = "with --cluster-id "
	i := strings.Index(out, marker)
	require.GreaterOrEqual(t, i, 0, "restore --new-cluster prints a seed: %s", out)
	return strings.Fields(out[i+len(marker):])[0]
}

// A total-loss restore onto new machines: every node's data directory is
// gone, each machine is restored from one archive as a different member, and
// the cluster starts on new addresses. Before the fix it never formed. A
// restored directory refuses to start as a node other than the one it was
// restored as.
func TestPeers_RestoreOntoNewAddresses(t *testing.T) {
	buildBinary(t)
	nodes := startCluster(t, 3)
	writeAndCheck(t, nodes, "before_loss")
	for _, n := range nodes {
		n.stopGraceful(t)
	}

	archive := filepath.Join(t.TempDir(), "cluster.tar.gz")
	runCommitted(t, "backup", "--data", nodes[0].dataDir, "--to", archive, "--node-id", "1")
	restoreAll(t, nodes, archive)

	moveTo(t, nodes)
	wrong := exec.Command(binPath, "node") //nolint:gosec // G204: the test's own binary
	wrong.Env = nodeEnv(2, nodes[0].dataDir, nodes[0].apiPort, peersOf(nodes), false)
	out, err := wrong.CombinedOutput()
	require.Error(t, err, "a directory restored as node 1 does not start as node 2")
	require.Contains(t, string(out), "restored or recovered as another node")

	for _, n := range nodes {
		n.start(t)
	}
	for _, n := range nodes {
		waitReady(t, n.base())
	}
	requireTypeListed(t, nodes[1].base(), "before_loss")
	writeAndCheck(t, nodes, "after_restore")
}

// Machines restored from archives taken at different times can't form one
// cluster, even with one seed: node 3's archive is older, so its cluster id
// differs and the others refuse it. (Mixing them could make raft panic or
// truncate committed history.)
func TestPeers_RestoreFromMixedArchivesIsRefused(t *testing.T) {
	buildBinary(t)
	nodes := startCluster(t, 3)
	writeAndCheck(t, nodes, "first")
	nodes[2].stopGraceful(t)
	older := filepath.Join(t.TempDir(), "older.tar.gz")
	runCommitted(t, "backup", "--data", nodes[2].dataDir, "--to", older, "--node-id", "3")
	nodes[2].start(t)
	waitReady(t, nodes[2].base())
	writeAndCheck(t, nodes, "second")
	for _, n := range nodes {
		n.stopGraceful(t)
	}
	newer := filepath.Join(t.TempDir(), "newer.tar.gz")
	runCommitted(t, "backup", "--data", nodes[0].dataDir, "--to", newer, "--node-id", "1")

	seed := restoreAll(t, nodes[:2], newer)
	restored := filepath.Join(t.TempDir(), "restored")
	runCommitted(t, "restore", "--from", older, "--data", restored, "--node-id", "3", "--cluster-id", seed)
	nodes[2].dataDir = restored

	moveTo(t, nodes)
	for _, n := range nodes {
		n.start(t)
	}
	waitReady(t, nodes[0].base())
	waitReady(t, nodes[1].base())
	writeAndCheck(t, nodes[:2], "third")
	nodes[2].waitForLog(t, "refused a peer request from another cluster", 30*time.Second)
	require.False(t, statusOK(nodes[2].base()+"/ready"), "node 3 never joins")
}

// The majority is lost: nodes 2 and 3 are gone. recover makes node 1 the
// only member of a new cluster with its whole log; it commits on its own,
// refuses the old members, and grows again with a new node.
func TestPeers_RecoverFromLostMajority(t *testing.T) {
	buildBinary(t)
	nodes := startCluster(t, 3)
	writeAndCheck(t, nodes, "before_loss")
	for _, n := range nodes {
		n.stopGraceful(t)
	}

	survivor := nodes[0]
	out := runCommitted(t, "recover", "--data", survivor.dataDir, "--node-id", "1")
	require.Contains(t, out, "removed members: 2, 3")
	survivor.env = nodeEnv(1, survivor.dataDir, survivor.apiPort, peersOf(nodes[:1]), false)
	survivor.start(t)
	waitReady(t, survivor.base())
	requireTypeListed(t, survivor.base(), "before_loss")
	writeAndCheck(t, nodes[:1], "after_recover")

	// An old member that comes back with its old data is refused.
	nodes[1].start(t)
	survivor.waitForLog(t, "refused a peer request from another cluster", 30*time.Second)
	nodes[1].kill(t)

	fresh := &clusterNode{id: 4, dataDir: t.TempDir(), apiPort: freePort(t), raftPort: freePort(t)}
	grown := []*clusterNode{survivor, fresh}
	fresh.env = nodeEnv(4, fresh.dataDir, fresh.apiPort, peersOf(grown), true)
	fresh.start(t)
	t.Cleanup(func() { fresh.kill(t) })
	addMember(t, survivor.base(), 4, fmt.Sprintf("http://127.0.0.1:%d", fresh.raftPort))
	waitReady(t, fresh.base())
	requireTypeListed(t, fresh.base(), "before_loss")
	writeAndCheck(t, grown, "after_growing")
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

// runCommitted runs the committed binary with args, fails the test on error,
// and returns its output.
func runCommitted(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command(binPath, args...).CombinedOutput() //nolint:gosec // G204: the test's own binary
	require.NoError(t, err, "committed %s: %s", strings.Join(args, " "), out)
	return string(out)
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
