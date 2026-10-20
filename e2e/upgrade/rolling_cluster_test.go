//go:build upgrade

package upgrade_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// rollingNode is one member of a multi-node cluster under upgrade.
type rollingNode struct {
	id       uint64
	dataDir  string
	apiPort  int
	raftPort int
	proc     *exec.Cmd
}

func (n *rollingNode) base() string { return fmt.Sprintf("http://127.0.0.1:%d", n.apiPort) }

func (n *rollingNode) start(t *testing.T, bin, peers string) {
	t.Helper()
	cmd := exec.Command(bin, "node")
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("COMMITTED_NODE_ID=%d", n.id),
		fmt.Sprintf("COMMITTED_API_ADDR=127.0.0.1:%d", n.apiPort),
		fmt.Sprintf("COMMITTED_API_URL=http://127.0.0.1:%d", n.apiPort),
		"COMMITTED_DATA_DIR="+n.dataDir,
		"COMMITTED_PEERS="+peers,
	)
	label := fmt.Sprintf("node%d", n.id)
	cmd.Stdout = testWriter{t, label + ":out"}
	cmd.Stderr = testWriter{t, label + ":err"}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, cmd.Start(), "spawn %s", label)
	n.proc = cmd
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if statusOK(n.base() + "/health") {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s did not become healthy within 30s", label)
}

func (n *rollingNode) stop(t *testing.T) {
	t.Helper()
	if n.proc == nil {
		return
	}
	(&nodeProcess{cmd: n.proc}).stopGraceful(t, n.base())
	n.proc = nil
}

func clusterIDOf(t *testing.T, n *rollingNode) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(n.dataDir, "cluster-id"))
	if os.IsNotExist(err) {
		return ""
	}
	require.NoError(t, err)
	return strings.TrimSpace(string(raw))
}

// refusesToStart runs this build's node over n's unconverted data directory
// and returns what it printed: it must exit, not serve.
func (n *rollingNode) refusesToStart(t *testing.T, bin, peers string) string {
	t.Helper()
	cmd := exec.Command(bin, "node") //nolint:gosec // G204: the test's own binary
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("COMMITTED_NODE_ID=%d", n.id),
		fmt.Sprintf("COMMITTED_API_ADDR=127.0.0.1:%d", n.apiPort),
		"COMMITTED_DATA_DIR="+n.dataDir,
		"COMMITTED_PEERS="+peers,
	)
	done := make(chan []byte, 1)
	go func() {
		out, _ := cmd.CombinedOutput()
		done <- out
	}()
	select {
	case out := <-done:
		require.False(t, cmd.ProcessState.Success(), "the node exits with an error")
		return string(out)
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("node%d kept running over an unconverted event log", n.id)
		return ""
	}
}

// scrubCompleted reports whether n has finished a requested scrub, from its
// node-local /node/status.
func (n *rollingNode) scrubCompleted(t *testing.T) bool {
	t.Helper()
	resp, err := http.Get(n.base() + "/v1/node/status") //nolint:gosec // G107: fixed loopback URL
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	var status struct {
		Scrub struct {
			PendingBound   uint64 `json:"pendingBound"`
			CompletedBound uint64 `json:"completedBound"`
		} `json:"scrub"`
	}
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&status) != nil {
		return false
	}
	return status.Scrub.CompletedBound > 0 && status.Scrub.CompletedBound >= status.Scrub.PendingBound
}

// newRollingCluster lays out n members on fresh ports and data directories,
// returning them and their COMMITTED_PEERS.
func newRollingCluster(t *testing.T, n int) ([]*rollingNode, string) {
	t.Helper()
	nodes := make([]*rollingNode, n)
	pairs := make([]string, n)
	for i := range nodes {
		nodes[i] = &rollingNode{id: uint64(i + 1), dataDir: t.TempDir(), apiPort: freePort(t), raftPort: freePort(t)}
		pairs[i] = fmt.Sprintf("%d=http://127.0.0.1:%d", i+1, nodes[i].raftPort)
	}
	return nodes, strings.Join(pairs, ",")
}

// A 3-node 0.8.2 cluster upgrades to this build the way 0.8.3 requires: stop
// every node, convert each node's tidwall event log, start them all. A node
// started before its conversion refuses and names the command. Once every
// member runs this build the leader mints one cluster id that every node
// takes. A scrub then erases a subject 0.8.2 wrote from every converted log,
// a full restart afterwards still forms (the nodes are now strict about ids),
// and a backup of a converted node restores as a new cluster that keeps the
// erasure.
func TestUpgrade_StoppedClusterConvertsAndGainsAnID(t *testing.T) {
	oldBin := buildTagBinary(t, "v0.8.2")
	newBin := buildBinary(t)

	nodes, peers := newRollingCluster(t, 3)
	for _, n := range nodes {
		n.start(t, oldBin, peers)
		t.Cleanup(func() { n.stop(t) })
	}
	for _, n := range nodes {
		waitReady(t, n.base())
	}
	postType(t, nodes[0].base(), "on_v082")
	postType(t, nodes[0].base(), "movie")
	requireTypeListed(t, nodes[0].base(), "movie")
	for _, body := range []string{
		`{"entities":[{"typeId":"movie","key":"mv1","data":{"title":"one"}}]}`,
		`{"entities":[{"typeId":"movie","key":"subject-erased","data":{"pii":"subject-erased"}}]}`,
		`{"entities":[{"typeId":"movie","key":"subject-erased","delete":true}]}`,
		`{"entities":[{"typeId":"movie","key":"mv2","data":{"title":"after-delete"}}]}`,
	} {
		postBody(t, nodes[0].base()+"/v1/proposal", "application/json", body)
	}
	for _, n := range nodes {
		n.stop(t)
	}

	out := nodes[0].refusesToStart(t, newBin, peers)
	require.Contains(t, out, "committed wal convert --data "+nodes[0].dataDir)

	for _, n := range nodes {
		runOrFail(t, newBin, "wal", "convert", "--data", n.dataDir)
	}
	for _, n := range nodes {
		n.start(t, newBin, peers)
	}
	for _, n := range nodes {
		waitReady(t, n.base())
		requireTypeListed(t, n.base(), "on_v082")
	}
	postType(t, nodes[1].base(), "after_upgrade")
	for _, n := range nodes {
		requireTypeListed(t, n.base(), "after_upgrade")
	}

	var id string
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		a, b, c := clusterIDOf(t, nodes[0]), clusterIDOf(t, nodes[1]), clusterIDOf(t, nodes[2])
		if a != "" && a == b && b == c {
			id = a
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	require.NotEmpty(t, id, "every node takes the minted cluster id")

	// The scrub rewrites every node's converted log: each reports it done,
	// and a fresh replay no longer sees the subject 0.8.2 wrote.
	postScrub(t, nodes[0].base())
	deadline = time.Now().Add(60 * time.Second)
	for _, n := range nodes {
		for !n.scrubCompleted(t) {
			if time.Now().After(deadline) {
				t.Fatalf("node%d never completed the scrub of its converted log", n.id)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	awaitErased(t, nodes[0].base(), "converted-verify", "mv1", "mv2")

	for _, n := range nodes {
		n.stop(t)
	}
	for _, n := range nodes {
		n.start(t, newBin, peers)
	}
	for _, n := range nodes {
		waitReady(t, n.base())
	}
	requireTypeListed(t, nodes[2].base(), "on_v082")
	postType(t, nodes[1].base(), "after_full_restart")
	for _, n := range nodes {
		requireTypeListed(t, n.base(), "after_full_restart")
		require.Equal(t, id, clusterIDOf(t, n))
	}

	// A backup of a converted node restores every member of a new cluster.
	for _, n := range nodes {
		n.stop(t)
	}
	archive := filepath.Join(t.TempDir(), "converted.tar.gz")
	runOrFail(t, newBin, "backup", "--data", nodes[0].dataDir, "--to", archive, "--node-id", "1")
	restored, restoredPeers := newRollingCluster(t, 3)
	seed := regexp.MustCompile(`--cluster-id ([0-9a-f-]+)`).FindStringSubmatch(
		runOrFail(t, newBin, "restore", "--from", archive, "--data", restored[0].dataDir, "--node-id", "1", "--new-cluster"))
	require.Len(t, seed, 2, "restore --new-cluster prints the cluster seed")
	for _, n := range restored[1:] {
		runOrFail(t, newBin, "restore", "--from", archive, "--data", n.dataDir, "--node-id", strconv.FormatUint(n.id, 10), "--cluster-id", seed[1])
	}
	for _, n := range restored {
		n.start(t, newBin, restoredPeers)
		t.Cleanup(func() { n.stop(t) })
	}
	for _, n := range restored {
		waitReady(t, n.base())
		requireTypeListed(t, n.base(), "after_full_restart")
	}
	awaitErased(t, restored[0].base(), "restored-verify", "mv1", "mv2")
	postType(t, restored[2].base(), "after_restore")
	for _, n := range restored {
		requireTypeListed(t, n.base(), "after_restore")
	}
}
