//go:build upgrade

package upgrade_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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

// A 3-node 0.8.2 cluster upgrades to this build the way 0.8.3 requires: stop
// every node, convert each node's tidwall event log, start them all. A node
// started before its conversion refuses and names the command. Once every
// member runs this build the leader mints one cluster id that every node
// takes, and a full restart afterwards still forms (the nodes are now strict
// about ids).
func TestUpgrade_StoppedClusterConvertsAndGainsAnID(t *testing.T) {
	oldBin := buildTagBinary(t, "v0.8.2")
	newBin := buildBinary(t)

	nodes := make([]*rollingNode, 3)
	pairs := make([]string, 3)
	for i := range nodes {
		nodes[i] = &rollingNode{id: uint64(i + 1), dataDir: t.TempDir(), apiPort: freePort(t), raftPort: freePort(t)}
		pairs[i] = fmt.Sprintf("%d=http://127.0.0.1:%d", i+1, nodes[i].raftPort)
	}
	peers := strings.Join(pairs, ",")
	for _, n := range nodes {
		n.start(t, oldBin, peers)
		t.Cleanup(func() { n.stop(t) })
	}
	for _, n := range nodes {
		waitReady(t, n.base())
	}
	postType(t, nodes[0].base(), "on_v082")
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
}
