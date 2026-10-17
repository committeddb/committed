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

// A 3-node 0.8.2 cluster upgraded one node at a time to this build: every
// step commits (the new identity headers never split a mixed cluster), and
// once every member runs this build the leader mints one cluster id that
// every node takes. A full restart afterwards still forms (the nodes are now
// strict about ids).
func TestRollingUpgrade_ClusterGainsAnID(t *testing.T) {
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

	for i, n := range nodes {
		n.stop(t)
		n.start(t, newBin, peers)
		waitReady(t, n.base())
		typ := fmt.Sprintf("after_upgrading_%d", i+1)
		postType(t, nodes[(i+1)%3].base(), typ)
		for _, m := range nodes {
			requireTypeListed(t, m.base(), typ)
		}
		if i < 2 {
			require.Empty(t, clusterIDOf(t, n), "no id is minted while a 0.8.2 member remains")
		}
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
