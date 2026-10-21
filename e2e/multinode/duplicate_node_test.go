//go:build multinode

package multinode_test

import (
	"fmt"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// spawn starts a node without waiting for it to become healthy, and returns a
// channel closed when the process exits.
func (n *clusterNode) spawn(t *testing.T) <-chan struct{} {
	t.Helper()
	cmd := exec.Command(binPath, "node") //nolint:gosec // G204: the test's own binary
	cmd.Env = n.env
	cmd.Stdout = testWriter{t, fmt.Sprintf("node%d:out", n.id), &n.logs}
	cmd.Stderr = testWriter{t, fmt.Sprintf("node%d:err", n.id), &n.logs}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, cmd.Start(), "spawn committed node %d", n.id)
	n.cmd = cmd
	n.stopped = false
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(exited)
	}()
	t.Cleanup(func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-exited
	})
	return exited
}

func hasExited(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

// restoreWithDuplicate runs a three-node cluster, backs it up, and restores
// it onto new machines — plus a fourth machine restored from the same archive
// and seed as node 1, the copy/paste slip the total-loss procedure invites.
// It returns the restored nodes, moved to new addresses, and the duplicate.
func restoreWithDuplicate(t *testing.T) ([]*clusterNode, *clusterNode) {
	t.Helper()
	nodes := startCluster(t, 3)
	writeAndCheck(t, nodes, "before_loss")
	for _, n := range nodes {
		n.stopGraceful(t)
	}
	archive := filepath.Join(t.TempDir(), "cluster.tar.gz")
	runCommitted(t, "backup", "--data", nodes[0].dataDir, "--to", archive, "--node-id", "1")
	seed := restoreAll(t, nodes, archive)
	duplicate := &clusterNode{id: 1, dataDir: filepath.Join(t.TempDir(), "restored")}
	runCommitted(t, "restore", "--from", archive, "--data", duplicate.dataDir, "--node-id", "1", "--cluster-id", seed)

	moveTo(t, nodes)
	duplicate.apiPort, duplicate.raftPort = freePort(t), freePort(t)
	duplicate.env = nodeEnv(1, duplicate.dataDir, duplicate.apiPort, peersOf([]*clusterNode{duplicate, nodes[1], nodes[2]}), false)
	return nodes, duplicate
}

const duplicateRefusal = "another process is running as"

// writeAndCheckOnceElected is writeAndCheck for a cluster that may be
// re-electing (its leader just stopped): the write is retried until a leader
// confirms it.
func writeAndCheckOnceElected(t *testing.T, nodes []*clusterNode, typ string) {
	t.Helper()
	body := fmt.Sprintf("[type]\nname = %q\n", typ)
	deadline := time.Now().Add(60 * time.Second)
	for {
		resp, err := http.Post(nodes[0].base()+"/v1/type/"+typ, "text/toml", strings.NewReader(body)) //nolint:gosec // G107: fixed loopback URL
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				break
			}
		}
		require.True(t, time.Now().Before(deadline), "the cluster never confirmed a write")
		time.Sleep(200 * time.Millisecond)
	}
	for _, n := range nodes {
		requireTypeListed(t, n.base(), typ)
	}
}

// Both machines restored as node 1 start together, before the cluster has
// recorded either: each could grant a vote in the same term and elect two
// leaders, so the cluster refuses the duplicate — a node that sees both
// refuses both, since it can't tell which is the original. At least one
// stops, naming the reason, at most one keeps running as node 1, and the
// cluster keeps committing.
func TestPeers_DuplicateNodeIDIsRefused(t *testing.T) {
	buildBinary(t)
	nodes, duplicate := restoreWithDuplicate(t)

	original := nodes[0].spawn(t)
	copied := duplicate.spawn(t)
	for _, n := range nodes[1:] {
		n.start(t)
		t.Cleanup(func() { n.kill(t) })
	}

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if (hasExited(original) || hasExited(copied)) && (nodes[0].logs.contains(duplicateRefusal) || duplicate.logs.contains(duplicateRefusal)) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.True(t, hasExited(original) || hasExited(copied), "a process running as a duplicate node id stops")
	require.True(t, nodes[0].logs.contains(duplicateRefusal) || duplicate.logs.contains(duplicateRefusal), "and says why")

	writeAndCheckOnceElected(t, nodes[1:], "after_duplicate")
	require.True(t, hasExited(original) || hasExited(copied), "at most one process keeps running as node 1")
}

// Once the cluster has recorded node 1's incarnation, a second process
// started as node 1 is refused alone: it stops, and the original keeps
// running and voting.
func TestPeers_DuplicateNodeIDAfterRecordStopsOnlyTheDuplicate(t *testing.T) {
	buildBinary(t)
	nodes, duplicate := restoreWithDuplicate(t)
	for _, n := range nodes {
		n.start(t)
		t.Cleanup(func() { n.kill(t) })
	}
	for _, n := range nodes {
		waitReady(t, n.base())
		n.waitForLog(t, "recorded this node's incarnation", 30*time.Second)
	}
	writeAndCheck(t, nodes, "recorded")

	copied := duplicate.spawn(t)
	deadline := time.Now().Add(60 * time.Second)
	for !hasExited(copied) && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	require.True(t, hasExited(copied), "the duplicate stops")
	require.True(t, duplicate.logs.contains(duplicateRefusal), "and says why")
	require.False(t, nodes[0].logs.contains(duplicateRefusal), "the original is never refused")
	writeAndCheck(t, nodes, "after_duplicate")
}
