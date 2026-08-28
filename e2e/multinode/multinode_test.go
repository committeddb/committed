//go:build multinode

// Package multinode_test exercises the durability guarantee a distributed
// commit log sells, at the level nothing else covers: a REAL multi-node
// cluster of REAL binaries with REAL wal.Storage on disk, through real
// process crashes and restarts. The in-process multi-node tests
// (TestRaftRestart_Cluster3, the adversarial suite) share a process and
// reuse in-RAM raft storage across a "restart", so WAL-replay-then-catch-up
// — HardState/commit-index reconciliation from disk, then log catch-up from
// the majority — was exercised by no test before this harness.
//
// Two scenarios:
//   - crash recovery: SIGKILL a follower (a dirty crash, no shutdown path),
//     advance the log on the surviving majority, restart the follower over
//     its data dir, and assert it replays its WAL and converges.
//   - restart-after-grow: grow 3→4 via the membership API, then restart
//     EVERY node over its data dir WITHOUT updating COMMITTED_PEERS, and
//     assert the grown cluster re-forms and commits (the
//     raft-peer-url-not-persisted wedge, b0c9340, at process level — the
//     in-RAM harness replays ConfChanges from memory and masks it).
//
// Tagged `multinode` so it stays out of `make test`; run via
// `make test/multinode`.
package multinode_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// clusterNode is one running committed process plus the identity needed to
// restart it over the same state.
type clusterNode struct {
	id       uint64
	dataDir  string
	apiPort  int
	raftPort int
	env      []string // the exact env of the last start — restarts reuse it
	cmd      *exec.Cmd
	stopped  bool
}

func (n *clusterNode) base() string { return fmt.Sprintf("http://127.0.0.1:%d", n.apiPort) }

// TestMultiNodeCrashRecovery_FollowerReplaysWALAndCatchesUp boots a real
// 3-node cluster, commits replicated state, SIGKILLs a follower, advances
// the log on the majority, restarts the follower over its data dir, and
// asserts it recovers BOTH halves: the pre-crash entries from its own WAL
// (a dirty kill, so only durable state survives) and the post-crash entries
// by catching up from the majority — then proves full liveness by proposing
// through the recovered node.
func TestMultiNodeCrashRecovery_FollowerReplaysWALAndCatchesUp(t *testing.T) {
	buildBinary(t)
	nodes := startCluster(t, 3)

	// Replicated baseline: committed on the cluster, visible on every node.
	postType(t, nodes[0].base(), "canary-0")
	for _, n := range nodes {
		requireTypeListed(t, n.base(), "canary-0")
	}

	// Kill a FOLLOWER (dirty: SIGKILL the process group, no shutdown path).
	leader := leaderID(t, nodes[0].base())
	var follower *clusterNode
	var survivors []*clusterNode
	for _, n := range nodes {
		if follower == nil && n.id != leader {
			follower = n
			continue
		}
		survivors = append(survivors, n)
	}
	require.NotNil(t, follower, "a 3-node cluster must have a follower")
	follower.kill(t)

	// The majority keeps committing while the follower is down.
	for i := 1; i <= 5; i++ {
		postType(t, survivors[0].base(), fmt.Sprintf("canary-%d", i))
	}
	for _, n := range survivors {
		requireTypeListed(t, n.base(), "canary-5")
	}

	// Restart the follower over its data dir. Reaching /ready means it
	// replayed its on-disk WAL (dirty crash — RAM is gone; a node that
	// cannot read its state fails to start, not serve /ready).
	follower.restart(t)
	waitReady(t, follower.base())

	// Both recovery halves: the pre-crash entry from its own WAL, and the
	// entries the majority committed while it was down, via catch-up.
	for i := 0; i <= 5; i++ {
		requireTypeListed(t, follower.base(), fmt.Sprintf("canary-%d", i))
	}

	// Full liveness: the recovered node proposes (forwarding to the leader)
	// and the whole cluster converges on the result.
	postType(t, follower.base(), "canary-post-recovery")
	for _, n := range nodes {
		requireTypeListed(t, n.base(), "canary-post-recovery")
	}
}

// TestMultiNodeRestartAfterGrow_ReformsWithoutPeersUpdate grows a real
// 3-node cluster to 4 via the membership API, then restarts EVERY node over
// its data dir with COMMITTED_PEERS still naming the original three — the
// documented contract ("you do NOT need to update it after growing"), which
// only holds because a member added at runtime has its raft URL persisted
// and reconciled onto the transport on restart. The all-down-then-up shape
// is the strongest wedge repro: no live leader exists to paper over a
// missing transport entry.
func TestMultiNodeRestartAfterGrow_ReformsWithoutPeersUpdate(t *testing.T) {
	buildBinary(t)
	nodes := startCluster(t, 3)

	postType(t, nodes[0].base(), "pre-grow")
	for _, n := range nodes {
		requireTypeListed(t, n.base(), "pre-grow")
	}

	// Grow: start node 4 in join mode (no bootstrap; PEERS seeds its
	// transport), then commit the membership change naming it.
	n4 := &clusterNode{id: 4, dataDir: t.TempDir(), apiPort: freePort(t), raftPort: freePort(t)}
	joinPeers := fmt.Sprintf("%s,4=http://127.0.0.1:%d", peersOf(nodes), n4.raftPort)
	n4.env = nodeEnv(4, n4.dataDir, n4.apiPort, joinPeers, true)
	n4.start(t)
	t.Cleanup(func() { n4.kill(t) })
	addMember(t, nodes[0].base(), 4, fmt.Sprintf("http://127.0.0.1:%d", n4.raftPort))
	waitReady(t, n4.base())
	requireTypeListed(t, n4.base(), "pre-grow")

	// The wedge shape: everything down, then everything up, with nodes 1-3
	// still holding the ORIGINAL 3-node COMMITTED_PEERS.
	all := append(append([]*clusterNode{}, nodes...), n4)
	for _, n := range all {
		n.stopGraceful(t)
	}
	for _, n := range all {
		n.restart(t)
	}
	for _, n := range all {
		waitReady(t, n.base())
	}

	// The grown cluster re-formed a leader and commits: a fresh write lands
	// everywhere, including through/on the added node.
	postType(t, n4.base(), "post-grow-restart")
	for _, n := range all {
		requireTypeListed(t, n.base(), "post-grow-restart")
	}
}

// TestMultiNodeThroughput_Report is a MEASUREMENT, not a gate: it bursts
// data-plane proposals through a real 3-node cluster (round-robin across
// nodes, so ~2/3 exercise follower→leader forwarding) and reports wall-clock
// throughput and per-request latency percentiles for the published envelope
// (docs/operations/performance.md). The only assertion is an
// order-of-magnitude sanity floor — shared CI runners make percent-level
// thresholds flake (the lesson of the mysql-speedup incident), so
// release-to-release comparison happens on the reported numbers, not in an
// assert.
func TestMultiNodeThroughput_Report(t *testing.T) {
	const workers, perWorker = 8, 25

	buildBinary(t)
	nodes := startCluster(t, 3)
	postType(t, nodes[0].base(), "bench")
	for _, n := range nodes {
		requireTypeListed(t, n.base(), "bench")
	}

	var mu sync.Mutex
	latencies := make([]time.Duration, 0, workers*perWorker)
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				base := nodes[(w+i)%len(nodes)].base()
				t0 := time.Now()
				postProposal(t, base, "bench", fmt.Sprintf("k-%d-%d", w, i))
				d := time.Since(t0)
				mu.Lock()
				latencies = append(latencies, d)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	wall := time.Since(start)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	total := len(latencies)
	p50, p99 := latencies[total/2], latencies[total*99/100]
	perSec := float64(total) / wall.Seconds()
	t.Logf("THROUGHPUT REPORT: %d proposals, %d workers, wall=%s, %.0f proposals/sec, p50=%s, p99=%s",
		total, workers, wall.Round(time.Millisecond), perSec, p50.Round(time.Millisecond), p99.Round(time.Millisecond))

	require.Greater(t, perSec, 5.0,
		"order-of-magnitude floor only: a real regression to single-digit throughput means the write path lost its fsync coalescing")
}

// postProposal writes one 256-byte data-plane proposal to topic via base.
func postProposal(t *testing.T, base, topic, key string) {
	t.Helper()
	body := fmt.Sprintf(`{"entities":[{"typeId":%q,"key":%q,"data":{"pad":%q}}]}`,
		topic, key, strings.Repeat("x", 256))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/proposal", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	require.GreaterOrEqualf(t, resp.StatusCode, 200, "POST proposal: %d %s", resp.StatusCode, out)
	require.Lessf(t, resp.StatusCode, 300, "POST proposal: %d %s", resp.StatusCode, out)
}

// --- harness ---

// startCluster boots n fresh nodes (ids 1..n) sharing one COMMITTED_PEERS
// and waits for every node to serve /ready.
func startCluster(t *testing.T, count int) []*clusterNode {
	t.Helper()
	nodes := make([]*clusterNode, count)
	peerPairs := make([]string, count)
	for i := range nodes {
		nodes[i] = &clusterNode{
			id: uint64(i + 1), dataDir: t.TempDir(),
			apiPort: freePort(t), raftPort: freePort(t),
		}
		peerPairs[i] = fmt.Sprintf("%d=http://127.0.0.1:%d", i+1, nodes[i].raftPort)
	}
	peers := strings.Join(peerPairs, ",")
	for _, n := range nodes {
		n.env = nodeEnv(n.id, n.dataDir, n.apiPort, peers, false)
		n.start(t)
		t.Cleanup(func() { n.kill(t) })
	}
	for _, n := range nodes {
		waitReady(t, n.base())
	}
	return nodes
}

// peersOf reconstructs the id=url pairs the cluster was booted with.
func peersOf(nodes []*clusterNode) string {
	pairs := make([]string, len(nodes))
	for i, n := range nodes {
		pairs[i] = fmt.Sprintf("%d=http://127.0.0.1:%d", n.id, n.raftPort)
	}
	return strings.Join(pairs, ",")
}

func nodeEnv(id uint64, dataDir string, apiPort int, peers string, join bool) []string {
	env := append(os.Environ(),
		fmt.Sprintf("COMMITTED_NODE_ID=%d", id),
		fmt.Sprintf("COMMITTED_API_ADDR=127.0.0.1:%d", apiPort),
		// Announced so a follower can proxy leader-only reads (the
		// membership GET this harness uses to find the leader).
		fmt.Sprintf("COMMITTED_API_URL=http://127.0.0.1:%d", apiPort),
		"COMMITTED_DATA_DIR="+dataDir,
		"COMMITTED_PEERS="+peers,
	)
	if join {
		env = append(env, "COMMITTED_JOIN=true")
	}
	return env
}

// start spawns the node with its stored env and waits for /health only (a
// join-mode node is healthy but not ready until membership commits).
func (n *clusterNode) start(t *testing.T) {
	t.Helper()
	cmd := exec.Command(binPath, "node")
	cmd.Env = n.env
	cmd.Stdout = testWriter{t, fmt.Sprintf("node%d:out", n.id)}
	cmd.Stderr = testWriter{t, fmt.Sprintf("node%d:err", n.id)}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, cmd.Start(), "spawn committed node %d", n.id)
	n.cmd = cmd
	n.stopped = false

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if statusOK(n.base() + "/health") {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	n.kill(t)
	t.Fatalf("node %d did not become healthy within 30s", n.id)
}

func (n *clusterNode) restart(t *testing.T) {
	t.Helper()
	n.start(t)
}

// kill sends SIGKILL to the whole process group — a dirty crash: no signal
// handler, no shutdown path, nothing but what fsync already made durable.
func (n *clusterNode) kill(t *testing.T) {
	t.Helper()
	if n.stopped || n.cmd == nil || n.cmd.Process == nil {
		return
	}
	n.stopped = true
	_ = syscall.Kill(-n.cmd.Process.Pid, syscall.SIGKILL)
	_, _ = n.cmd.Process.Wait()
	waitPortFree(n.base())
}

// stopGraceful is the clean half: SIGTERM, bounded wait, port release.
func (n *clusterNode) stopGraceful(t *testing.T) {
	t.Helper()
	if n.stopped || n.cmd == nil || n.cmd.Process == nil {
		return
	}
	n.stopped = true
	_ = n.cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- n.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(35 * time.Second):
		_ = syscall.Kill(-n.cmd.Process.Pid, syscall.SIGKILL)
		<-done
		t.Fatalf("node %d did not exit within the graceful window", n.id)
	}
	waitPortFree(n.base())
}

func waitPortFree(base string) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !statusOK(base + "/health") {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func leaderID(t *testing.T, base string) uint64 {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(base + "/v1/membership") //nolint:gosec // G107: fixed loopback URL
		if err == nil {
			var m struct {
				LeaderID uint64 `json:"leaderId"`
			}
			derr := json.NewDecoder(resp.Body).Decode(&m)
			_ = resp.Body.Close()
			if derr == nil && resp.StatusCode == http.StatusOK && m.LeaderID != 0 {
				return m.LeaderID
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("no leader reported within 30s")
	return 0
}

func addMember(t *testing.T, base string, id uint64, url string) {
	t.Helper()
	body := fmt.Sprintf(`{"id":%d,"url":%q}`, id, url)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/membership", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	require.GreaterOrEqualf(t, resp.StatusCode, 200, "member add: %d %s", resp.StatusCode, out)
	require.Lessf(t, resp.StatusCode, 300, "member add: %d %s", resp.StatusCode, out)
}

// --- shared plumbing (the upgrade harness pattern, adapted for N nodes) ---

func buildBinary(t *testing.T) string {
	t.Helper()
	root := projectRoot(t)
	out := filepath.Join(t.TempDir(), "committed")
	if runtime.GOOS == "windows" {
		out += ".exe"
	}
	cmd := exec.Command("go", "build", "-o", out, ".")
	cmd.Dir = root
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build committed: %v\n%s", err, output)
	}
	binPath = out
	return out
}

// binPath is the built binary all starts/restarts spawn; buildBinary sets it
// once per test.
var binPath string

func projectRoot(t *testing.T) string {
	t.Helper()
	_, this, _, ok := runtime.Caller(0)
	require.True(t, ok, "runtime.Caller")
	dir := filepath.Dir(this)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, parent, dir, "could not locate go.mod")
		dir = parent
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitReady(t *testing.T, base string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if statusOK(base + "/ready") {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("node never became ready (/ready) within 60s: %s", base)
}

func postType(t *testing.T, base, id string) {
	t.Helper()
	body := fmt.Sprintf("[type]\nname = %q\n", id)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/type/"+id, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "text/toml")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	require.GreaterOrEqualf(t, resp.StatusCode, 200, "POST type: %d %s", resp.StatusCode, out)
	require.Lessf(t, resp.StatusCode, 300, "POST type: %d %s", resp.StatusCode, out)
}

func requireTypeListed(t *testing.T, base, id string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(base + "/v1/type") //nolint:gosec // G107: fixed loopback URL
		if err == nil {
			out, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK && strings.Contains(string(out), id) {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("type %q not found in GET /v1/type on %s within 30s", id, base)
}

func statusOK(url string) bool {
	resp, err := http.Get(url) //nolint:gosec // G107: fixed loopback URL
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

type testWriter struct {
	t      *testing.T
	prefix string
}

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("[%s] %s", w.prefix, strings.TrimRight(string(p), "\n"))
	return len(p), nil
}
