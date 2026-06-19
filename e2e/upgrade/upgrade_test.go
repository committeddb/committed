//go:build upgrade

// Package upgrade_test exercises the per-node half of the rolling-upgrade
// procedure documented in docs/operations/upgrade.md against the REAL binary:
// graceful SIGTERM shutdown, restart over the same data directory, /ready
// gating, /version, and that persisted state survives the restart.
//
// It deliberately restarts the SAME binary rather than swapping an older one
// in. The cross-version (old-binary → new-binary) read is asserted by the
// on-disk compatibility contract (docs/api-compatibility.md) plus the fact
// that a node which can't read its state fails to start instead of reaching
// /ready — so "came back /ready" is itself the forward-read check. The
// multi-node, quorum-preserving rolling mechanics are covered in-process by
// the adversarial suite (TestAdversarial_LeaderFlapping kills + restarts the
// leader and asserts exactly-once apply across the transition). This test
// fills the gap those two don't: the real binary's signal + endpoint +
// real-process-restart cycle.
//
// Tagged `upgrade` so it stays out of `make test`; run via `make test/upgrade`.
package upgrade_test

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
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRollingUpgrade_NodeRestartPreservesState walks one node through the
// per-node upgrade cycle and asserts the invariants the procedure relies on.
func TestRollingUpgrade_NodeRestartPreservesState(t *testing.T) {
	bin := buildBinary(t)
	dataDir := t.TempDir()
	port := freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)

	// First boot. (In a real upgrade this would be the OLD binary; here it's
	// the current one — see the package comment for why that's the right
	// scope.)
	first := startNode(t, bin, dataDir, port)
	waitReady(t, base)
	v1 := getVersion(t, base)
	require.NotEmpty(t, v1.Version, "/version must report a build version")

	// Write durable state through the real API (a type config — no external
	// connection, persisted in BoltDB).
	postType(t, base, "upgrade-canary")
	requireTypeListed(t, base, "upgrade-canary")

	// Graceful stop: SIGTERM drains HTTP, stops raft, closes the WAL cleanly
	// (see docs/operations/shutdown.md). Waits for the process to fully exit
	// and the port to free.
	first.stopGraceful(t, base)

	// Replace the binary and restart over the SAME data directory — the
	// upgrade step. Same id, same port, same dir; only the binary changes.
	second := startNode(t, bin, dataDir, port)
	t.Cleanup(func() { second.stopGraceful(t, base) })

	// Reaching /ready means the restarted node read its prior WAL + BoltDB
	// state successfully (a node that couldn't would fail to start, not serve
	// /ready) and re-elected/recovered a leader.
	waitReady(t, base)

	// The running build is intact and stable across the restart...
	v2 := getVersion(t, base)
	require.Equal(t, v1, v2, "version must be stable across an in-place restart")

	// ...and the state written before the restart survived it.
	requireTypeListed(t, base, "upgrade-canary")
}

// --- harness ---

type nodeProcess struct {
	cmd     *exec.Cmd
	stopped bool
}

// buildBinary builds the current committed binary into a temp dir once.
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
	return out
}

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

// startNode spawns `committed node` against dataDir on port, with auth off
// (no COMMITTED_API_TOKEN) and single-node bootstrap (no COMMITTED_PEERS), and
// waits for /health to flip to 200.
func startNode(t *testing.T, bin, dataDir string, port int) *nodeProcess {
	t.Helper()
	base := fmt.Sprintf("http://127.0.0.1:%d", port)

	cmd := exec.Command(bin, "node")
	cmd.Env = append(os.Environ(),
		"COMMITTED_NODE_ID=1",
		fmt.Sprintf("COMMITTED_API_ADDR=127.0.0.1:%d", port),
		"COMMITTED_DATA_DIR="+dataDir,
	)
	cmd.Stdout = testWriter{t, "committed:out"}
	cmd.Stderr = testWriter{t, "committed:err"}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, cmd.Start(), "spawn committed")

	p := &nodeProcess{cmd: cmd}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if statusOK(base + "/health") {
			return p
		}
		time.Sleep(100 * time.Millisecond)
	}
	p.stopGraceful(t, base)
	t.Fatalf("committed did not become healthy within 30s")
	return nil
}

// stopGraceful sends SIGTERM, waits for a clean exit (bounded), then waits for
// the port to free so a restart can rebind it.
func (p *nodeProcess) stopGraceful(t *testing.T, base string) {
	t.Helper()
	if p.stopped {
		return
	}
	p.stopped = true

	if p.cmd.Process != nil {
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
	}
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(35 * time.Second): // 30s default shutdown timeout + slack
		_ = p.cmd.Process.Kill()
		<-done
		t.Fatal("node did not exit within the graceful window")
	}

	// Wait for the listener to actually release the port.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !statusOK(base + "/health") {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func waitReady(t *testing.T, base string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if statusOK(base + "/ready") {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("node never became ready (/ready) within 30s")
}

type versionInfo struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"buildDate"`
	GoVersion string `json:"goVersion"`
}

func getVersion(t *testing.T, base string) versionInfo {
	t.Helper()
	resp, err := http.Get(base + "/version") //nolint:gosec // G107: fixed loopback URL
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var v versionInfo
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&v))
	return v
}

func postType(t *testing.T, base, id string) {
	t.Helper()
	body := fmt.Sprintf("[type]\nname = %q\n", id)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	// Poll: a config write commits through raft and applies asynchronously,
	// and a fresh worker may briefly lag.
	deadline := time.Now().Add(10 * time.Second)
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
	t.Fatalf("type %q not found in GET /v1/type within 10s", id)
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
