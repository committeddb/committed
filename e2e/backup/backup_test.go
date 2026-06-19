//go:build backup

// Package backup_test is the end-to-end round-trip for the offline backup
// primitive (docs/operations/backup.md) against the REAL binary: boot a node,
// write state, confirm `committed backup` refuses while it's live, stop it,
// back it up, restore into a fresh directory, boot a node on the restored
// directory, and confirm the state survived.
//
// Tagged `backup` so it stays out of `make test`; run via `make test/backup`.
package backup_test

import (
	"context"
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

func TestBackupRestore_RoundTrip(t *testing.T) {
	bin := buildBinary(t)
	srcDir := t.TempDir()
	port := freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)

	// Boot the source node and write durable state.
	node := startNode(t, bin, srcDir, port)
	waitReady(t, base)
	postType(t, base, "bk-canary")
	requireTypeListed(t, base, "bk-canary")

	// Backup MUST refuse while the node is live (the lock probe).
	tarPath := filepath.Join(t.TempDir(), "backup.tar.gz")
	out, err := runCLI(t, bin, "backup", "--data", srcDir, "--to", tarPath)
	require.Error(t, err, "backup must refuse a running node")
	require.Contains(t, out, "in use by a running node")
	require.NoFileExists(t, tarPath, "a refused backup must not leave a file")

	// Stop the node, then back up its quiescent directory.
	node.stopGraceful(t, base)
	out, err = runCLI(t, bin, "backup", "--data", srcDir, "--to", tarPath, "--node-id", "1")
	require.NoError(t, err, "backup of a stopped node: %s", out)
	require.FileExists(t, tarPath)

	// Restore into a fresh directory.
	dstDir := filepath.Join(t.TempDir(), "restored")
	out, err = runCLI(t, bin, "restore", "--from", tarPath, "--data", dstDir)
	require.NoError(t, err, "restore: %s", out)
	require.FileExists(t, filepath.Join(dstDir, "RESTORED.json"))

	// Boot a node on the restored directory (same id/port; the source is
	// stopped) and confirm it recovers and still has the data.
	restored := startNode(t, bin, dstDir, port)
	t.Cleanup(func() { restored.stopGraceful(t, base) })
	waitReady(t, base)
	requireTypeListed(t, base, "bk-canary")
}

// --- harness (self-contained; mirrors e2e/upgrade) ---

type nodeProcess struct {
	cmd     *exec.Cmd
	stopped bool
}

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
	require.True(t, ok)
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
	case <-time.After(35 * time.Second):
		_ = p.cmd.Process.Kill()
		<-done
		t.Fatal("node did not exit within the graceful window")
	}
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
	t.Fatalf("node never became ready within 30s")
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

// runCLI runs `committed <args...>` to completion and returns its combined
// output and exit error.
func runCLI(t *testing.T, bin string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	out, err := cmd.CombinedOutput()
	t.Logf("committed %s -> %v\n%s", strings.Join(args, " "), err, out)
	return string(out), err
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
