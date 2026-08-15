//go:build docker

package harness

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// committedAddr is the HTTP address the harness's committed children listen
// on, allocated per test-binary run and passed to every child via
// COMMITTED_API_ADDR. It used to be the hardcoded default :8080, which
// collided with any other committed on the machine — and worse than
// colliding, it failed SILENTLY WRONG: the harness child died on the taken
// port while the health gate got 200 from the foreign node, so the tests
// posted their configs into somebody else's cluster. -p=1 on test/cdc still
// ensures no two tests of this suite bind it simultaneously.
//
// Ports come from a LOW fixed range (21000–29999), deliberately BELOW every
// ephemeral floor (32768 on Linux, 49152 on macOS): the old scheme bound
// 127.0.0.1:0, read the kernel-assigned port, and closed the listener — a
// reserve-then-release from the same pool the kernel auto-assigns from and
// docker-proxy publishes container ports from. On a CI run a postgres
// container's host-mapped port landed exactly on the reserved raft peer
// port, and because the pair is shared across the whole binary, EVERY
// subsequent test's committed died with "bind: address already in use"
// (each test creates its container before its committed, so the collision
// looped for the rest of the run). A low-range port can only be taken by
// another explicit user of that range, which the runner doesn't have — and
// if it ever is, startCommittedAt reallocates a fresh pair and retries
// instead of poisoning the rest of the run.
var (
	committedPortMu  sync.Mutex
	committedAddr    string // HTTP API
	committedPeerURL string // raft peer listener (default 9022 collides too)
	// nextProbePort cycles through the low range; the pid offset keeps
	// concurrently-running test binaries (different packages) apart.
	nextProbePort = 21000 + os.Getpid()%2000
)

// probeLowPort binds one candidate port from the low range to prove it free,
// closes it, and returns the address. Caller holds committedPortMu.
func probeLowPort() (string, bool) {
	for range 500 {
		candidate := nextProbePort
		nextProbePort++
		if nextProbePort >= 30000 {
			nextProbePort = 21000
		}
		l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", candidate))
		if err != nil {
			continue
		}
		_ = l.Close()
		return fmt.Sprintf("127.0.0.1:%d", candidate), true
	}
	return "", false
}

func committedAddrs(t *testing.T) (api, peerURL string) {
	t.Helper()
	committedPortMu.Lock()
	defer committedPortMu.Unlock()
	if committedAddr == "" {
		if a, ok := probeLowPort(); ok {
			if p, ok := probeLowPort(); ok {
				committedAddr = a
				committedPeerURL = "http://" + p
			}
		}
	}
	require.NotEmpty(t, committedAddr, "allocate free ports for committed's API and raft peer listeners")
	return committedAddr, committedPeerURL
}

// reallocCommittedAddrs discards the shared port pair so the next
// committedAddrs picks fresh ones — the recovery path when a child dies on
// a stolen port.
func reallocCommittedAddrs() {
	committedPortMu.Lock()
	defer committedPortMu.Unlock()
	committedAddr, committedPeerURL = "", ""
}

// committedBinary returns the path to a freshly-built committed binary.
// Built once per test binary invocation and reused; binary lives in
// t.TempDir() of the first caller so the OS cleans it up.
var (
	binaryOnce sync.Once
	binaryPath string
	binaryErr  error
)

func committedBinary(t *testing.T) string {
	t.Helper()
	binaryOnce.Do(func() {
		root, err := projectRoot()
		if err != nil {
			binaryErr = err
			return
		}
		// Use os.TempDir() — not t.TempDir() — because t.TempDir is
		// per-test-cleanup and the binary is shared across tests in
		// the same process.
		dir, err := os.MkdirTemp("", "committed-cdc-*")
		if err != nil {
			binaryErr = err
			return
		}
		out := filepath.Join(dir, "committed")
		if runtime.GOOS == "windows" {
			out += ".exe"
		}
		cmd := exec.Command("go", "build", "-o", out, ".") //nolint:gosec // G204: out is os.MkdirTemp output; root is the project's own go.mod dir — both trusted by construction
		cmd.Dir = root
		if output, err := cmd.CombinedOutput(); err != nil {
			binaryErr = fmt.Errorf("build committed: %w\n%s", err, output)
			return
		}
		binaryPath = out
	})
	require.NoError(t, binaryErr, "build committed binary")
	return binaryPath
}

// projectRoot walks up from the e2e/cdc/harness package source file to
// find the repository root (the directory containing go.mod). The test
// binary's working directory at runtime is the test package's source
// directory, but the harness might be invoked from elsewhere; walking
// from the source file is robust to both.
func projectRoot() (string, error) {
	_, this, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("runtime.Caller failed")
	}
	dir := filepath.Dir(this)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("could not locate go.mod from %s", filepath.Dir(this))
		}
		dir = parent
	}
}

// committedProcess is one running committed binary. The harness owns
// stop() and uses t.Cleanup to invoke it; Stop is idempotent.
type committedProcess struct {
	cmd     *exec.Cmd
	dataDir string
	stopped bool
	// exited closes when the child has been reaped. The reaper goroutine
	// started alongside the process is the SINGLE cmd.Wait caller; the
	// startup health gate and Stop both watch this channel instead of
	// racing a second Wait.
	exited chan struct{}
}

// startCommitted spawns the committed binary as a child process. Each
// test gets its own data dir under t.TempDir() so state never leaks
// between tests. Blocks until GET /health returns 200, with a generous
// timeout because the first start in a test binary run includes Raft
// initialization + WAL creation.
func startCommitted(t *testing.T) *committedProcess {
	t.Helper()
	return startCommittedAt(t, t.TempDir())
}

// startCommittedAt is startCommitted's reusable form: spawns committed
// against an existing data dir, used by RestartCommitted to bring a
// fresh process up over the same WAL + bbolt state. Same lifecycle
// guarantees as startCommitted.
//
// A child that DIES during startup (a bind failure on a stolen port) is
// retried up to twice with a freshly allocated port pair before failing the
// test — the shared pair means a stolen port would otherwise fail every
// remaining test in the binary, not just this one.
func startCommittedAt(t *testing.T, dataDir string) *committedProcess {
	t.Helper()

	const attempts = 3
	for attempt := 1; ; attempt++ {
		p, ok := spawnCommitted(t, dataDir)
		if ok {
			return p
		}
		if attempt >= attempts {
			t.Fatalf("committed exited during startup %d times (bind failure? see its log lines above)", attempts)
			return nil
		}
		t.Logf("committed exited during startup (stolen port?); reallocating the port pair and retrying (%d/%d)", attempt, attempts)
		reallocCommittedAddrs()
	}
}

// spawnCommitted runs one start attempt: spawn, then gate on /health. ok is
// false when the child exited before turning healthy (the retryable case);
// every other failure is fatal inline.
func spawnCommitted(t *testing.T, dataDir string) (p *committedProcess, ok bool) {
	t.Helper()

	bin := committedBinary(t)

	cmd := exec.Command(bin, "node") //nolint:gosec // G204: bin is the result of our own go build into os.MkdirTemp — trusted by construction
	cmd.Dir = dataDir
	// The POSTed configs reference the (constant) container DB password as
	// ${TEST_DB_PASSWORD} rather than inlining it — committed rejects an inline
	// connection-string password. The node resolves it from this environment at
	// config-parse time. mysqlPass == pgPass, so one variable serves both dialects.
	apiAddr, peerURL := committedAddrs(t)
	cmd.Env = append(os.Environ(),
		"TEST_DB_PASSWORD="+mysqlPass,
		// SQL Server's SA password can't be "secret" (complexity policy), so
		// it externalizes through its own variable.
		"TEST_MSSQL_PASSWORD="+mssqlSAPassword,
		"COMMITTED_API_ADDR="+apiAddr,
		// A single-node cluster on a harness-owned peer port: the default
		// (9022) collides with any other committed on the machine exactly
		// like the API port did.
		"COMMITTED_PEERS=1="+peerURL,
	)
	cmd.Stdout = testWriter{t, "committed:stdout"}
	cmd.Stderr = testWriter{t, "committed:stderr"}
	// SysProcAttr lets us send SIGTERM to the whole process group on
	// shutdown — committed itself is a single process today but if it
	// ever spawns helpers this is correct without revisiting.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	require.NoError(t, cmd.Start(), "spawn committed")

	p = &committedProcess{cmd: cmd, dataDir: dataDir, exited: make(chan struct{})}
	go func() { _ = cmd.Wait(); close(p.exited) }()
	t.Cleanup(p.Stop)

	// Wait for /health to flip to 200. Polled rather than parsed from
	// stdout because the "API Listening" log message races with the
	// listener actually accepting connections.
	// Also watch for the child DYING during startup: with a foreign process
	// answering health on our port, a dead child plus a 200 would silently
	// aim every test at the wrong cluster (the exact incident the dynamic
	// port exists to prevent) — a dead child must be handled loudly instead
	// (retried on fresh ports by startCommittedAt, then fatal).
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-p.exited:
			p.stopped = true // already gone; keep Cleanup's Stop a no-op
			return p, false
		default:
		}
		resp, err := http.Get("http://" + apiAddr + "/health") //nolint:gosec // G107: fixed in-process URL
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return p, true
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("committed did not become healthy within 30s")
	return nil, false
}

// Stop sends SIGTERM and waits for the child to exit. Returns quickly
// if already stopped. Always waits for full exit so the next test's
// committed can bind the same port — there is no "kill -9 and move on"
// path because that would leave port 8080 in TIME_WAIT and break the
// next test.
func (p *committedProcess) Stop() {
	if p.stopped {
		return
	}
	p.stopped = true

	if p.cmd.Process != nil {
		// SIGTERM triggers the graceful path in cmd/node.go:runNode.
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
	}

	select {
	case <-p.exited:
	case <-time.After(35 * time.Second):
		// 35s = COMMITTED_SHUTDOWN_TIMEOUT default (30s) + 5s slack.
		// If we hit this, graceful shutdown is broken; kill hard so the
		// test runner doesn't hang.
		_ = p.cmd.Process.Kill()
		<-p.exited
	}

	// Wait for the port to actually free up — kernel can hold it for
	// a moment after the process exits even with SO_REUSEADDR off.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + committedAddr + "/health") //nolint:gosec // G107: fixed in-process URL
		if err != nil {
			return
		}
		_ = resp.Body.Close()
		time.Sleep(50 * time.Millisecond)
	}
}

// testWriter is an io.Writer that forwards to t.Log so committed's
// stdout/stderr show up in test failure output.
type testWriter struct {
	t      *testing.T
	prefix string
}

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("[%s] %s", w.prefix, string(p))
	return len(p), nil
}

// committedURL returns a full URL for a path on the committed HTTP API.
func committedURL(path string) string {
	return "http://" + committedAddr + path
}
