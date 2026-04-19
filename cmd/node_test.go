package cmd

import (
	"net"
	"net/http"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster/db"
	"github.com/philborlin/committed/internal/cluster/db/parser"
	dbtest "github.com/philborlin/committed/internal/cluster/db/testing"
	clusterhttp "github.com/philborlin/committed/internal/cluster/http"
)

// newTestDB builds a single-node db.DB backed by MemoryStorage. Mirrors
// the shape of dbtest.CreateDB but returns the underlying *db.DB so
// runNode can consume d.ErrorC directly.
func newTestDB(t *testing.T) *db.DB {
	t.Helper()
	s := dbtest.NewMemoryStorage()
	p := parser.New()
	peers := make(db.Peers)
	peers[1] = "" // empty URL skips peer-transport listener
	d := db.New(1, peers, s, p, nil, nil, db.WithTickInterval(1*time.Millisecond))
	d.EatCommitC()
	return d
}

// pickFreeAddr allocates a loopback port, closes it immediately, and
// returns "127.0.0.1:<port>" for the caller to re-bind. Same pattern as
// internal/cluster/db/httptransport/mtls_test.go:pickFreeAddr.
func pickFreeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

// waitListening polls addr until it accepts a TCP connection.
func waitListening(t *testing.T, addr string) {
	t.Helper()
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err != nil {
			return false
		}
		_ = c.Close()
		return true
	}, 2*time.Second, 10*time.Millisecond, "http server never bound to %s", addr)
}

// TestRunNode_SIGINT starts runNode, sends SIGINT to the test process,
// and asserts runNode returns 0 after the HTTP server has been
// gracefully shut down. The signal path is the one operators will
// exercise via `kill`, `systemctl stop`, or a Kubernetes rolling
// restart — the in-process signal is the only way to drive it from a
// test.
func TestRunNode_SIGINT(t *testing.T) {
	d := newTestDB(t)
	h := clusterhttp.New(d)
	addr := pickFreeAddr(t)
	srv := h.NewServer(addr)

	done := make(chan int, 1)
	go func() { done <- runNode(d, srv) }()

	waitListening(t, addr)

	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGINT))

	select {
	case code := <-done:
		require.Equal(t, 0, code)
	case <-time.After(5 * time.Second):
		t.Fatal("runNode did not return after SIGINT")
	}

	// After graceful shutdown the listener should be gone.
	_, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	require.Error(t, err, "http server still accepting connections after shutdown")
}

// TestGracefulShutdown_ClosesHTTPAndDB verifies the drain path
// directly, without going through the signal handler. A dial after
// shutdown must fail (httpServer.Shutdown actually closed the
// listener), and calling d.Close a second time must be safe (proves
// the first call inside gracefulShutdown completed).
func TestGracefulShutdown_ClosesHTTPAndDB(t *testing.T) {
	d := newTestDB(t)
	h := clusterhttp.New(d)
	addr := pickFreeAddr(t)
	srv := h.NewServer(addr)

	serveErrC := make(chan error, 1)
	go func() { serveErrC <- srv.ListenAndServe() }()
	waitListening(t, addr)

	code := gracefulShutdown(d, srv)
	require.Equal(t, 0, code)

	select {
	case err := <-serveErrC:
		require.ErrorIs(t, err, http.ErrServerClosed)
	case <-time.After(2 * time.Second):
		t.Fatal("ListenAndServe did not return after Shutdown")
	}

	_, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	require.Error(t, err, "http server still accepting connections after shutdown")

	require.NoError(t, d.Close(), "db.Close must be idempotent after gracefulShutdown")
}

// TestShutdownTimeout_Default covers env-var parsing: unset returns
// the default, a valid duration is honored, and a garbage value falls
// back to the default rather than silently disabling the graceful
// path.
func TestShutdownTimeout(t *testing.T) {
	orig, hadOrig := os.LookupEnv("COMMITTED_SHUTDOWN_TIMEOUT")
	t.Cleanup(func() {
		if hadOrig {
			_ = os.Setenv("COMMITTED_SHUTDOWN_TIMEOUT", orig)
		} else {
			_ = os.Unsetenv("COMMITTED_SHUTDOWN_TIMEOUT")
		}
	})

	_ = os.Unsetenv("COMMITTED_SHUTDOWN_TIMEOUT")
	require.Equal(t, defaultShutdownTimeout, shutdownTimeout())

	_ = os.Setenv("COMMITTED_SHUTDOWN_TIMEOUT", "45s")
	require.Equal(t, 45*time.Second, shutdownTimeout())

	_ = os.Setenv("COMMITTED_SHUTDOWN_TIMEOUT", "not-a-duration")
	require.Equal(t, defaultShutdownTimeout, shutdownTimeout())

	_ = os.Setenv("COMMITTED_SHUTDOWN_TIMEOUT", "0s")
	require.Equal(t, defaultShutdownTimeout, shutdownTimeout())
}
