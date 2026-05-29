package cmd

import (
	"crypto/tls"
	"fmt"
	"net"
	nethttp "net/http"
	"os"
	"time"

	"github.com/spf13/cobra"
)

// healthcheckTimeout bounds the whole self-probe. It matches the
// Dockerfile HEALTHCHECK --timeout so the probe gives up at the same
// moment Docker would kill it, rather than leaving a dangling request.
const healthcheckTimeout = 3 * time.Second

// healthcheckCmd is the container HEALTHCHECK self-probe. The runtime
// image is distroless — no shell, no curl — so the binary has to probe
// itself. It performs a single GET /ready against the local API and
// exits 0 (ready) or non-zero (not ready / unreachable), which is
// exactly the contract Docker's HEALTHCHECK and most orchestrator
// readiness gates expect.
//
// /ready (not /health) is the right target: /health is always 200 once
// the process can serve HTTP, so it would report "healthy" before raft
// has elected a leader and replayed the WAL. /ready returns 503 until
// the node can actually serve correct reads, which is what gates
// traffic during a rolling restart.
var healthcheckCmd = &cobra.Command{
	Use:   "healthcheck",
	Short: "Probe the local node's /ready endpoint; exit 0 if ready, non-zero otherwise",
	Long: `Probe the local node's /ready endpoint and exit 0 if it returns 200,
non-zero otherwise.

Intended as the container HEALTHCHECK command: the distroless runtime
image has no shell or curl, so the binary self-probes. The target is
derived from the same environment the node reads:

  COMMITTED_API_ADDR            listen address to probe (default ":8080");
                                an empty or wildcard host is dialed as
                                127.0.0.1.
  COMMITTED_HTTP_TLS_CERT_FILE  when set, the API is HTTPS, so the probe
  COMMITTED_HTTP_TLS_KEY_FILE   uses TLS and presents this cert/key as a
                                client cert (for mTLS deployments where
                                the node cert chains to the client CA).
                                The server cert is not verified — this is
                                a loopback self-probe.`,
	// A failed probe should print a one-line reason, not the full cobra
	// usage dump. Errors still surface (Execute returns them, main exits
	// non-zero).
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runHealthcheck()
	},
}

// runHealthcheck performs the GET /ready and maps the result to an
// error (→ exit 1) or nil (→ exit 0).
func runHealthcheck() error {
	url := healthcheckURL()

	client, err := healthcheckClient()
	if err != nil {
		return err
	}

	// No request context needed — client.Timeout bounds the whole probe.
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("healthcheck: GET %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != nethttp.StatusOK {
		return fmt.Errorf("healthcheck: %s returned %d", url, resp.StatusCode)
	}
	return nil
}

// healthcheckURL builds the loopback /ready URL from COMMITTED_API_ADDR.
// An empty or wildcard listen host (":8080", "0.0.0.0:8080", "[::]:8080")
// is not dialable, so it is rewritten to 127.0.0.1.
func healthcheckURL() string {
	addr := getenvDefault("COMMITTED_API_ADDR", ":8080")

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		// addr isn't host:port (e.g. a bare port) — treat it as the port.
		host, port = "", addr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}

	scheme := "http"
	if os.Getenv("COMMITTED_HTTP_TLS_CERT_FILE") != "" {
		scheme = "https"
	}

	return fmt.Sprintf("%s://%s/ready", scheme, net.JoinHostPort(host, port))
}

// healthcheckClient returns an HTTP client for the self-probe. When the
// API serves HTTPS (COMMITTED_HTTP_TLS_CERT_FILE set) the client uses
// TLS with server verification disabled — a loopback self-probe can't
// satisfy hostname verification (the cert's SANs won't list 127.0.0.1)
// and shouldn't need the CA just to ping itself. If a key is also set,
// the cert/key are presented as a client cert so the probe still works
// when the server requires mTLS and the node cert chains to the client
// CA.
func healthcheckClient() (*nethttp.Client, error) {
	c := &nethttp.Client{Timeout: healthcheckTimeout}

	cert := os.Getenv("COMMITTED_HTTP_TLS_CERT_FILE")
	if cert == "" {
		return c, nil
	}

	tlsCfg := &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // G402: loopback self-probe, not a trust decision
		MinVersion:         tls.VersionTLS12,
	}
	if key := os.Getenv("COMMITTED_HTTP_TLS_KEY_FILE"); key != "" {
		pair, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("healthcheck: load TLS cert/key: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{pair}
	}
	c.Transport = &nethttp.Transport{TLSClientConfig: tlsCfg}
	return c, nil
}

func init() {
	rootCmd.AddCommand(healthcheckCmd)
}
