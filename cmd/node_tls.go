package cmd

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net"
	nethttp "net/http"
	"os"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.uber.org/zap"
)

// isLoopbackBind reports whether addr (an HTTP listen address like "127.0.0.1:8080"
// or ":8080") binds ONLY the loopback interface. An empty host (":8080") binds all
// interfaces and is NOT loopback. A hostname we can't cheaply classify is treated
// as non-loopback — the safe default is to assume the node is reachable and warn.
func isLoopbackBind(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr // no :port present
	}
	switch host {
	case "":
		return false // all interfaces
	case "localhost":
		return true
	default:
		if ip := net.ParseIP(host); ip != nil {
			return ip.IsLoopback()
		}
		return false
	}
}

// apiUnauthenticatedOffHost is the security-floor trigger: the write API is
// reachable off this host (bound a non-loopback address) AND has no authentication
// — neither a bearer token nor mTLS client-cert verification. Pure and testable;
// warnInsecurePosture does the logging.
func apiUnauthenticatedOffHost(addr, apiToken string, apiTLS *tls.Config) bool {
	authed := apiToken != "" || (apiTLS != nil && apiTLS.ClientAuth == tls.RequireAndVerifyClientCert)
	return !isLoopbackBind(addr) && !authed
}

// warnInsecurePosture is committed's security-posture floor. When the write API is
// reachable off this host with no authentication it logs a loud Error and prints a
// startup banner to stderr — committed deliberately does NOT refuse to boot (self-
// hosted "kick the tires" with no real data is supported, and the hosted deployment
// is secured by its orchestration), but an operator who exposes an unauthenticated
// node must SEE it. Quiet when bound to loopback or when auth is configured.
func warnInsecurePosture(addr, apiToken string, apiTLS *tls.Config, peerMTLS bool) {
	if apiUnauthenticatedOffHost(addr, apiToken, apiTLS) {
		plaintext := ""
		if apiTLS == nil {
			plaintext = " over PLAINTEXT HTTP"
		}
		peerNote := ""
		if !peerMTLS && apiToken == "" {
			peerNote = "\n  The raft peer transport is also unauthenticated — anyone who can reach the\n  peer port can impersonate a cluster member."
		}
		fmt.Fprintf(os.Stderr, `
========================================================================
  SECURITY: committed's write API is UNAUTHENTICATED and reachable off
  this host (bound %q)%s. Anyone who can reach it can READ and WRITE the
  log and reconfigure the cluster.%s

  Fine for local testing, but DO NOT put production data on this node.
  Set COMMITTED_API_TOKEN (and TLS), or bind to loopback.
  See docs/operations/authentication.md.
========================================================================
`, addr, plaintext, peerNote)
		zap.L().Error("serving an UNAUTHENTICATED write API on a non-loopback address; do not put production data here — set COMMITTED_API_TOKEN and TLS (see docs/operations/authentication.md)",
			zap.String("addr", addr), zap.Bool("tls", apiTLS != nil), zap.Bool("peerMTLS", peerMTLS))
		return
	}
	if apiTLS == nil {
		zap.L().Warn("API TLS disabled (no COMMITTED_HTTP_TLS_CERT_FILE/KEY_FILE set); fine for loopback/local use, otherwise set TLS — see docs/operations/authentication.md",
			zap.String("addr", addr))
	}
}

// loadPeerTLSInfo reads the three COMMITTED_TLS_* env vars and returns
// the corresponding transport.TLSInfo. Returns nil when none of them are
// set (plaintext — today's default). Fatal-exits if some are set but not
// all three, since a half-configured TLS setup is almost always an
// operator mistake and silent fallback to plaintext is worse than a loud
// startup failure.
func loadPeerTLSInfo() *transport.TLSInfo {
	ca := os.Getenv("COMMITTED_TLS_CA_FILE")
	cert := os.Getenv("COMMITTED_TLS_CERT_FILE")
	key := os.Getenv("COMMITTED_TLS_KEY_FILE")

	set := 0
	if ca != "" {
		set++
	}
	if cert != "" {
		set++
	}
	if key != "" {
		set++
	}
	if set == 0 {
		return nil
	}
	if set != 3 {
		// G706 false positive: the "user-controlled" values are env
		// vars from the process operator, who already has full control
		// over the process. Log-injection is not a coherent threat here.
		log.Fatalf("peer mTLS: all of COMMITTED_TLS_CA_FILE, COMMITTED_TLS_CERT_FILE, COMMITTED_TLS_KEY_FILE must be set together (got CA=%q CERT=%q KEY=%q)", ca, cert, key) //nolint:gosec // G706
	}
	return &transport.TLSInfo{
		TrustedCAFile: ca,
		CertFile:      cert,
		KeyFile:       key,
		// TrustedCAFile alone makes ServerConfig() require and verify
		// client certs, but ClientCertAuth=true makes it explicit — so
		// the server-side ClientAuth policy survives even if someone
		// later swaps in a ServerConfig override that doesn't read
		// TrustedCAFile for that decision.
		ClientCertAuth: true,
	}
}

// loadAPITLSConfig reads COMMITTED_HTTP_TLS_* env vars and returns a
// *tls.Config for the client-facing HTTP server, or (nil, nil) when no
// TLS env vars are set (plaintext — today's default, kept for laptop
// dev).
//
// Rules:
//   - CERT_FILE + KEY_FILE together → HTTPS with TLS 1.2 minimum.
//   - CERT_FILE + KEY_FILE + CLIENT_CA_FILE → HTTPS with required
//     client certs (mTLS). Strictly more secure than bearer alone: a
//     stolen bearer token is useless without a client cert chaining to
//     CLIENT_CA_FILE.
//   - Any other non-empty combination → error. Silent fallback to
//     plaintext when an operator thinks they configured TLS is a worse
//     failure mode than a loud startup refusal — same rationale as
//     loadPeerTLSInfo.
//
// Returning (cfg, err) instead of calling log.Fatalf lets node_test.go
// exercise the error cases directly.
func loadAPITLSConfig() (*tls.Config, error) {
	cert := os.Getenv("COMMITTED_HTTP_TLS_CERT_FILE")
	key := os.Getenv("COMMITTED_HTTP_TLS_KEY_FILE")
	clientCA := os.Getenv("COMMITTED_HTTP_TLS_CLIENT_CA_FILE")

	if cert == "" && key == "" && clientCA == "" {
		return nil, nil
	}
	if cert == "" || key == "" {
		return nil, fmt.Errorf("COMMITTED_HTTP_TLS_CERT_FILE and COMMITTED_HTTP_TLS_KEY_FILE must be set together (got CERT=%q KEY=%q CLIENT_CA=%q)", cert, key, clientCA)
	}

	pair, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, fmt.Errorf("load cert/key: %w", err)
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{pair},
		MinVersion:   tls.VersionTLS12,
	}

	if clientCA != "" {
		// G304 false positive: the path comes from an env var set by
		// the process operator, not an untrusted request.
		pem, err := os.ReadFile(clientCA) //nolint:gosec // G304
		if err != nil {
			return nil, fmt.Errorf("read client CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("client CA file %q contains no PEM certificates", clientCA)
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return cfg, nil
}

// proxyClientTimeout bounds the follower→leader hop the leader-read proxy
// makes for GET /v1/membership. A few seconds is plenty for a leader's local
// membership read, and staying under the server WriteTimeout means a wedged
// leader yields a clean 503 rather than holding the caller's connection.
const proxyClientTimeout = 5 * time.Second

// loadProxyClient builds the HTTP client the leader-read proxy uses for the
// follower→leader hop. It returns (nil, nil) when no TLS customization is
// needed — the http layer then uses its default client (system-root TLS,
// bounded timeout), which is correct for plaintext peer APIs or ones whose
// certs chain to a public CA.
//
// For a TLS cluster with a private CA or self-signed certs the operator sets:
//   - COMMITTED_HTTP_TLS_CA_FILE — CA bundle to trust when dialing a peer's
//     API as a client (typically the same CA that signs the server certs).
//   - COMMITTED_HTTP_TLS_INSECURE_SKIP_VERIFY — skip verification entirely
//     (self-signed without a shared CA; the same escape hatch as the
//     `member --insecure` flag).
//
// Under mTLS (a peer API configured with COMMITTED_HTTP_TLS_CLIENT_CA_FILE)
// the node also presents its own COMMITTED_HTTP_TLS_CERT_FILE/KEY_FILE as the
// client cert so the forwarded request is accepted. Returning (cfg, err)
// instead of log.Fatalf lets node_test.go exercise the error cases directly,
// matching loadAPITLSConfig.
func loadProxyClient() (*nethttp.Client, error) {
	caFile := os.Getenv("COMMITTED_HTTP_TLS_CA_FILE")
	insecure := boolEnv("COMMITTED_HTTP_TLS_INSECURE_SKIP_VERIFY")
	if caFile == "" && !insecure {
		return nil, nil
	}

	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: insecure, //nolint:gosec // G402: operator opt-in via COMMITTED_HTTP_TLS_INSECURE_SKIP_VERIFY
	}
	if caFile != "" {
		// G304 false positive: the path comes from an operator-set env var.
		pem, err := os.ReadFile(caFile) //nolint:gosec // G304
		if err != nil {
			return nil, fmt.Errorf("read proxy CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("proxy CA file %q contains no PEM certificates", caFile)
		}
		tlsCfg.RootCAs = pool
	}
	// Present our own cert as a client cert for peers whose API requires mTLS.
	cert := os.Getenv("COMMITTED_HTTP_TLS_CERT_FILE")
	key := os.Getenv("COMMITTED_HTTP_TLS_KEY_FILE")
	if cert != "" && key != "" {
		pair, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("load proxy client cert/key: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{pair}
	}

	return &nethttp.Client{
		Timeout:   proxyClientTimeout,
		Transport: &nethttp.Transport{TLSClientConfig: tlsCfg},
	}, nil
}
