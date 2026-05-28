package cmd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	nethttp "net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"

	"github.com/spf13/cobra"

	"github.com/philborlin/committed/internal/cluster/db"
	parser "github.com/philborlin/committed/internal/cluster/db/parser"
	"github.com/philborlin/committed/internal/cluster/db/wal"
	"github.com/philborlin/committed/internal/cluster/http"
	ingestablesql "github.com/philborlin/committed/internal/cluster/ingestable/sql"
	ingestablemysql "github.com/philborlin/committed/internal/cluster/ingestable/sql/mysql"
	ingestablepostgres "github.com/philborlin/committed/internal/cluster/ingestable/sql/postgres"
	"github.com/philborlin/committed/internal/cluster/metrics"
	synchttp "github.com/philborlin/committed/internal/cluster/syncable/http"
	syncsql "github.com/philborlin/committed/internal/cluster/syncable/sql"
	syncmysql "github.com/philborlin/committed/internal/cluster/syncable/sql/dialects"
	"github.com/philborlin/committed/internal/version"
)

// defaultShutdownTimeout bounds how long graceful shutdown waits for the
// HTTP server to drain. Kubernetes pod terminationGracePeriodSeconds
// defaults to 30s; staying inside that envelope keeps the graceful path
// reachable in a normal rolling restart.
const defaultShutdownTimeout = 30 * time.Second

var nodeCmd = &cobra.Command{
	Use:   "node",
	Short: "Run a committed node",
	Long: `Run a committed node.

Configuration is supplied through environment variables so the same
image can be templated per-node by an orchestrator:

  COMMITTED_NODE_ID    raft node ID (default 1; must be unique and
                       present in COMMITTED_PEERS)
  COMMITTED_API_ADDR   HTTP API listen address (default ":8080")
  COMMITTED_DATA_DIR   data directory for WAL/state (default "./data")
  COMMITTED_PEER_URL   this node's advertised raft peer URL
                       (default "http://127.0.0.1:9022"); used when
                       COMMITTED_PEERS is unset
  COMMITTED_PEERS      full static cluster membership as id=url pairs,
                       e.g. "1=http://n1:9022,2=http://n2:9022". The
                       same value is given to every node and must
                       include this node's own COMMITTED_NODE_ID.
                       Consumed only on first boot; thereafter
                       membership is restored from the WAL.`,
	Run: func(cmd *cobra.Command, args []string) {
		v := version.Get()
		zap.L().Info("committed starting",
			zap.String("version", v.Version),
			zap.String("commit", v.Commit),
			zap.String("buildDate", v.BuildDate),
			zap.String("goVersion", v.GoVersion),
		)

		// Node identity and addressing come from the environment so the
		// same image can be templated per-node by an orchestrator (Docker,
		// Nomad, k8s). The historical stdlib `flag` calls here were dead —
		// flag.Parse() was never invoked, so they always returned defaults.
		id := nodeID()
		addr := getenvDefault("COMMITTED_API_ADDR", ":8080")
		dataDir := getenvDefault("COMMITTED_DATA_DIR", "./data")

		// Resolve peer membership before opening storage so a malformed
		// COMMITTED_PEERS fails fast without first creating an empty data
		// directory.
		peers := loadPeers(id)

		sync := make(chan *db.SyncableWithID)
		ingest := make(chan *db.IngestableWithID)

		p := parser.New()
		s, err := wal.Open(dataDir, p, sync, ingest)
		if err != nil {
			log.Fatalf("cannot open storage: %v", err)
		}

		var dbOpts []db.Option

		// Wire the global zap logger into db so internal supervisor /
		// raft / leader-transition logs are visible. Without this, the
		// DB defaults to zap.NewNop and operators have no visibility
		// into ingest worker startup, propose failures, or leader
		// flaps. main.go initializes the global, this propagates it.
		dbOpts = append(dbOpts, db.WithLogger(zap.L()))

		// mTLS for peer transport is configured via three env vars that
		// must be set together: COMMITTED_TLS_CA_FILE,
		// COMMITTED_TLS_CERT_FILE, COMMITTED_TLS_KEY_FILE. All three set
		// enables mTLS; none set keeps plaintext peer transport. Any
		// other combination is a hard startup error — silently running
		// partial-TLS ("I thought we had TLS") is the failure mode this
		// check exists to prevent.
		if tlsInfo := loadPeerTLSInfo(); tlsInfo != nil {
			dbOpts = append(dbOpts, db.WithTLSInfo(tlsInfo))
		}

		if n, ok := parseInt64Env("COMMITTED_MAX_PROPOSAL_BYTES"); ok {
			dbOpts = append(dbOpts, db.WithMaxProposalBytes(uint64(n)))
		}

		// When OTEL_EXPORTER_OTLP_ENDPOINT is set (e.g., "localhost:4317"),
		// metrics are pushed to an OTel Collector via gRPC. The collector
		// handles routing to backends (Prometheus, Datadog, etc.). When
		// unset, no metrics are collected and there is zero overhead.
		if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
			ctx := context.Background()
			exporter, err := otlpmetricgrpc.New(ctx)
			if err != nil {
				log.Fatalf("otel exporter: %v", err)
			}
			provider := sdkmetric.NewMeterProvider(
				sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)),
			)
			defer func() { _ = provider.Shutdown(context.Background()) }()
			m := metrics.New(provider.Meter("committed"))
			dbOpts = append(dbOpts, db.WithMetrics(m))
		}

		d := db.New(id, peers, s, p, sync, ingest, dbOpts...)
		fmt.Printf("Raft Running...\n")

		var httpOpts []http.Option
		if token := os.Getenv("COMMITTED_API_TOKEN"); token != "" {
			httpOpts = append(httpOpts, http.WithBearerToken(token))
		}

		h := http.New(d, httpOpts...)
		fmt.Printf("API Listening on %s...\n", addr)

		d.AddDatabaseParser("sql", dbParser())
		d.AddIngestableParser("sql", ingestableParser(d))
		d.AddSyncableParser("sql", &syncsql.SyncableParser{})
		d.AddSyncableParser("http", &synchttp.SyncableParser{})

		d.EatCommitC()

		var serverOpts []http.ServerOption
		if d, ok := parseDurationEnv("COMMITTED_HTTP_READ_HEADER_TIMEOUT"); ok {
			serverOpts = append(serverOpts, http.WithReadHeaderTimeout(d))
		}
		if d, ok := parseDurationEnv("COMMITTED_HTTP_READ_TIMEOUT"); ok {
			serverOpts = append(serverOpts, http.WithReadTimeout(d))
		}
		if d, ok := parseDurationEnv("COMMITTED_HTTP_WRITE_TIMEOUT"); ok {
			serverOpts = append(serverOpts, http.WithWriteTimeout(d))
		}
		if d, ok := parseDurationEnv("COMMITTED_HTTP_IDLE_TIMEOUT"); ok {
			serverOpts = append(serverOpts, http.WithIdleTimeout(d))
		}

		tlsCfg, err := loadAPITLSConfig()
		if err != nil {
			// G706 false positive: env-var values are supplied by the
			// process operator, not an untrusted user.
			log.Fatalf("API TLS: %v", err) //nolint:gosec // G706
		}
		if tlsCfg != nil {
			serverOpts = append(serverOpts, http.WithTLSConfig(tlsCfg))
			zap.L().Info("API TLS enabled",
				zap.Bool("clientCertAuth", tlsCfg.ClientAuth == tls.RequireAndVerifyClientCert))
		} else {
			zap.L().Warn("API TLS disabled (no COMMITTED_HTTP_TLS_CERT_FILE/KEY_FILE set) — do not expose the API to untrusted networks in this state")
		}

		exitCode := runNode(d, h.NewServer(addr, serverOpts...))
		if exitCode != 0 {
			os.Exit(exitCode)
		}
	},
}

// runNode owns the signal-handling + graceful-shutdown lifecycle. It
// starts the HTTP server in a goroutine and blocks until one of:
//
//   - SIGINT/SIGTERM — graceful path: httpServer.Shutdown, then
//     d.Close(), exit 0. If Shutdown exceeds COMMITTED_SHUTDOWN_TIMEOUT,
//     httpServer.Close() is called for a hard drop and we still call
//     d.Close() so raft + WAL + worker goroutines exit cleanly. Exit 1
//     in that case so an orchestrator can tell the graceful path
//     missed its deadline.
//   - The HTTP server exits on its own (listener bind failure, etc.).
//     Treated like a fatal error — we still try to close the db.
//   - A raft error on d.ErrorC. Close the HTTP server, close the db,
//     and exit 1.
//
// Returns the process exit code. Factored out of the cobra Run closure
// so tests can drive the same shutdown logic in-process.
func runNode(d *db.DB, httpServer *nethttp.Server) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	httpErrC := make(chan error, 1)
	go func() {
		// When TLSConfig is set the cert + key are already embedded in
		// it via tls.LoadX509KeyPair (loadAPITLSConfig), so empty
		// strings are the correct arguments to ListenAndServeTLS.
		var err error
		if httpServer.TLSConfig != nil {
			err = httpServer.ListenAndServeTLS("", "")
		} else {
			err = httpServer.ListenAndServe()
		}
		if err != nil && !errors.Is(err, nethttp.ErrServerClosed) {
			httpErrC <- err
		}
		close(httpErrC)
	}()

	select {
	case <-ctx.Done():
		zap.L().Info("shutdown.signal_received")
		return gracefulShutdown(d, httpServer)
	case err, ok := <-httpErrC:
		if ok && err != nil {
			zap.L().Error("http server exited unexpectedly", zap.Error(err))
		}
		_ = d.Close()
		return 1
	case err, ok := <-d.ErrorC:
		if ok && err != nil {
			zap.L().Error("raft error", zap.Error(err))
		}
		_ = httpServer.Close()
		_ = d.Close()
		return 1
	}
}

// gracefulShutdown runs the signal-triggered drain: bounded
// httpServer.Shutdown, then db.Close. On Shutdown timeout we fall
// through to Close() (hard drop) but still call db.Close so the WAL
// fsync + worker drain happen — otherwise we'd be trading a slow drain
// for a dirty process exit, which is exactly what this ticket is trying
// to prevent.
func gracefulShutdown(d *db.DB, httpServer *nethttp.Server) int {
	timeout := shutdownTimeout()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	exitCode := 0
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		zap.L().Warn("shutdown.http_timeout",
			zap.Duration("timeout", timeout),
			zap.Error(err))
		_ = httpServer.Close()
		exitCode = 1
	} else {
		zap.L().Info("shutdown.http_closed")
	}

	if err := d.Close(); err != nil {
		zap.L().Warn("shutdown.db_close_error", zap.Error(err))
		exitCode = 1
	} else {
		zap.L().Info("shutdown.db_closed")
	}

	zap.L().Info("shutdown.done", zap.Int("exitCode", exitCode))
	return exitCode
}

// getenvDefault returns the value of env var name, or def when it is
// unset or empty.
func getenvDefault(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// nodeID reads COMMITTED_NODE_ID and fatal-exits on a bad value. The
// parsing lives in parseNodeID so it can be unit-tested without the
// process-exiting wrapper.
func nodeID() uint64 {
	id, err := parseNodeID(os.Getenv("COMMITTED_NODE_ID"))
	if err != nil {
		// G706 false positive: the value is an operator-supplied env var.
		log.Fatalf("%v", err) //nolint:gosec // G706
	}
	return id
}

// parseNodeID interprets the COMMITTED_NODE_ID env value: empty defaults
// to 1, otherwise it must be a positive uint64. A zero or unparseable
// value is an error rather than a silent fallback — collapsing a
// mistyped identity onto ID 1 would let two nodes claim the same raft ID
// and corrupt the group, which is far worse than refusing to start.
func parseNodeID(raw string) (uint64, error) {
	if raw == "" {
		return 1, nil
	}
	id, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || id == 0 {
		return 0, fmt.Errorf("COMMITTED_NODE_ID must be a positive integer (got %q)", raw)
	}
	return id, nil
}

// loadPeers builds the static raft peer set used for first-boot
// bootstrap and fatal-exits on a malformed COMMITTED_PEERS. Parsing
// lives in parsePeers so it can be unit-tested without the wrapper.
func loadPeers(id uint64) db.Peers {
	peers, err := parsePeers(id, os.Getenv("COMMITTED_PEERS"), getenvDefault("COMMITTED_PEER_URL", "http://127.0.0.1:9022"))
	if err != nil {
		// G706 false positive: the value is an operator-supplied env var.
		log.Fatalf("%v", err) //nolint:gosec // G706
	}
	return peers
}

// parsePeers builds the static raft peer set for first-boot bootstrap
// (raft.StartNode).
//
// raw is the COMMITTED_PEERS env value: when non-empty it is the full
// cluster membership as a comma-separated list of id=url pairs, e.g.
//
//	COMMITTED_PEERS="1=http://n1:9022,2=http://n2:9022,3=http://n3:9022"
//
// Every node receives the same COMMITTED_PEERS and the set must include
// this node's own id. Membership is consumed only on first boot; on
// restart it is restored from the WAL (raft.RestartNode), so editing
// COMMITTED_PEERS after a node has state has no effect — use the
// conf-change API for live membership changes.
//
// When raw is empty the node bootstraps a single-node cluster
// advertising selfURL (COMMITTED_PEER_URL) for itself — the historical
// laptop-dev default.
//
// Malformed input is an error rather than best-effort: a bad peer set
// yields split-brain or a node that can never reach quorum, both worse
// than a loud refusal to start.
func parsePeers(id uint64, raw, selfURL string) (db.Peers, error) {
	if raw == "" {
		return db.Peers{id: selfURL}, nil
	}

	peers := make(db.Peers)
	for entry := range strings.SplitSeq(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		k, v, ok := strings.Cut(entry, "=")
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if !ok || k == "" || v == "" {
			return nil, fmt.Errorf("COMMITTED_PEERS entry %q is not in id=url form", entry)
		}
		pid, err := strconv.ParseUint(k, 10, 64)
		if err != nil || pid == 0 {
			return nil, fmt.Errorf("COMMITTED_PEERS entry %q has an invalid peer id", entry)
		}
		if _, dup := peers[pid]; dup {
			return nil, fmt.Errorf("COMMITTED_PEERS has a duplicate peer id %d", pid)
		}
		peers[pid] = v
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("COMMITTED_PEERS is set but contains no valid peers")
	}
	if _, ok := peers[id]; !ok {
		return nil, fmt.Errorf("COMMITTED_PEERS must include this node's own COMMITTED_NODE_ID (%d)", id)
	}
	return peers, nil
}

// parseInt64Env reads an int64-valued env var. Returns (0, false)
// when the var is unset or unparseable, with a logged warning for
// the unparseable case — a typo in an HTTP-limit env var should be
// visible, not silently reverted to the default.
func parseInt64Env(name string) (int64, bool) {
	raw := os.Getenv(name)
	if raw == "" {
		return 0, false
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v <= 0 {
		zap.L().Warn(name+" invalid, using default", zap.String("value", raw))
		return 0, false
	}
	return v, true
}

// parseDurationEnv reads a Go-duration-formatted env var (e.g. "15s").
func parseDurationEnv(name string) (time.Duration, bool) {
	raw := os.Getenv(name)
	if raw == "" {
		return 0, false
	}
	v, err := time.ParseDuration(raw)
	if err != nil || v <= 0 {
		zap.L().Warn(name+" invalid, using default", zap.String("value", raw))
		return 0, false
	}
	return v, true
}

// shutdownTimeout returns the configured graceful-shutdown deadline.
// Reads COMMITTED_SHUTDOWN_TIMEOUT (Go duration syntax, e.g. "45s").
// An unset or unparseable value falls back to defaultShutdownTimeout
// with a warning — a misconfigured env var should not silently disable
// the graceful path.
func shutdownTimeout() time.Duration {
	raw := os.Getenv("COMMITTED_SHUTDOWN_TIMEOUT")
	if raw == "" {
		return defaultShutdownTimeout
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		zap.L().Warn("COMMITTED_SHUTDOWN_TIMEOUT invalid, using default",
			zap.String("value", raw),
			zap.Duration("default", defaultShutdownTimeout))
		return defaultShutdownTimeout
	}
	return d
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

func dbParser() *syncsql.DBParser {
	ds := make(map[string]syncsql.Dialect)
	p := &syncsql.DBParser{Dialects: ds}
	ds["mysql"] = &syncmysql.MySQLDialect{}
	return p
}

func ingestableParser(t ingestablesql.Typer) *ingestablesql.IngestableParser {
	p := ingestablesql.NewIngestableParser(t)
	p.Dialects["mysql"] = &ingestablemysql.MySQLDialect{}
	p.Dialects["postgres"] = &ingestablepostgres.PostgreSQLDialect{}
	return p
}

func init() {
	rootCmd.AddCommand(nodeCmd)
}
