package cmd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	nethttp "net/http"
	"net/url"
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

	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
	"github.com/committeddb/committed/internal/cluster/http"
	ingestablesql "github.com/committeddb/committed/internal/cluster/ingestable/sql"
	ingestablemysql "github.com/committeddb/committed/internal/cluster/ingestable/sql/mysql"
	ingestablepostgres "github.com/committeddb/committed/internal/cluster/ingestable/sql/postgres"
	"github.com/committeddb/committed/internal/cluster/metrics"
	synchttp "github.com/committeddb/committed/internal/cluster/syncable/http"
	syncsql "github.com/committeddb/committed/internal/cluster/syncable/sql"
	syncdialects "github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
	"github.com/committeddb/committed/internal/version"
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
                       membership is restored from the WAL.
  COMMITTED_JOIN       when truthy, this node joins an existing cluster
                       instead of bootstrapping a new one: it starts with
                       no raft configuration and learns its membership from
                       the leader after a "committed member add" naming it
                       commits. COMMITTED_PEERS still seeds the transport.

  COMMITTED_HTTP_CORS_ORIGINS
                       comma-separated browser-origin allowlist, e.g.
                       "https://app.example.com,https://admin.example.com",
                       or the literal "*" to allow any origin. Unset
                       (default) disables CORS entirely — no
                       Access-Control-* headers are emitted. Each entry
                       must be scheme://host or "*"; a malformed entry is
                       a hard startup error.
  COMMITTED_HTTP_CORS_METHODS
                       comma-separated allowed request methods (default
                       "GET,POST,PUT,DELETE,OPTIONS"). Only applies when
                       CORS is enabled.
  COMMITTED_HTTP_CORS_HEADERS
                       comma-separated allowed request headers (default
                       "Content-Type,Authorization,X-Request-ID"). Only
                       applies when CORS is enabled.`,
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
		// The database sub-parser MUST be registered before wal.Open: Open
		// calls loadDatabases, which rebuilds every persisted database handle
		// via Parser.ParseDatabase. Register it afterward (as the ingestable /
		// syncable parsers are, because they need *d) and a restarted node
		// silently fails to rebuild its databases — which then breaks
		// RestoreSyncableWorkers, since a syncable resolves its sink through
		// storage.Database. dbParser() has no dependency on *d, so it can and
		// must be wired here. See wal.TestDatabaseRestore_ParserOrdering.
		p.AddDatabaseParser("sql", dbParser())

		// Build metrics before wal.Open so the storage layer can emit the
		// committed.wal.corrupt_entries counter, including for corruption
		// detected during the Open recovery reads. When
		// OTEL_EXPORTER_OTLP_ENDPOINT is set (e.g., "localhost:4317"), metrics
		// are pushed to an OTel Collector via gRPC; the collector routes to
		// backends (Prometheus, Datadog, etc.). When unset, m stays nil and
		// every metrics call is a nil-safe no-op (zero overhead).
		var m *metrics.Metrics
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
			m = metrics.New(provider.Meter("committed"))
		}

		// Pass the real logger so storage-layer warnings are visible in
		// production. Without this the Storage defaults to a Nop logger, which
		// is why a degraded ingestable restore (and other wal warnings) used
		// to fail completely silently.
		walOpts := []wal.Option{wal.WithLogger(zap.L())}
		if m != nil {
			walOpts = append(walOpts, wal.WithMetrics(m))
		}
		s, err := wal.Open(dataDir, p, sync, ingest, walOpts...)
		if err != nil {
			log.Fatalf("cannot open storage: %v", err)
		}

		var dbOpts []db.Option
		if m != nil {
			dbOpts = append(dbOpts, db.WithMetrics(m))
		}

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

		// COMMITTED_SCRUB_INTERVAL sets the automatic right-to-be-forgotten
		// scrub cadence (Go duration, e.g. "30m"). 0 disables the scheduler;
		// the manual POST /v1/scrub lever still works. Unset uses the default
		// (db.DefaultScrubInterval).
		if d, ok := parseDurationEnv("COMMITTED_SCRUB_INTERVAL"); ok {
			dbOpts = append(dbOpts, db.WithScrubInterval(d))
		}

		// COMMITTED_JOIN marks this node as joining an existing cluster
		// rather than bootstrapping a new one. A joining node comes up with
		// no raft configuration and learns its membership from the leader
		// once an "member add" naming it commits — so COMMITTED_PEERS must
		// still list the existing members (and itself, for the listener URL)
		// to seed the transport, but it is NOT bootstrapped into a config.
		// Without this flag a fresh node would StartNode the static peer set
		// and split-brain against the cluster it meant to join. See
		// docs/operations/membership.md.
		if boolEnv("COMMITTED_JOIN") {
			dbOpts = append(dbOpts, db.WithJoin())
			zap.L().Info("joining existing cluster (COMMITTED_JOIN set); membership will be learned from the leader")
		}

		d := db.New(id, peers, s, p, sync, ingest, dbOpts...)
		fmt.Printf("Raft Running...\n")

		var httpOpts []http.Option
		if token := os.Getenv("COMMITTED_API_TOKEN"); token != "" {
			httpOpts = append(httpOpts, http.WithBearerToken(token))
		}

		corsOrigins, err := loadCORSOrigins()
		if err != nil {
			// G706 false positive: the value is an operator-supplied env var.
			log.Fatalf("CORS: %v", err) //nolint:gosec // G706
		}
		if len(corsOrigins) > 0 {
			httpOpts = append(httpOpts, http.WithCORS(
				corsOrigins,
				parseListEnv("COMMITTED_HTTP_CORS_METHODS"),
				parseListEnv("COMMITTED_HTTP_CORS_HEADERS"),
			))
			zap.L().Info("API CORS enabled", zap.Strings("origins", corsOrigins))
		}

		// Default (linearizable) GETs run a raft ReadIndex round-trip; this
		// bounds how long one waits for quorum confirmation before returning
		// 503, so a partitioned node fails fast instead of holding the
		// connection until the write timeout. See docs/consistency.md.
		if d, ok := parseDurationEnv("COMMITTED_HTTP_READ_INDEX_TIMEOUT"); ok {
			httpOpts = append(httpOpts, http.WithReadIndexTimeout(d))
		}

		h := http.New(d, httpOpts...)
		fmt.Printf("API Listening on %s...\n", addr)

		// NB: the database parser is registered earlier, before wal.Open (see
		// above). These three need *d (the ingestable parser) or are simply
		// fine to register here alongside it.
		d.AddIngestableParser("sql", ingestableParser(d))
		d.AddSyncableParser("sql", &syncsql.SyncableParser{})
		d.AddSyncableParser("http", &synchttp.SyncableParser{})

		// Restore ingestable and syncable workers for configs applied in a
		// previous run. These MUST run after the sub-parsers above are
		// registered and after db.New started draining the ingest/sync
		// channels — see the ordering contract on RestoreIngestableWorkers /
		// RestoreSyncableWorkers. Spawning them from inside wal.Open (as the
		// ingestable side once did) raced this registration and silently
		// dropped the workers on restart under load.
		go s.RestoreIngestableWorkers()
		go s.RestoreSyncableWorkers()

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
// "committed member add/remove" commands (the /v1/membership API) for
// live membership changes. A node joining an existing cluster sets
// COMMITTED_JOIN=true so its COMMITTED_PEERS seeds the transport without
// bootstrapping a competing configuration. See
// docs/operations/membership.md.
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

// loadCORSOrigins parses the comma-separated COMMITTED_HTTP_CORS_ORIGINS
// allowlist. Unset or empty returns (nil, nil) — CORS stays off and the
// http package emits no Access-Control-* headers. Each entry must be the
// literal "*" (allow any origin) or an absolute scheme://host origin;
// anything else is a hard error so a typo'd origin fails fast at startup
// rather than silently rejecting every browser preflight at runtime.
//
// Returning (origins, err) instead of calling log.Fatalf lets node_test.go
// exercise the parsing and error cases directly, matching loadAPITLSConfig.
func loadCORSOrigins() ([]string, error) {
	raw := os.Getenv("COMMITTED_HTTP_CORS_ORIGINS")
	if raw == "" {
		return nil, nil
	}

	var origins []string
	for entry := range strings.SplitSeq(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if entry == "*" {
			origins = append(origins, entry)
			continue
		}
		u, err := url.Parse(entry)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return nil, fmt.Errorf("COMMITTED_HTTP_CORS_ORIGINS entry %q is not a valid origin (want scheme://host, e.g. https://app.example.com, or \"*\")", entry)
		}
		origins = append(origins, entry)
	}
	if len(origins) == 0 {
		return nil, fmt.Errorf("COMMITTED_HTTP_CORS_ORIGINS is set but contains no valid origins")
	}
	return origins, nil
}

// parseListEnv reads a comma-separated env var into a trimmed, non-empty
// string slice. Unset or empty returns nil so the caller falls back to
// its own defaults.
func parseListEnv(name string) []string {
	raw := os.Getenv(name)
	if raw == "" {
		return nil
	}
	var out []string
	for entry := range strings.SplitSeq(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry != "" {
			out = append(out, entry)
		}
	}
	return out
}

// boolEnv reports whether env var name holds a truthy value, parsed by
// strconv.ParseBool ("1", "t", "true", "TRUE", etc.). Unset, empty, or
// unparseable all read as false — a flag-style env var is opt-in, so any
// non-affirmative value leaves the default behavior in place.
func boolEnv(name string) bool {
	v, err := strconv.ParseBool(os.Getenv(name))
	return err == nil && v
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
	// Both dialects live in the same syncable/sql/dialects package. The
	// ingestable side wires postgres too (see ingestableParser); a syncable
	// sink config with dialect = "postgres" failed with "dialect postgres
	// not found" until this was added.
	ds["mysql"] = &syncdialects.MySQLDialect{}
	ds["postgres"] = &syncdialects.PostgreSQLDialect{}
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
