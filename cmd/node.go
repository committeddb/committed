package cmd

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	nethttp "net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/httptransport"
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

// defaultShutdownTimeout is the TOTAL budget for graceful shutdown — the HTTP
// drain, then the worker drain and leadership hand-off in db.Close, all clamped to
// this one deadline (COMMITTED_SHUTDOWN_TIMEOUT). Kubernetes pod
// terminationGracePeriodSeconds defaults to 30s; keep it at least a few seconds
// above this so the final raft stop — the one unclamped, normally-instant tail —
// finishes before SIGKILL.
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
  COMMITTED_API_URL    this node's advertised HTTP API base URL, e.g.
                       "http://n1:8080". Self-announced to the cluster so a
                       follower can proxy a leader-only read
                       (GET /v1/membership) to the leader — letting a caller
                       behind a load balancer get a leader-truthful answer
                       from any node. Unset disables the announce: such a
                       read on a follower returns 503 + the leader id so the
                       caller can target the leader directly.
  COMMITTED_DATA_DIR   data directory for WAL/state (default "./data")
  COMMITTED_PEER_URL   this node's advertised raft peer URL
                       (default "http://127.0.0.1:9022"); used when
                       COMMITTED_PEERS is unset
  COMMITTED_PEERS      the initial cluster membership as id=url pairs,
                       e.g. "1=http://n1:9022,2=http://n2:9022". The
                       same value is given to every node and must
                       include this node's own COMMITTED_NODE_ID. On the
                       first boot it bootstraps the cluster; on every boot
                       it seeds the peer transport. You do NOT need to
                       update it after growing the cluster — a member added
                       at runtime has its raft URL persisted and reconciled
                       onto the transport automatically on restart.
  COMMITTED_JOIN       when truthy, this node joins an existing cluster
                       instead of bootstrapping a new one: it starts with
                       no raft configuration and learns its membership from
                       the leader after a "committed member add" naming it
                       commits. COMMITTED_PEERS still seeds the transport.

  COMMITTED_DISK_WARN_PERCENT
                       free-space percent at which the disk watcher logs a
                       warning (default 20). Writes still flow.
  COMMITTED_DISK_CRITICAL_PERCENT
                       free-space percent at which user-data proposals are
                       rejected with 507 (default 10). Config changes and
                       internal sync/ingest housekeeping still flow, and
                       compaction is nudged to free space sooner.
  COMMITTED_DISK_FULL_PERCENT
                       free-space percent at which config writes are also
                       frozen (default 3); user data and config return 507
                       while sync/ingest checkpoints and compaction continue.
                       Writes re-enable automatically once free space recovers
                       above the warn threshold. Thresholds must be descending
                       (warn > critical > full).
  COMMITTED_DISK_REPORT_INTERVAL
                       cadence (Go duration, default 10s) at which each member
                       reports its disk state to the leader and the leader
                       recomputes the cluster-wide write-admission verdict
                       (admit iff the leader and a quorum of voters have disk
                       headroom), which every node enforces at its propose
                       gate. Requires COMMITTED_API_URL on every node and a
                       cluster-uniform COMMITTED_API_TOKEN; without them the
                       gate falls back to the node-local decision. "0"
                       disables cluster-aware admission entirely.

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
                       applies when CORS is enabled.

  OTEL_EXPORTER_OTLP_ENDPOINT
                       OpenTelemetry Collector address (e.g.
                       "http://otel-collector:4317"). Setting it enables
                       metrics, which are PUSHED via OTLP — committed serves
                       no /metrics scrape endpoint. Unset (default) disables
                       metrics with zero overhead. See
                       docs/operations/metrics.md.`,
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
		// meterProvider is kept so the observable lag gauges can be registered
		// after db.New (their callbacks read the live db at scrape time). nil
		// when metrics are disabled.
		var meterProvider *sdkmetric.MeterProvider
		if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
			ctx := context.Background()
			exporter, err := otlpmetricgrpc.New(ctx)
			if err != nil {
				log.Fatalf("otel exporter: %v", err)
			}
			meterProvider = sdkmetric.NewMeterProvider(
				sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)),
			)
			defer func() { _ = meterProvider.Shutdown(context.Background()) }()
			m = metrics.New(meterProvider.Meter("committed"))
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

		// Self-announce this node's cluster feature level so the version-skew
		// gate can compute the cluster-agreed minimum and hold emission of
		// anything an older peer can't yet apply. Real nodes always announce;
		// only unit tests (which assert exact entry counts) leave it off.
		dbOpts = append(dbOpts, db.WithVersionAnnounce())

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
		peerTLS := loadPeerTLSInfo()
		if peerTLS != nil {
			dbOpts = append(dbOpts, db.WithTLSInfo(peerTLS))
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

		// The disk-usage watcher polls the data dir's filesystem and, as free
		// space falls, first warns, then rejects user writes (507), then goes
		// read-only — so a filling disk degrades gracefully instead of the
		// raft loop fatal-exiting on ENOSPC. Always enabled (Path is the data
		// dir); the COMMITTED_DISK_*_PERCENT env vars override the descending
		// warn/critical/full thresholds, unset uses db.DefaultDisk*Percent.
		dbOpts = append(dbOpts, db.WithDiskWatcher(db.DiskWatcherConfig{
			Path:            dataDir,
			WarnPercent:     parsePercentEnv("COMMITTED_DISK_WARN_PERCENT"),
			CriticalPercent: parsePercentEnv("COMMITTED_DISK_CRITICAL_PERCENT"),
			FullPercent:     parsePercentEnv("COMMITTED_DISK_FULL_PERCENT"),
		}))

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

		// COMMITTED_API_URL is this node's advertised HTTP API base URL. When
		// set, the node self-announces it so a follower can proxy a leader-only
		// read (GET /v1/membership) to the leader. Unset leaves the node's API
		// address unknown to the proxy (the documented degraded path).
		if apiURL := os.Getenv("COMMITTED_API_URL"); apiURL != "" {
			dbOpts = append(dbOpts, db.WithAdvertisedAPIURL(apiURL))
			zap.L().Info("advertising API URL for leader-read proxying", zap.String("url", apiURL))
		}

		// Cluster-aware disk admission: each member reports its disk state
		// to the leader over the HTTP API and enforces the verdict the
		// response carries. The report sender reuses the leader-read proxy's
		// TLS client (same peer-API trust) and the cluster's API bearer
		// token (the report endpoint is authenticated like every write).
		// Read here, before db.New, and reused for the HTTP options below.
		apiToken := os.Getenv("COMMITTED_API_TOKEN")
		proxyClient, err := loadProxyClient()
		if err != nil {
			// G706 false positive: values come from operator-supplied env vars.
			log.Fatalf("leader-read proxy client: %v", err) //nolint:gosec // G706
		}
		dbOpts = append(dbOpts, db.WithDiskReportHTTP(proxyClient, apiToken))
		if raw := os.Getenv("COMMITTED_DISK_REPORT_INTERVAL"); raw == "0" {
			dbOpts = append(dbOpts, db.WithDiskReportInterval(-1))
			zap.L().Info("cluster disk admission disabled (COMMITTED_DISK_REPORT_INTERVAL=0); the propose gate is node-local only")
		} else if d, ok := parseDurationEnv("COMMITTED_DISK_REPORT_INTERVAL"); ok {
			dbOpts = append(dbOpts, db.WithDiskReportInterval(d))
		}

		dbOpts = append(dbOpts, db.WithTransportFactory(httptransport.Factory()))
		// Inject the bearer token the peer transport sends, read once above, so
		// the transport constructor doesn't reach into the environment itself.
		dbOpts = append(dbOpts, db.WithAPIToken(apiToken))

		d := db.New(id, peers, s, p, sync, ingest, dbOpts...)
		fmt.Printf("Raft Running...\n")

		// Observable sync/ingest lag gauges read the live db at scrape time, so
		// they register after db.New. No-op when metrics are disabled.
		if meterProvider != nil {
			if err := metrics.RegisterLagGauges(meterProvider.Meter("committed"), d); err != nil {
				log.Fatalf("register lag gauges: %v", err)
			}
		}

		var httpOpts []http.Option
		if apiToken != "" {
			httpOpts = append(httpOpts, http.WithBearerToken(apiToken))
		}
		if m != nil {
			httpOpts = append(httpOpts, http.WithMetrics(m))
		}
		if n, ok := parseInt64Env("COMMITTED_MAX_PROPOSAL_BYTES"); ok {
			// Keep the request-body cap above the (raised) proposal cap so a large
			// but valid proposal isn't false-rejected at the HTTP body read; the
			// body is JSON/TOML and larger than the marshaled proposal it produces.
			httpOpts = append(httpOpts, http.WithMaxBodyBytes(n*2))
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

		// The leader-read proxy (GET /v1/membership on a follower) calls a
		// peer's API. When that API serves TLS with a private CA or
		// self-signed certs, the proxy client needs the trust anchor (and,
		// under mTLS, a client cert); loadProxyClient (called above, shared
		// with the disk-report sender) builds it from the COMMITTED_HTTP_TLS_*
		// env vars. Nil → the http layer's default client (system-root TLS),
		// which is correct for plaintext or publicly-signed peer APIs.
		if proxyClient != nil {
			httpOpts = append(httpOpts, http.WithProxyClient(proxyClient))
		}

		h := http.New(d, httpOpts...)
		fmt.Printf("API Listening on %s...\n", addr)

		// NB: the database parser is registered earlier, before wal.Open (see
		// above). These three need *d (the ingestable parser) or are simply
		// fine to register here alongside it.
		d.AddIngestableParser("sql", ingestableParser(d, d))
		d.AddSyncableParser("sql", &syncsql.SyncableParser{Metrics: m})
		d.AddSyncableParser("sql-projection", &syncsql.ProjectionSyncableParser{Metrics: m})
		d.AddSyncableParser("http", &synchttp.SyncableParser{})

		// Restore ingestable and syncable workers for configs applied in a
		// previous run. These MUST run after the sub-parsers above are
		// registered and after db.New started draining the ingest/sync
		// channels — see the ordering contract on RestoreIngestableWorkers /
		// RestoreSyncableWorkers. Spawning them from inside wal.Open (as the
		// ingestable side once did) raced this registration and silently
		// dropped the workers on restart under load.
		go s.RequestIngestReconcile()
		go s.RequestSyncReconcile()

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
		}
		// Security-posture floor: loud Error + startup banner if the write API is
		// reachable off-host with no auth (see docs/operations/authentication.md).
		// Deliberately not a refuse-to-boot — self-hosted test use is supported.
		warnInsecurePosture(addr, apiToken, tlsCfg, peerTLS != nil)

		exitCode := runNode(d, h.NewServer(addr, serverOpts...))

		// runNode always closes the DB (raft + workers) before returning; now
		// close the WAL Storage we own. db.Close deliberately leaves this to the
		// owner (so post-close storage.Database queries still work), but nothing
		// was ever calling it — leaking the background scrubber goroutine and the
		// SQL sink connection pools, and skipping the final WAL/bbolt handle close,
		// on every shutdown. Closing here also releases any config-notification
		// sender still blocked on the sync/ingest channel (see Storage.closeC).
		if err := s.Close(); err != nil {
			zap.L().Warn("shutdown.storage_close_error", zap.Error(err))
		}

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
	deadline := time.Now().Add(timeout)
	shutdownCtx, cancel := context.WithDeadline(context.Background(), deadline)
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

	// db.Close gets whatever budget remains under the SAME deadline, so the total
	// graceful path (HTTP drain + worker drain + leadership hand-off) stays within
	// COMMITTED_SHUTDOWN_TIMEOUT rather than running additive to it.
	if err := d.CloseWithDeadline(deadline); err != nil {
		zap.L().Warn("shutdown.db_close_error", zap.Error(err))
		exitCode = 1
	} else {
		zap.L().Info("shutdown.db_closed")
	}

	zap.L().Info("shutdown.done", zap.Int("exitCode", exitCode))
	return exitCode
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

func ingestableParser(t ingestablesql.Typer, epoch ingestablesql.TopicEpochReader) *ingestablesql.IngestableParser {
	p := ingestablesql.NewIngestableParser(t)
	p.Dialects["mysql"] = &ingestablemysql.MySQLDialect{}
	p.Dialects["postgres"] = &ingestablepostgres.PostgreSQLDialect{}
	// Wire the delete-surviving per-topic refresh-epoch floor so a same-topic
	// recreate resumes its generation above the rows still on the sink.
	p.EpochFloor = epoch
	return p
}

func init() {
	rootCmd.AddCommand(nodeCmd)
}
