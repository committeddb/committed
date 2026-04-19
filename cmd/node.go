package cmd

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	nethttp "net/http"
	"os"
	"os/signal"
	"strconv"
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
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("node called")

		v := version.Get()
		zap.L().Info("committed starting",
			zap.String("version", v.Version),
			zap.String("commit", v.Commit),
			zap.String("buildDate", v.BuildDate),
			zap.String("goVersion", v.GoVersion),
		)

		url := flag.String("url", "http://127.0.0.1:9022", "url with port")
		id := flag.Uint64("id", 1, "node ID")
		addr := flag.String("addr", ":8080", "node ID")
		sync := make(chan *db.SyncableWithID)
		ingest := make(chan *db.IngestableWithID)

		p := parser.New()
		s, err := wal.Open("./data", p, sync, ingest)
		if err != nil {
			log.Fatalf("cannot open storage: %v", err)
		}

		peers := make(db.Peers)
		peers[*id] = *url

		var dbOpts []db.Option

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

		d := db.New(*id, peers, s, p, sync, ingest, dbOpts...)
		fmt.Printf("Raft Running...\n")

		var httpOpts []http.Option
		if token := os.Getenv("COMMITTED_API_TOKEN"); token != "" {
			httpOpts = append(httpOpts, http.WithBearerToken(token))
		}

		h := http.New(d, httpOpts...)
		fmt.Printf("API Listening on %s...\n", *addr)

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

		exitCode := runNode(d, h.NewServer(*addr, serverOpts...))
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
		err := httpServer.ListenAndServe()
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
