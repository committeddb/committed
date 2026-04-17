package cmd

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/philborlin/committed/internal/cluster/db"
	parser "github.com/philborlin/committed/internal/cluster/db/parser"
	"github.com/philborlin/committed/internal/cluster/db/wal"
	"github.com/philborlin/committed/internal/cluster/http"
	"github.com/philborlin/committed/internal/cluster/metrics"
	ingestablesql "github.com/philborlin/committed/internal/cluster/ingestable/sql"
	ingestablemysql "github.com/philborlin/committed/internal/cluster/ingestable/sql/mysql"
	ingestablepostgres "github.com/philborlin/committed/internal/cluster/ingestable/sql/postgres"
	synchttp "github.com/philborlin/committed/internal/cluster/syncable/http"
	syncsql "github.com/philborlin/committed/internal/cluster/syncable/sql"
	syncmysql "github.com/philborlin/committed/internal/cluster/syncable/sql/dialects"
	"github.com/spf13/cobra"
)

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
			defer provider.Shutdown(context.Background())
			m := metrics.New(provider.Meter("committed"))
			dbOpts = append(dbOpts, db.WithMetrics(m))
		}

		db := db.New(*id, peers, s, p, sync, ingest, dbOpts...)
		fmt.Printf("Raft Running...\n")

		var httpOpts []http.Option
		if token := os.Getenv("COMMITTED_API_TOKEN"); token != "" {
			httpOpts = append(httpOpts, http.WithBearerToken(token))
		}

		h := http.New(db, httpOpts...)
		fmt.Printf("API Listening on %s...\n", *addr)

		db.AddDatabaseParser("sql", dbParser())
		db.AddIngestableParser("sql", ingestableParser(db))
		db.AddSyncableParser("sql", &syncsql.SyncableParser{})
		db.AddSyncableParser("http", &synchttp.SyncableParser{})

		db.EatCommitC()

		go func() {
			if err := h.ListenAndServe(*addr); err != nil {
				log.Fatal(err)
			}
		}()

		if err, ok := <-db.ErrorC; ok {
			log.Fatalf("raft error: %v", err)
		}
	},
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
		log.Fatalf("peer mTLS: all of COMMITTED_TLS_CA_FILE, COMMITTED_TLS_CERT_FILE, COMMITTED_TLS_KEY_FILE must be set together (got CA=%q CERT=%q KEY=%q)", ca, cert, key)
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
