// Command eventstore-load is the event-store acceptance workload: it drives
// the event-store recipe (docs/recipes/event-store-read-models.md) against a
// running committed cluster and measures how quickly read models follow the
// source.
//
// It creates the recipe's events table in the source, posts the recipe's
// configs as published, appends events one per source transaction at a fixed
// rate, and measures, from the load generator's own clock:
//
//   - commit-to-row latency: from a source commit returning to the read-model
//     row showing that event's version (a sample of events);
//   - catch-up: from the last commit to every row matching the source;
//   - correctness: every row against the value the generator expects;
//   - idle latency: single appends into a quiet system;
//   - rebuild: a second projection built from the start of the log.
//
// It knows nothing about where the cluster runs. bench/eventstore/run.sh
// starts the cluster and the databases and calls it.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type options struct {
	nodes         []string
	engine        string
	payload       string
	srcDSN        string
	srcURL        string
	readDSN       string
	readURL       string
	recipe        string
	results       string
	rate          float64
	duration      time.Duration
	writers       int
	hotStreams    int
	hotEvents     int
	sampleEvery   int
	idleProbes    int
	skipRebuild   bool
	readyTimeout  time.Duration
	catchUpLimit  time.Duration
	rebuildLimit  time.Duration
	stallLimit    time.Duration
	latencyBar    time.Duration
	catchUpBar    time.Duration
	label         string
	sqlserverPoll string
	pinFollower   bool
	history       int64
	snapshotOnly  bool
	fromLog       string
}

func main() {
	var o options
	var nodes string
	flag.StringVar(&nodes, "nodes", "http://127.0.0.1:18080", "committed API base URLs, comma-separated")
	flag.StringVar(&o.engine, "engine", "postgres", "source engine: postgres, mysql, or sqlserver")
	flag.StringVar(&o.payload, "payload", "", "payload column type (default: jsonb, JSON, or NVARCHAR(MAX) by engine)")
	flag.StringVar(&o.srcDSN, "src-dsn", "", "the generator's own connection to the source (driver DSN)")
	flag.StringVar(&o.srcURL, "src-url", "", "the source connection string as committed sees it (use ${APP_DB_PASSWORD})")
	flag.StringVar(&o.readDSN, "read-dsn", "", "the generator's own connection to the read-model Postgres")
	flag.StringVar(&o.readURL, "read-url", "", "the read-model connection string as committed sees it (search_path=read)")
	flag.StringVar(&o.recipe, "recipe", "docs/recipes/event-store-read-models.md", "the recipe to take configs from")
	flag.StringVar(&o.results, "results", "results", "directory for report.json, summary.txt, timeseries.jsonl")
	flag.Float64Var(&o.rate, "rate", 1000, "source transactions per second, one event each")
	flag.DurationVar(&o.duration, "duration", 10*time.Minute, "how long to append")
	flag.IntVar(&o.writers, "writers", 32, "concurrent source connections")
	flag.IntVar(&o.hotStreams, "hot-streams", 4, "streams that grow to -hot-events over the run")
	flag.IntVar(&o.hotEvents, "hot-events", 10000, "events each hot stream reaches by the end of the run")
	flag.IntVar(&o.sampleEvery, "sample-every", 10, "measure commit-to-row for one event in this many")
	flag.IntVar(&o.idleProbes, "idle-probes", 20, "single appends into a quiet system")
	flag.BoolVar(&o.skipRebuild, "skip-rebuild", false, "skip building a second projection from the log")
	flag.DurationVar(&o.readyTimeout, "ready-timeout", 5*time.Minute, "how long to wait for the cluster and the ingestable")
	flag.DurationVar(&o.catchUpLimit, "catch-up-limit", 30*time.Minute, "give up waiting for the read model after this")
	flag.DurationVar(&o.rebuildLimit, "rebuild-limit", 60*time.Minute, "give up waiting for the rebuilt projection after this")
	flag.DurationVar(&o.stallLimit, "stall-limit", 2*time.Minute, "give up waiting for a read model that has not changed for this long")
	flag.DurationVar(&o.latencyBar, "latency-bar", time.Second, "the bar for p99 commit-to-row")
	flag.DurationVar(&o.catchUpBar, "catch-up-bar", 5*time.Second, "the bar for catch-up after the last append")
	flag.StringVar(&o.label, "label", "", "a name for this run in the report (e.g. the commit)")
	flag.StringVar(&o.sqlserverPoll, "sqlserver-poll-interval", "", "set [sql.options] pollInterval on a SQL Server ingestable (default: the recipe's, i.e. none)")
	flag.Int64Var(&o.history, "history", 0, "instead of the live workload, bulk-load this many events into the source, snapshot them into the log, and time building the projection over them (postgres only; set -rebuild-limit for large histories)")
	flag.BoolVar(&o.snapshotOnly, "history-snapshot-only", false, "with -history: stop once the history is in the log — delete the ingestable (so a restore of the log needs no source) and write dataset.json, which describes the history so a later run can check a build over it — instead of building the projection")
	flag.StringVar(&o.fromLog, "history-from-log", "", "the dataset.json of a saved history: the cluster was restored from its log, so regenerate the history from it instead of loading the source, check the log came back whole, and build over it (with -history-snapshot-only, stop after the check)")
	flag.BoolVar(&o.pinFollower, "pin-follower", false, "pin the projections to a follower's zone (run.sh starts node i in zone zi), so they sync off the leader")
	flag.Parse()

	for _, n := range strings.Split(nodes, ",") {
		if n = strings.TrimRight(strings.TrimSpace(n), "/"); n != "" {
			o.nodes = append(o.nodes, n)
		}
	}
	if o.payload == "" {
		o.payload = map[string]string{"postgres": "jsonb", "mysql": "JSON", "sqlserver": "NVARCHAR(MAX)"}[o.engine]
	}
	if err := validate(o); err != nil {
		fmt.Fprintln(os.Stderr, "eventstore-load:", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if o.history > 0 || o.fromLog != "" {
		hrep, err := runHistory(ctx, o)
		if hrep != nil {
			if werr := writeHistoryReport(o.results, hrep); werr != nil {
				fmt.Fprintln(os.Stderr, "eventstore-load: write report:", werr)
			}
			fmt.Print(hrep.Summary())
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "eventstore-load:", err)
			os.Exit(1)
		}
		if !hrep.Pass {
			os.Exit(3)
		}
		return
	}
	rep, err := run(ctx, o)
	if rep != nil {
		if werr := writeReport(o.results, rep); werr != nil {
			fmt.Fprintln(os.Stderr, "eventstore-load: write report:", werr)
		}
		fmt.Print(rep.Summary())
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "eventstore-load:", err)
		os.Exit(1)
	}
	if !rep.Pass {
		os.Exit(3)
	}
}

func validate(o options) error {
	switch {
	case o.engine != "postgres" && o.engine != "mysql" && o.engine != "sqlserver":
		return fmt.Errorf("unknown -engine %q", o.engine)
	case len(o.nodes) == 0:
		return fmt.Errorf("-nodes is empty")
	case o.srcDSN == "" || o.srcURL == "" || o.readDSN == "" || o.readURL == "":
		return fmt.Errorf("-src-dsn, -src-url, -read-dsn and -read-url are required")
	case o.rate <= 0 || o.writers <= 0 || o.duration <= 0:
		return fmt.Errorf("-rate, -writers and -duration must be positive")
	case o.sampleEvery <= 0:
		return fmt.Errorf("-sample-every must be positive")
	case o.snapshotOnly && o.history <= 0 && o.fromLog == "":
		return fmt.Errorf("-history-snapshot-only needs -history or -history-from-log")
	case o.fromLog != "" && o.history > 0:
		return fmt.Errorf("give -history or -history-from-log, not both: the dataset sets the history's size")
	}
	return nil
}

func writeReport(dir string, rep *Report) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "report.json"), b, 0o600); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "summary.txt"), []byte(rep.Summary()), 0o600)
}
