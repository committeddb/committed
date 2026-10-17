package main

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	mrand "math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// The history workload measures building a projection over a log that
// already holds a large history (-history N): N events are bulk-loaded into
// the PostgreSQL source with COPY, the ingestable snapshots them into the
// log (one row per proposal, as onboarding a real events table does), and
// only then is the projection posted. It reports the snapshot time, the
// projection's build time from posting to a complete, correct read model,
// and checks every row.

// historyReport is report.json for a history run.
type historyReport struct {
	Label    string    `json:"label,omitempty"`
	Started  time.Time `json:"started"`
	Hardware string    `json:"hardware,omitempty"`
	Nodes    int       `json:"nodes"`

	Events     int64 `json:"events"`
	Streams    int   `json:"streams"`
	HotStreams []int `json:"hotStreamSizes"`

	LoadSeconds     float64 `json:"loadSeconds"`     // COPY into the source
	SnapshotSeconds float64 `json:"snapshotSeconds"` // ingestable posted → every node applied the snapshot
	LogEntries      uint64  `json:"logEntries"`      // applied index once the snapshot applied

	BuildSeconds float64 `json:"buildSeconds"` // projection posted → every row present, lag 0
	BuildRate    float64 `json:"buildEventsPerSec"`
	BuildTimeout bool    `json:"buildTimeout,omitempty"`

	Rows     int      `json:"rows"`
	Wrong    int      `json:"wrong"`
	Extra    int      `json:"extra"`
	Examples []string `json:"examples,omitempty"`
	Pass     bool     `json:"pass"`
}

func (r *historyReport) Summary() string {
	var b strings.Builder
	verdict := "FAIL"
	if r.Pass {
		verdict = "PASS"
	}
	fmt.Fprintf(&b, "EVENT-STORE HISTORY %s  %s\n", verdict, r.Label)
	fmt.Fprintf(&b, "  %d nodes %s\n", r.Nodes, r.Hardware)
	fmt.Fprintf(&b, "  history: %d events in %d streams; hot streams %v\n", r.Events, r.Streams, r.HotStreams)
	fmt.Fprintf(&b, "  load into the source (COPY): %.1fs\n", r.LoadSeconds)
	fmt.Fprintf(&b, "  snapshot into the log: %.1fs (%.0f events/sec), %d log entries\n",
		r.SnapshotSeconds, float64(r.Events)/max(r.SnapshotSeconds, 1e-9), r.LogEntries)
	if r.BuildTimeout {
		fmt.Fprintf(&b, "  projection build: did not finish within the limit (%.1fs)\n", r.BuildSeconds)
	} else {
		fmt.Fprintf(&b, "  projection build: %.1fs (%.0f events/sec)\n", r.BuildSeconds, r.BuildRate)
	}
	fmt.Fprintf(&b, "  rows %d, wrong %d, extra %d\n", r.Rows, r.Wrong, r.Extra)
	for _, e := range r.Examples {
		fmt.Fprintf(&b, "  - %s\n", e)
	}
	return b.String()
}

func writeHistoryReport(dir string, rep *historyReport) error {
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

// historyCopyChunk is how many rows each COPY carries.
const historyCopyChunk = 1_000_000

func runHistory(ctx context.Context, o options) (*historyReport, error) {
	rep := &historyReport{Label: o.label, Started: time.Now().UTC(), Hardware: os.Getenv("BENCH_INSTANCE_TYPES"), Nodes: len(o.nodes)}
	if o.engine != "postgres" {
		return nil, fmt.Errorf("-history needs -engine postgres (it bulk-loads with COPY)")
	}
	r, err := loadRecipe(o.recipe)
	if err != nil {
		return nil, err
	}
	read, err := gosql.Open("pgx", o.readDSN)
	if err != nil {
		return nil, err
	}
	defer func() { _ = read.Close() }()
	if _, err := read.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS read"); err != nil {
		return nil, fmt.Errorf("read model: %w", err)
	}
	src, err := openSource(o, r.eventsDDL)
	if err != nil {
		return nil, err
	}
	src.close()

	// The history: one writer's streams (each stream's versions in order),
	// with the live workload's shapes — short orders and hot streams.
	w := &writer{rnd: mrand.New(mrand.NewPCG(1, 0x5eed)), hotTarget: o.hotEvents} //nolint:gosec // G404: workload shape, not security
	for range o.hotStreams {
		w.hot = append(w.hot, &stream{id: newUUID(), hot: true})
	}
	if len(w.hot) > 0 {
		w.hotShare = min(1, float64(len(w.hot)*o.hotEvents)/float64(o.history))
	}
	conn, err := pgx.Connect(ctx, o.srcDSN)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	start := time.Now()
	for done := int64(0); done < o.history; {
		n := min(historyCopyChunk, o.history-done)
		rows := &historyRows{w: w, left: n}
		if _, err := conn.CopyFrom(ctx, pgx.Identifier{"events"}, []string{"stream_id", "version", "type", "data"}, rows); err != nil {
			return rep, fmt.Errorf("copy events: %w", err)
		}
		done += n
		if done%(10*historyCopyChunk) == 0 || done == o.history {
			logf("loaded %d/%d events (%.0f/s)", done, o.history, float64(done)/time.Since(start).Seconds())
		}
	}
	rep.LoadSeconds = time.Since(start).Seconds()
	rep.Events = o.history
	rep.HotStreams = nil
	for _, s := range w.hot {
		rep.HotStreams = append(rep.HotStreams, s.want.version)
	}
	want := make(map[string]expected)
	for _, s := range w.streams() {
		if s.want.version > 0 {
			want[s.id] = s.want
		}
	}
	rep.Streams = len(want)

	// Snapshot the history into the log.
	c := newCluster(o.nodes)
	if err := c.waitReady(ctx, o.readyTimeout); err != nil {
		return rep, err
	}
	ingestable, err := r.ingestableFor(o)
	if err != nil {
		return rep, err
	}
	database, err := r.databaseFor(o)
	if err != nil {
		return rep, err
	}
	for _, p := range []struct{ path, body string }{
		{"/v1/type/order-events", r.typ},
		{"/v1/database/read", database},
		{"/v1/ingestable/order-events", ingestable},
	} {
		if err := c.postRetrying(ctx, p.path, p.body, time.Minute); err != nil {
			return rep, err
		}
	}
	snapStart := time.Now()
	logf("snapshotting %d events into the log", o.history)
	if err := c.waitStreaming(ctx, "order-events", o.rebuildLimit); err != nil {
		return rep, err
	}
	applied, err := c.waitApplySettled(ctx, o.rebuildLimit)
	if err != nil {
		return rep, err
	}
	rep.SnapshotSeconds = time.Since(snapStart).Seconds()
	rep.LogEntries = applied
	logf("snapshot applied on every node: %.1fs, %d log entries", rep.SnapshotSeconds, applied)

	// Build the projection over the history.
	buildStart := time.Now()
	if err := c.postRetrying(ctx, "/v1/syncable/orders", r.orders, time.Minute); err != nil {
		return rep, err
	}
	logf("building orders over %d events", o.history)
	done := false
	lastLog := time.Now()
	for deadline := buildStart.Add(o.rebuildLimit); time.Now().Before(deadline) && ctx.Err() == nil; {
		var n int
		_ = read.QueryRowContext(ctx, "SELECT count(*) FROM read.orders").Scan(&n)
		lag, known := c.maxSyncLag(ctx, "orders")
		if n == len(want) && known && lag == 0 {
			done = true
			break
		}
		if time.Since(lastLog) >= 30*time.Second {
			logf("  build %.0fs: %d/%d rows, lag %d", time.Since(buildStart).Seconds(), n, len(want), lag)
			lastLog = time.Now()
		}
		if err := sleep(ctx, 2*time.Second); err != nil {
			return rep, err
		}
	}
	rep.BuildSeconds = time.Since(buildStart).Seconds()
	rep.BuildTimeout = !done
	if done {
		rep.BuildRate = float64(o.history) / rep.BuildSeconds
	}
	logf("build: %.1fs", rep.BuildSeconds)

	got, err := readRows(ctx, read, "orders")
	if err != nil {
		return rep, err
	}
	rep.Rows = len(got)
	rep.Wrong, rep.Extra, rep.Examples = compare(want, got)
	rep.Pass = done && rep.Wrong == 0 && rep.Extra == 0
	return rep, nil
}

// historyRows feeds COPY the writer's next events, applying each to its
// stream's expected row as it goes.
type historyRows struct {
	w    *writer
	left int64
	e    event
}

func (h *historyRows) Next() bool {
	if h.left == 0 {
		return false
	}
	h.left--
	s, e := h.w.nextEvent()
	h.w.apply(s, e)
	h.e = e
	return true
}

func (h *historyRows) Values() ([]any, error) {
	return []any{h.e.stream, h.e.version, h.e.typ, []byte(h.e.data)}, nil
}

func (h *historyRows) Err() error { return nil }
