package main

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	mrand "math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"slices"
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
// and checks every row. With -history-snapshot-only it stops once the
// history is in the log, deletes the ingestable so a backup of the log
// restores without the source, and writes dataset.json: the history is a
// pure function of the parameters it records (seeded event shapes and stream
// ids), so a later run over a restored log can regenerate the expected rows.
// With -history-scrub N, a run over a restored log builds nothing: it erases
// N subjects with right-to-be-forgotten deletes and times the scrub. Each
// event is its own entity, keyed by its seq_id, which COPY assigned in
// history order (1..N), so the regenerated history names every event of a
// chosen stream without the source.

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

	RestoredFrom  string    `json:"restoredFrom,omitempty"`     // -history-from-log: the dataset the cluster was restored from
	SavedEntries  uint64    `json:"savedLogEntries,omitempty"`  // the saved log's entries, which the restored log must reach
	SnapshotOnly  bool      `json:"snapshotOnly,omitempty"`     // -history-snapshot-only: no build
	ScrubSubjects int       `json:"scrubSubjects,omitempty"`    // -history-scrub: streams erased
	ScrubDeletes  int       `json:"scrubDeletes,omitempty"`     // their events, one delete each
	DeleteSeconds float64   `json:"deleteSeconds,omitempty"`    // deletes proposed → applied on every node
	ScrubSeconds  []float64 `json:"scrubSeconds,omitempty"`     // scrub requested → each node (in -nodes order) completed it
	ScrubTimeout  bool      `json:"scrubTimeout,omitempty"`     // a node did not complete within -rebuild-limit
	ScrubWrites   *latency  `json:"scrubWrites,omitempty"`      // probe writes to the first node until every node scrubbed, proposed → applied there
	ScrubWriteErr int       `json:"scrubWriteErrors,omitempty"` // probe writes that failed or timed out (10s)
	BuildSeconds  float64   `json:"buildSeconds"`               // projection posted → every row present, lag 0
	BuildRate     float64   `json:"buildEventsPerSec"`
	BuildTimeout  bool      `json:"buildTimeout,omitempty"`

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
	if r.RestoredFrom != "" {
		fmt.Fprintf(&b, "  restored log: %d entries (the saved log held %d), from %s\n", r.LogEntries, r.SavedEntries, r.RestoredFrom)
	} else {
		fmt.Fprintf(&b, "  load into the source (COPY): %.1fs\n", r.LoadSeconds)
		fmt.Fprintf(&b, "  snapshot into the log: %.1fs (%.0f events/sec), %d log entries\n",
			r.SnapshotSeconds, float64(r.Events)/max(r.SnapshotSeconds, 1e-9), r.LogEntries)
	}
	if r.ScrubSubjects > 0 {
		fmt.Fprintf(&b, "  erased %d subjects: %d deletes applied on every node in %.1fs\n", r.ScrubSubjects, r.ScrubDeletes, r.DeleteSeconds)
		if r.ScrubTimeout {
			fmt.Fprintf(&b, "  scrub: not completed on every node within the limit; per node %v s\n", r.ScrubSeconds)
		} else {
			fmt.Fprintf(&b, "  scrub: completed on every node; per node %v s\n", r.ScrubSeconds)
		}
		if w := r.ScrubWrites; w != nil {
			fmt.Fprintf(&b, "  writes to the first node during the scrub: %d applied (p50 %s  p99 %s  max %s), %d failed\n",
				w.Samples, w.P50, w.P99, w.Max, r.ScrubWriteErr)
		}
		return b.String()
	}
	switch {
	case r.SnapshotOnly && r.RestoredFrom != "":
		fmt.Fprintf(&b, "  projection build: not run (-history-snapshot-only)\n")
		return b.String()
	case r.SnapshotOnly:
		fmt.Fprintf(&b, "  projection build: not run (-history-snapshot-only); ingestable deleted, %d log entries\n", r.LogEntries)
		return b.String()
	case r.BuildTimeout:
		fmt.Fprintf(&b, "  projection build: did not finish within the limit (%.1fs)\n", r.BuildSeconds)
	default:
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

// dataset describes a history in a log, for a later run over a restore of
// it: the parameters that regenerate the history exactly, and what the log
// held once it was in.
type dataset struct {
	Created    time.Time `json:"created"`
	Label      string    `json:"label,omitempty"`
	Events     int64     `json:"events"`
	HotStreams int       `json:"hotStreams"`
	HotEvents  int       `json:"hotEvents"`
	Streams    int       `json:"streams"`
	LogEntries uint64    `json:"logEntries"`
}

func readDataset(path string) (dataset, error) {
	var ds dataset
	b, err := os.ReadFile(path) //nolint:gosec // G304: the operator names the dataset
	if err != nil {
		return ds, err
	}
	if err := json.Unmarshal(b, &ds); err != nil {
		return ds, fmt.Errorf("%s: %w", path, err)
	}
	if ds.Events <= 0 || ds.LogEntries == 0 {
		return ds, fmt.Errorf("%s: no events or log entries recorded", path)
	}
	return ds, nil
}

func writeDataset(dir string, o options, rep *historyReport) error {
	b, err := json.MarshalIndent(dataset{
		Created: time.Now().UTC(), Label: o.label, Events: o.history, HotStreams: o.hotStreams,
		HotEvents: o.hotEvents, Streams: rep.Streams, LogEntries: rep.LogEntries,
	}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "dataset.json"), b, 0o600)
}

// historyCopyChunk is how many rows each COPY carries.
const historyCopyChunk = 1_000_000

func runHistory(ctx context.Context, o options) (*historyReport, error) {
	rep := &historyReport{Label: o.label, Started: time.Now().UTC(), Hardware: os.Getenv("BENCH_INSTANCE_TYPES"), Nodes: len(o.nodes)}
	if o.engine != "postgres" {
		return nil, fmt.Errorf("-history needs -engine postgres (it bulk-loads with COPY)")
	}
	var ds dataset
	if o.fromLog != "" {
		var err error
		if ds, err = readDataset(o.fromLog); err != nil {
			return nil, err
		}
		o.history, o.hotStreams, o.hotEvents = ds.Events, ds.HotStreams, ds.HotEvents
		rep.RestoredFrom, rep.SavedEntries = o.fromLog, ds.LogEntries
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
	// The history: one writer's streams (each stream's versions in order),
	// with the live workload's shapes — short orders and hot streams.
	w := &writer{
		rnd:       mrand.New(mrand.NewPCG(1, 0x5eed)), //nolint:gosec // G404: workload shape, not security
		ids:       mrand.New(mrand.NewPCG(2, 0x5eed)), //nolint:gosec // G404: reproducible ids, not security
		hotTarget: o.hotEvents,
	}
	for range o.hotStreams {
		w.hot = append(w.hot, &stream{id: w.newID(), hot: true})
	}
	if len(w.hot) > 0 {
		w.hotShare = min(1, float64(len(w.hot)*o.hotEvents)/float64(o.history))
	}
	start := time.Now()
	var erase []int64
	if o.fromLog != "" {
		// The log already holds the history: only the expected rows are needed,
		// and with -history-scrub the seq_ids of the chosen streams' events.
		chosen := map[*stream]bool{}
		stride := max(1, ds.Streams/max(o.scrubSubjects, 1))
		started := 0
		for seq := int64(1); seq <= o.history; seq++ {
			s, e := w.nextEvent()
			w.apply(s, e)
			if o.scrubSubjects == 0 || s.hot {
				continue
			}
			if e.version == 1 {
				if started%stride == 0 && len(chosen) < o.scrubSubjects {
					chosen[s] = true
				}
				started++
			}
			if chosen[s] {
				erase = append(erase, seq)
			}
		}
		rep.ScrubSubjects = len(chosen)
		logf("regenerated the %d-event history from %s in %.1fs", o.history, o.fromLog, time.Since(start).Seconds())
	} else if err := copyHistory(ctx, o, r, w); err != nil {
		return rep, err
	} else {
		rep.LoadSeconds = time.Since(start).Seconds()
	}
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

	c := newCluster(o.nodes)
	if err := c.waitReady(ctx, o.readyTimeout); err != nil {
		return rep, err
	}
	if o.fromLog != "" {
		return buildRestored(ctx, o, r, c, read, want, erase, rep)
	}

	// Snapshot the history into the log.
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

	if o.snapshotOnly {
		if err := c.delete(ctx, "/v1/ingestable/order-events"); err != nil {
			return rep, err
		}
		applied, err := c.waitApplySettled(ctx, o.rebuildLimit)
		if err != nil {
			return rep, err
		}
		rep.LogEntries = applied
		rep.SnapshotOnly = true
		rep.Pass = true
		logf("ingestable deleted; %d log entries", applied)
		return rep, writeDataset(o.results, o, rep)
	}

	return rep, buildHistory(ctx, o, r, c, read, want, rep)
}

// copyHistory creates the source's events table and fills it with the
// writer's history.
func copyHistory(ctx context.Context, o options, r *recipe, w *writer) error {
	src, err := openSource(o, r.eventsDDL)
	if err != nil {
		return err
	}
	src.close()
	conn, err := pgx.Connect(ctx, o.srcDSN)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	start := time.Now()
	for done := int64(0); done < o.history; {
		n := min(historyCopyChunk, o.history-done)
		rows := &historyRows{w: w, left: n}
		if _, err := conn.CopyFrom(ctx, pgx.Identifier{"events"}, []string{"stream_id", "version", "type", "data"}, rows); err != nil {
			return fmt.Errorf("copy events: %w", err)
		}
		done += n
		if done%(10*historyCopyChunk) == 0 || done == o.history {
			logf("loaded %d/%d events (%.0f/s)", done, o.history, float64(done)/time.Since(start).Seconds())
		}
	}
	return nil
}

// buildRestored checks that a cluster restored from a saved history's log
// holds all of it, points the read-model database config at this run's
// read model (the saved one names another machine), and builds.
func buildRestored(ctx context.Context, o options, r *recipe, c *cluster, read *gosql.DB, want map[string]expected, erase []int64, rep *historyReport) (*historyReport, error) {
	applied, err := c.waitApplySettled(ctx, o.rebuildLimit)
	if err != nil {
		return rep, err
	}
	rep.LogEntries = applied
	if applied < rep.SavedEntries {
		return rep, fmt.Errorf("the restored log holds %d entries; the saved one held %d", applied, rep.SavedEntries)
	}
	logf("restored log: %d entries on every node (the saved log held %d)", applied, rep.SavedEntries)
	if o.snapshotOnly {
		rep.SnapshotOnly = true
		rep.Pass = true
		return rep, nil
	}
	if o.scrubSubjects > 0 {
		return rep, scrubHistory(ctx, o, c, erase, rep)
	}
	database, err := r.databaseFor(o)
	if err != nil {
		return rep, err
	}
	if err := c.postRetrying(ctx, "/v1/database/read", database, time.Minute); err != nil {
		return rep, err
	}
	return rep, buildHistory(ctx, o, r, c, read, want, rep)
}

// scrubDeleteBatch is how many deletes each proposal carries.
const scrubDeleteBatch = 500

// scrubHistory proposes a right-to-be-forgotten delete for every event in
// erase, waits for every node to apply them, requests a scrub, and times
// each node until its status shows the scrub completed.
func scrubHistory(ctx context.Context, o options, c *cluster, erase []int64, rep *historyReport) error {
	rep.ScrubDeletes = len(erase)
	logf("erasing %d subjects: %d events", rep.ScrubSubjects, len(erase))
	start := time.Now()
	for i := 0; i < len(erase); i += scrubDeleteBatch {
		var b strings.Builder
		b.WriteString(`{"entities":[`)
		for j, seq := range erase[i:min(i+scrubDeleteBatch, len(erase))] {
			if j > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `{"typeId":"order-events","key":"%d","delete":true}`, seq)
		}
		b.WriteString(`]}`)
		if err := c.propose(ctx, b.String()); err != nil {
			return err
		}
	}
	if _, err := c.waitApplySettled(ctx, o.rebuildLimit); err != nil {
		return err
	}
	rep.DeleteSeconds = time.Since(start).Seconds()
	logf("deletes applied on every node: %.1fs", rep.DeleteSeconds)

	before := make([]uint64, len(c.nodes))
	for i, n := range c.nodes {
		st, err := c.scrubStatus(ctx, n)
		if err != nil {
			return err
		}
		before[i] = st.CompletedBound
	}
	if err := c.postRetrying(ctx, "/v1/type/"+scrubProbeType, fmt.Sprintf("[type]\nname = %q\n", scrubProbeType), time.Minute); err != nil {
		return err
	}
	probeCtx, stopProbe := context.WithCancel(ctx)
	defer stopProbe()
	probed := make(chan []time.Duration, 1)
	start = time.Now()
	if err := c.post(ctx, "/v1/scrub", ""); err != nil {
		return err
	}
	go func() { probed <- probeWrites(probeCtx, c, &rep.ScrubWriteErr) }()
	logf("scrub requested")
	rep.ScrubSeconds = make([]float64, len(c.nodes))
	left := len(c.nodes)
	lastLog := time.Now()
	for deadline := start.Add(o.rebuildLimit); left > 0 && time.Now().Before(deadline); {
		for i, n := range c.nodes {
			if rep.ScrubSeconds[i] > 0 {
				continue
			}
			st, err := c.scrubStatus(ctx, n)
			if err == nil && st.CompletedBound > before[i] && st.CompletedBound >= st.PendingBound {
				rep.ScrubSeconds[i] = time.Since(start).Seconds()
				left--
				logf("  scrub completed on %s: %.1fs (bound %d)", n, rep.ScrubSeconds[i], st.CompletedBound)
			}
		}
		if time.Since(lastLog) >= 30*time.Second {
			logf("  scrub %.0fs: %d of %d nodes done", time.Since(start).Seconds(), len(c.nodes)-left, len(c.nodes))
			lastLog = time.Now()
		}
		if err := sleep(ctx, time.Second); err != nil {
			return err
		}
	}
	stopProbe()
	writes := <-probed
	slices.Sort(writes)
	w := summarize(writes)
	rep.ScrubWrites = &w
	logf("writes during the scrub: %d applied (p50 %s  p99 %s  max %s), %d failed", w.Samples, w.P50, w.P99, w.Max, rep.ScrubWriteErr)
	rep.ScrubTimeout = left > 0
	rep.Pass = !rep.ScrubTimeout && rep.ScrubWriteErr == 0
	return nil
}

// scrubProbeType holds the probe writes, apart from the projection's input.
const scrubProbeType = "scrub-probe"

// probeWrites proposes one small write to the first node every 100ms until
// ctx ends, and returns how long each took to apply there. The first node is
// written directly, not through propose's fallback, so a node whose apply
// stalls shows as failures rather than being routed around. errs counts them.
func probeWrites(ctx context.Context, c *cluster, errs *int) []time.Duration {
	var took []time.Duration
	for n := 0; ctx.Err() == nil; n++ {
		begun := time.Now()
		body := fmt.Sprintf(`{"entities":[{"typeId":%q,"key":"%d","data":{"n":%d}}]}`, scrubProbeType, n, n)
		code, _, err := c.doAs(ctx, http.MethodPost, c.nodes[0]+"/v1/proposal", "application/json", body)
		switch {
		case ctx.Err() != nil:
		case err != nil || code/100 != 2:
			*errs++
		default:
			took = append(took, time.Since(begun))
		}
		_ = sleep(ctx, 100*time.Millisecond)
	}
	return took
}

// buildHistory posts the projection and times it from posting to a
// complete read model, then checks every row.
func buildHistory(ctx context.Context, o options, r *recipe, c *cluster, read *gosql.DB, want map[string]expected, rep *historyReport) error {
	buildStart := time.Now()
	if err := c.postRetrying(ctx, "/v1/syncable/orders", r.orders, time.Minute); err != nil {
		return err
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
			return err
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
		return err
	}
	rep.Rows = len(got)
	rep.Wrong, rep.Extra, rep.Examples = compare(want, got)
	rep.Pass = done && rep.Wrong == 0 && rep.Extra == 0
	return nil
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
