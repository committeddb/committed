package main

import (
	"context"
	gosql "database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func run(ctx context.Context, o options) (*Report, error) {
	rep := newReport(o)
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
	defer src.close()
	if err := os.MkdirAll(o.results, 0o750); err != nil {
		return nil, err
	}

	c := newCluster(o.nodes)
	if err := c.waitReady(ctx, o.readyTimeout); err != nil {
		return nil, err
	}
	logf("cluster ready: %d nodes", len(o.nodes))
	ingestable, err := r.ingestableFor(o)
	if err != nil {
		return nil, err
	}
	database, err := r.databaseFor(o)
	if err != nil {
		return nil, err
	}
	for _, p := range []struct{ path, body string }{
		{"/v1/type/order-events", r.typ},
		{"/v1/ingestable/order-events", ingestable},
		{"/v1/database/read", database},
		{"/v1/syncable/orders", r.orders},
	} {
		if err := c.post(ctx, p.path, p.body); err != nil {
			return nil, err
		}
	}
	if err := c.waitStreaming(ctx, "order-events", o.readyTimeout); err != nil {
		return nil, err
	}
	logf("configs posted; ingestable streaming")

	// The append phase, measured.
	tr := newTracker(read, "orders")
	wl := newWorkload(o, src, tr)
	measureCtx, stopMeasure := context.WithCancel(ctx)
	defer stopMeasure()
	go tr.run(measureCtx, 25*time.Millisecond)
	smp := &sampler{
		c: c, path: filepath.Join(o.results, "timeseries.jsonl"), start: time.Now(),
		appends: wl.appended.Load, pending: tr.pendingCount,
	}
	go smp.run(measureCtx)
	logf("appending at %.0f/s for %s with %d writers", o.rate, o.duration, o.writers)
	start, lastCommit := wl.run(ctx)
	rep.recordAppends(wl, start, lastCommit)
	logf("appended %d events (%.0f/s); waiting for the read model", rep.Appended, rep.AchievedRate)

	// Catch-up and correctness.
	want := wl.expectedRows()
	m := waitMatch(ctx, read, "orders", want, o.catchUpLimit, o.stallLimit, 50*time.Millisecond)
	rep.recordCatchUp(m, lastCommit, len(want))
	_ = sleep(ctx, 500*time.Millisecond) // let the tracker see the last rows
	stopMeasure()
	rep.recordLatency(tr)
	rep.MaxSyncLag = smp.maxLag()
	logf("caught up in %.1fs; %d rows, %d wrong, %d extra", rep.CatchUpSeconds, rep.Rows, rep.WrongRows, rep.ExtraRows)

	// Single appends into a quiet system.
	if o.idleProbes > 0 {
		lat, err := idleProbes(ctx, o, src, read, want)
		if err != nil {
			return rep, err
		}
		rep.recordIdle(lat)
		logf("idle probes: p50 %s, max %s", rep.Idle.P50, rep.Idle.Max)
	}

	// A second read model built from the start of the log (recipe step 3).
	if !o.skipRebuild {
		body, err := r.projectionNamed("orders_v2")
		if err != nil {
			return rep, err
		}
		t0 := time.Now()
		if err := c.post(ctx, "/v1/syncable/orders_v2", body); err != nil {
			return rep, err
		}
		logf("building orders_v2 from the log")
		rep.recordRebuild(t0, waitMatch(ctx, read, "orders_v2", want, o.rebuildLimit, o.stallLimit, time.Second))
		logf("orders_v2: %.1fs", rep.Rebuild.Seconds)
	}
	rep.judge(o)
	return rep, nil
}

// idleProbes appends one order_placed event to a fresh stream at a time,
// with the system otherwise quiet, and times each to its row.
func idleProbes(ctx context.Context, o options, src *source, read *gosql.DB, want map[string]expected) ([]time.Duration, error) {
	if err := sleep(ctx, 5*time.Second); err != nil { // let idle workers back off
		return nil, err
	}
	var out []time.Duration
	for i := range o.idleProbes {
		id := newUUID()
		e := event{stream: id, version: 1, typ: "order_placed", customer: "ada", data: `{"customer": "ada"}`}
		if err := src.append(ctx, e); err != nil {
			return out, fmt.Errorf("idle probe: %w", err)
		}
		at := time.Now()
		want[id] = expected{customer: "ada", status: "order_placed", version: 1}
		deadline := at.Add(30 * time.Second)
		for time.Now().Before(deadline) {
			if got, err := readVersions(ctx, read, "orders", []string{id}); err == nil && got[id] >= 1 {
				out = append(out, time.Since(at))
				break
			}
			if err := sleep(ctx, 5*time.Millisecond); err != nil {
				return out, err
			}
		}
		// Spread the probes over the poll backoff's phases.
		if err := sleep(ctx, 2*time.Second+time.Duration(i%4)*700*time.Millisecond); err != nil {
			return out, err
		}
	}
	return out, nil
}

func logf(format string, args ...any) {
	fmt.Printf("%s  %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...))
}
