package main

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

// Report is the run's result: report.json, and Summary() as summary.txt.
type Report struct {
	Label     string    `json:"label,omitempty"`
	Started   time.Time `json:"started"`
	Engine    string    `json:"engine"`
	Payload   string    `json:"payload"`
	Nodes     int       `json:"nodes"`
	Hardware  string    `json:"hardware,omitempty"`
	Rate      float64   `json:"offeredRate"`
	Duration  string    `json:"duration"`
	Writers   int       `json:"writers"`
	HotEvents int       `json:"hotEvents"`

	Appended        int64   `json:"appended"`
	AchievedRate    float64 `json:"achievedRate"`
	MinSecondRate   int64   `json:"minSecondRate"`
	AppendErrors    int64   `json:"appendErrors"`
	LastAppendError string  `json:"lastAppendError,omitempty"`
	HotStreamSizes  []int   `json:"hotStreamSizes"`

	Latency    latency  `json:"commitToRow"`
	PerMinute  []minute `json:"perMinute"`
	Unobserved int      `json:"unobservedSamples"`
	MaxSyncLag uint64   `json:"maxSyncLag"`
	Idle       latency  `json:"idle"`
	Rebuild    rebuild  `json:"rebuild"`

	CatchUpSeconds float64  `json:"catchUpSeconds"` // -1: never caught up
	Rows           int      `json:"rows"`
	WrongRows      int      `json:"wrongRows"`
	ExtraRows      int      `json:"extraRows"`
	WrongExamples  []string `json:"wrongExamples,omitempty"`

	Pass     bool     `json:"pass"`
	Failures []string `json:"failures,omitempty"`
}

type latency struct {
	Samples int `json:"samples"`
	P50     ms  `json:"p50"`
	P90     ms  `json:"p90"`
	P99     ms  `json:"p99"`
	P999    ms  `json:"p999"`
	Max     ms  `json:"max"`
}

type minute struct {
	Minute  int `json:"minute"`
	Samples int `json:"samples"`
	P50     ms  `json:"p50"`
	P99     ms  `json:"p99"`
}

type rebuild struct {
	Seconds      float64 `json:"seconds"` // -1: never matched
	EventsPerSec float64 `json:"eventsPerSec"`
	WrongRows    int     `json:"wrongRows"`
	ExtraRows    int     `json:"extraRows"`
}

// ms is a duration that marshals as milliseconds.
type ms time.Duration

func (d ms) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%.1f", float64(d)/float64(time.Millisecond))), nil
}
func (d ms) String() string { return time.Duration(d).Round(100 * time.Microsecond).String() }

func newReport(o options) *Report {
	return &Report{
		Label: o.label, Started: time.Now().UTC(), Engine: o.engine, Payload: o.payload,
		Nodes: len(o.nodes), Hardware: os.Getenv("BENCH_INSTANCE_TYPES"), Rate: o.rate,
		Duration: o.duration.String(), Writers: o.writers, HotEvents: o.hotEvents,
	}
}

func summarize(sorted []time.Duration) latency {
	return latency{
		Samples: len(sorted), P50: ms(percentile(sorted, .50)), P90: ms(percentile(sorted, .90)),
		P99: ms(percentile(sorted, .99)), P999: ms(percentile(sorted, .999)), Max: ms(percentile(sorted, 1)),
	}
}

func (r *Report) recordAppends(wl *workload, start, last time.Time) {
	r.Appended = wl.appended.Load()
	if el := last.Sub(start).Seconds(); el > 0 {
		r.AchievedRate = float64(r.Appended) / el
	}
	r.MinSecondRate = -1
	// The first and last seconds are partial.
	for i := 1; i < int(wl.o.duration.Seconds())-1 && i < len(wl.perSec); i++ {
		if r.MinSecondRate < 0 || wl.perSec[i] < r.MinSecondRate {
			r.MinSecondRate = wl.perSec[i]
		}
	}
	r.AppendErrors = wl.errors.Load()
	if e, ok := wl.lastErr.Load().(string); ok {
		r.LastAppendError = e
	}
	r.HotStreamSizes = wl.hotSizes()
}

func (r *Report) recordLatency(t *tracker) {
	all, byMinute, unobserved, _ := t.latencies()
	r.Latency, r.Unobserved = summarize(all), unobserved
	mins := make([]int, 0, len(byMinute))
	for m := range byMinute {
		mins = append(mins, m)
	}
	sort.Ints(mins)
	for _, m := range mins {
		v := byMinute[m]
		r.PerMinute = append(r.PerMinute, minute{Minute: m, Samples: len(v), P50: ms(percentile(v, .5)), P99: ms(percentile(v, .99))})
	}
}

func (r *Report) recordIdle(lat []time.Duration) {
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	r.Idle = summarize(lat)
}

func (r *Report) recordRebuild(t0, done time.Time, wrong, extra int, ex []string) {
	r.Rebuild = rebuild{Seconds: -1, WrongRows: wrong, ExtraRows: extra}
	if !done.IsZero() {
		r.Rebuild.Seconds = done.Sub(t0).Seconds()
		r.Rebuild.EventsPerSec = float64(r.Appended) / r.Rebuild.Seconds
	} else if len(ex) > 0 {
		r.WrongExamples = append(r.WrongExamples, ex...)
	}
}

// judge applies the bar: the source took the offered rate, every row is
// right, p99 commit-to-row is under the bar during the run and for idle
// appends, and the read model caught up within the catch-up bar.
func (r *Report) judge(o options) {
	fail := func(f string, a ...any) { r.Failures = append(r.Failures, fmt.Sprintf(f, a...)) }
	if r.AchievedRate < 0.98*o.rate {
		fail("the source took %.0f/s, under the offered %.0f/s: the run does not test the bar", r.AchievedRate, o.rate)
	}
	if r.CatchUpSeconds < 0 {
		fail("the read model never matched the source within %s", o.catchUpLimit)
	} else if r.CatchUpSeconds > o.catchUpBar.Seconds() {
		fail("caught up %.1fs after the last append, over the %s bar: the read model fell behind", r.CatchUpSeconds, o.catchUpBar)
	}
	if r.WrongRows > 0 || r.ExtraRows > 0 {
		fail("%d rows wrong or missing, %d unexpected", r.WrongRows, r.ExtraRows)
	}
	if r.Unobserved > 0 {
		fail("%d sampled events never observed", r.Unobserved)
	}
	if time.Duration(r.Latency.P99) > o.latencyBar {
		fail("p99 commit-to-row %s, over the %s bar", r.Latency.P99, o.latencyBar)
	}
	if r.Idle.Samples > 0 && time.Duration(r.Idle.P99) > o.latencyBar {
		fail("idle p99 commit-to-row %s, over the %s bar", r.Idle.P99, o.latencyBar)
	}
	if r.Rebuild.Seconds < 0 && !o.skipRebuild {
		fail("orders_v2 never matched within %s", o.rebuildLimit)
	}
	r.Pass = len(r.Failures) == 0
}

func (r *Report) Summary() string {
	var b strings.Builder
	p := func(f string, a ...any) { fmt.Fprintf(&b, f+"\n", a...) }
	verdict := "PASS"
	if !r.Pass {
		verdict = "FAIL"
	}
	p("EVENT-STORE WORKLOAD %s  %s", verdict, r.Label)
	p("  source %s (%s), %d nodes %s", r.Engine, r.Payload, r.Nodes, r.Hardware)
	p("  offered %.0f/s for %s, %d writers; achieved %.0f/s (slowest second %d), %d appended, %d errors",
		r.Rate, r.Duration, r.Writers, r.AchievedRate, r.MinSecondRate, r.Appended, r.AppendErrors)
	p("  hot streams reached %v events", r.HotStreamSizes)
	l := r.Latency
	p("  commit-to-row (%d samples): p50 %s  p90 %s  p99 %s  p99.9 %s  max %s", l.Samples, l.P50, l.P90, l.P99, l.P999, l.Max)
	pm := make([]string, 0, len(r.PerMinute))
	for _, m := range r.PerMinute {
		pm = append(pm, m.P99.String())
	}
	p("  p99 by minute: %s", strings.Join(pm, " "))
	p("  catch-up after the last append: %.1fs; rows %d, wrong %d, extra %d", r.CatchUpSeconds, r.Rows, r.WrongRows, r.ExtraRows)
	if r.Idle.Samples > 0 {
		p("  idle single appends (%d): p50 %s  p99 %s  max %s", r.Idle.Samples, r.Idle.P50, r.Idle.P99, r.Idle.Max)
	}
	if r.Rebuild.Seconds != 0 {
		p("  second read model from the log: %.1fs (%.0f events/s)", r.Rebuild.Seconds, r.Rebuild.EventsPerSec)
	}
	for _, f := range r.Failures {
		p("  - %s", f)
	}
	for _, e := range r.WrongExamples {
		p("    %s", e)
	}
	return b.String()
}
