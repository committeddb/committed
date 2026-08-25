package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/stages"
	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// dryRunSampleLimit is how many output objects each stage keeps in the
// report — enough to see value shape (null columns, wrong paths),
// small enough to read.
const dryRunSampleLimit = 3

// DryRun implements cluster.DryRunner: fold the fed sample through this
// projection's REAL stage graph into a throwaway store, evaluate every
// source's and rule's matching observationally, and report counters,
// samples, and auto-generated findings. No admission, no raft, no SQL —
// the destination database is never touched, so a dry run works even
// when it is unreachable.
func (p *Projection) DryRun(ctx context.Context, feed cluster.DryRunFeed, opts cluster.DryRunOptions) (*cluster.DryRunReport, error) {
	start := time.Now()
	cfg := p.config
	g := stages.BuildGraph(cfg.Stages)

	dir, err := os.MkdirTemp("", "committed-dryrun-*")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	store, _, err := stagestore.Open(dir, "dryrun", stageFingerprint(cfg))
	if err != nil {
		return nil, err
	}
	defer func() { _ = store.Close() }()

	samples := map[string][]json.RawMessage{}
	liveDeltas := map[string]int64{}
	retractions := map[string]int64{}
	g.OnDelta = func(stage string, _ []byte, obj any, live bool) error {
		if !live {
			retractions[stage]++
			return nil
		}
		liveDeltas[stage]++
		if len(samples[stage]) < dryRunSampleLimit {
			if bs, err := json.Marshal(obj); err == nil {
				samples[stage] = append(samples[stage], bs)
			}
		}
		return nil
	}

	obs := newDryRunObserver(cfg)
	entries := 0
	err = feed(func(a *cluster.Actual) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries++
		obs.observe(a)
		return store.Update(func(stx *stagestore.Tx) error {
			return g.FoldActual(stx, a)
		})
	})
	if err != nil {
		return nil, err
	}

	flows := g.FlowCounts()
	rep := &cluster.DryRunReport{
		Entries:    entries,
		DurationMs: time.Since(start).Milliseconds(),
		Stages:     make(map[string]cluster.DryRunStage, len(flows)),
	}
	err = store.View(func(tx *stagestore.Tx) error {
		for name, fc := range flows {
			keys, err := tx.OutKeyCount(name)
			if err != nil {
				return err
			}
			rep.Stages[name] = cluster.DryRunStage{
				Inputs:         fc.Inputs,
				Fanned:         fc.Fanned,
				Keys:           keys,
				UnkeyedDeletes: fc.UnkeyedDeletes,
				LiveDeltas:     liveDeltas[name],
				Retractions:    retractions[name],
				Joins:          joinStats(fc.Joins),
				Samples:        samples[name],
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	rep.Sources, rep.Findings = obs.report(cfg, rep)
	return rep, nil
}

func joinStats(flows []stages.JoinFlow) []cluster.JoinStat {
	if len(flows) == 0 {
		return nil
	}
	out := make([]cluster.JoinStat, len(flows))
	for i, f := range flows {
		out[i] = cluster.JoinStat{Target: f.Target, Alias: f.Alias, Absent: f.Absent, Optional: f.Optional, Hits: f.Hits, Misses: f.Misses}
	}
	return out
}

// whenObs observes one when-clause list against one topic's events —
// per-clause match counts plus a value-family histogram for misses, so
// a clause that NEVER matched can say what the data actually looked
// like (the equals-"true"-against-a-boolean class).
type whenObs struct {
	label         string
	clauses       []WhenClause
	seen          int64
	matched       int64
	clauseMatched []int64
	families      []map[string]int64
}

func newWhenObs(label string, clauses []WhenClause) *whenObs {
	return &whenObs{
		label:         label,
		clauses:       clauses,
		clauseMatched: make([]int64, len(clauses)),
		families:      make([]map[string]int64, len(clauses)),
	}
}

func (o *whenObs) observe(data any) bool {
	o.seen++
	all := true
	for i, c := range o.clauses {
		ok, v := stages.ClauseEval(c, data, nil)
		if ok {
			o.clauseMatched[i]++
			continue
		}
		all = false
		if o.families[i] == nil {
			o.families[i] = map[string]int64{}
		}
		o.families[i][valueFamily(v)]++
	}
	if all {
		o.matched++
	}
	return all
}

// hints reports the never-matched clauses' family evidence.
func (o *whenObs) hints() []string {
	if o.seen == 0 || o.matched > 0 {
		return nil
	}
	var out []string
	for i, c := range o.clauses {
		if o.clauseMatched[i] > 0 {
			continue
		}
		out = append(out, fmt.Sprintf("%s: clause %s never matched; values seen: %s",
			o.label, clauseDesc(c), familyList(o.families[i])))
	}
	return out
}

func clauseDesc(c WhenClause) string {
	switch {
	case c.Expr != "":
		return fmt.Sprintf("expr=%q", c.Expr)
	case c.Null:
		return fmt.Sprintf("[%s] null=true", c.Path)
	case c.NotNull:
		return fmt.Sprintf("[%s] notNull=true", c.Path)
	case c.NotEquals != nil:
		return fmt.Sprintf("[%s] notEquals=%v (%s)", c.Path, c.NotEquals, valueFamily(c.NotEquals))
	case c.GreaterThan != nil:
		return fmt.Sprintf("[%s] greaterThan=%v", c.Path, c.GreaterThan)
	case c.LessThan != nil:
		return fmt.Sprintf("[%s] lessThan=%v", c.Path, c.LessThan)
	default:
		return fmt.Sprintf("[%s] equals=%v (%s)", c.Path, c.Equals, valueFamily(c.Equals))
	}
}

func valueFamily(v any) string {
	switch v.(type) {
	case nil:
		return "missing/null"
	case string:
		return "string"
	case bool:
		return "boolean"
	case json.Number, float64, float32, int, int32, int64, *big.Rat:
		return "number"
	case map[string]any:
		return "object"
	case []any:
		return "array"
	default:
		return fmt.Sprintf("%T", v)
	}
}

func familyList(m map[string]int64) string {
	if len(m) == 0 {
		return "(none)"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s×%d", k, m[k])
	}
	return strings.Join(parts, ", ")
}

// dryRunObserver evaluates source and rule matching (and topic-fed
// stage whens) observationally, per topic, alongside the real fold.
type dryRunObserver struct {
	// byTopic dispatches decoded row payloads to their observers.
	byTopic map[string][]func(data any)
	sources []*sourceObs
	// stageWhens carry hints for topic-fed stages' whens, attached to
	// findings (stage-fed stages' whens are visible through counters).
	stageWhens []*whenObs
}

type sourceObs struct {
	topic string
	from  string
	when  *whenObs
	rules []*whenObs
}

func newDryRunObserver(cfg *ProjectionConfig) *dryRunObserver {
	o := &dryRunObserver{byTopic: map[string][]func(data any){}}
	names := map[string]bool{}
	for i := range cfg.Stages {
		names[cfg.Stages[i].Name] = true
	}
	for i := range cfg.Stages {
		st := &cfg.Stages[i]
		if st.From == "" || names[st.From] || len(st.When) == 0 {
			continue // merge stages, stage-fed stages, or nothing to observe
		}
		w := newWhenObs(fmt.Sprintf("stage %q when", st.Name), st.When)
		o.stageWhens = append(o.stageWhens, w)
		o.byTopic[st.From] = append(o.byTopic[st.From], func(data any) { w.observe(data) })
	}
	for i := range cfg.Sources {
		src := &cfg.Sources[i]
		so := &sourceObs{topic: src.Topic, from: src.FromStage}
		label := fmt.Sprintf("source %d (topic %q)", i+1, src.Topic)
		so.when = newWhenObs(label+" when", src.When)
		for ri := range src.Rules {
			so.rules = append(so.rules, newWhenObs(fmt.Sprintf("%s rule %d when", label, ri+1), src.Rules[ri].When))
		}
		o.sources = append(o.sources, so)
		if src.Topic == "" {
			continue // stage-fed: observed through the stage's counters
		}
		o.byTopic[src.Topic] = append(o.byTopic[src.Topic], func(data any) {
			if so.when.observe(data) {
				for _, r := range so.rules {
					r.observe(data)
				}
			}
		})
	}
	return o
}

func (o *dryRunObserver) observe(a *cluster.Actual) {
	for _, e := range a.Entities {
		obs := o.byTopic[e.Type.ID]
		if len(obs) == 0 || e.Variant() != cluster.EntityVariantRow {
			continue
		}
		data, err := decodeStageObject(e.Data)
		if err != nil {
			continue // the fold path dead-letters this loudly; observation just skips
		}
		for _, fn := range obs {
			fn(data)
		}
	}
}

// report assembles the per-source rows and the findings — the
// silent-empty playbook, codified: every signature the field campaign
// learned to read by hand becomes a sentence with the next move in it.
func (o *dryRunObserver) report(cfg *ProjectionConfig, rep *cluster.DryRunReport) ([]cluster.DryRunSource, []string) {
	var findings []string
	names := make([]string, 0, len(rep.Stages))
	for name := range rep.Stages {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		st := rep.Stages[name]
		def := &cfg.Stages[stageNamed(cfg, name)]
		switch {
		case st.Inputs == 0:
			from := def.From
			if len(def.Merge) > 0 {
				continue // merges have no direct inputs; their sides report
			}
			findings = append(findings, fmt.Sprintf("stage %q folded ZERO inputs — the sampled windows may not cover its input's log region (from %q); target it with ?fromIndex", name, from))
		case def.ForEach != "" && st.Fanned == 0:
			findings = append(findings, fmt.Sprintf("stage %q received %d inputs but its forEach fanned ZERO elements — if the fan path crosses a serialized-JSON string column, decode it at ingest (jsonColumns)", name, st.Inputs))
		case st.Keys == 0 && st.Inputs > 0:
			findings = append(findings, fmt.Sprintf("stage %q received %d inputs but holds ZERO keys — its when/joins rejected everything", name, st.Inputs))
		}
		for _, j := range st.Joins {
			if !j.Absent && j.Hits == 0 && j.Misses > 0 {
				findings = append(findings, fmt.Sprintf("stage %q join on %q never resolved a dimension row (0 hits / %d attempts) — check the on path, the key space (normalize/keyType), and whether the dimension's region was sampled", name, j.Target, j.Misses))
			}
		}
		if st.UnkeyedDeletes > 0 {
			findings = append(findings, fmt.Sprintf("stage %q dropped %d delete-shaped input(s) that could not key — LOST retractions; the delete events' payloads lack the keyPath field or fail its keyType", name, st.UnkeyedDeletes))
		}
	}
	for _, w := range o.stageWhens {
		findings = append(findings, w.hints()...)
	}

	srcRows := make([]cluster.DryRunSource, 0, len(o.sources))
	for _, so := range o.sources {
		row := cluster.DryRunSource{Topic: so.topic, From: so.from, Seen: so.when.seen, Matched: so.when.matched}
		for ri, r := range so.rules {
			row.RuleMatches = append(row.RuleMatches, r.matched)
			row.Hints = append(row.Hints, r.hints()...)
			if r.seen > 0 && r.matched == 0 {
				findings = append(findings, fmt.Sprintf("source (topic %q) rule %d matched ZERO of %d events — see its hints for the value families actually seen", so.topic, ri+1, r.seen))
			}
		}
		row.Hints = append(row.Hints, so.when.hints()...)
		if so.topic != "" && so.when.seen > 0 && so.when.matched == 0 {
			findings = append(findings, fmt.Sprintf("source (topic %q) matched ZERO of %d events — see its hints for the value families actually seen", so.topic, so.when.seen))
		}
		srcRows = append(srcRows, row)
	}
	if len(findings) == 0 {
		findings = []string{"no silent-empty signatures detected in the sampled windows"}
	}
	return srcRows, findings
}
