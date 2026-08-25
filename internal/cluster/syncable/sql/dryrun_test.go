package sql

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

func dryRunFixture(t *testing.T, toml string) *Projection {
	t.Helper()
	v, err := cluster.ParseConfigBytes("toml", []byte(toml))
	require.NoError(t, err)
	cfg, err := parseProjectionConfigFields(v, nil)
	require.NoError(t, err)
	cfg.applyDefaults()
	require.NoError(t, validateProjectionConfig(cfg))
	return &Projection{config: cfg}
}

func feedOf(actuals ...*cluster.Actual) cluster.DryRunFeed {
	return func(yield func(*cluster.Actual) error) error {
		for _, a := range actuals {
			if err := yield(a); err != nil {
				return err
			}
		}
		return nil
	}
}

func rowActual(index uint64, topic, key, payload string) *cluster.Actual {
	return &cluster.Actual{
		Index:    index,
		Entities: []*cluster.Entity{cluster.NewUpsertEntity(&cluster.Type{ID: topic}, []byte(key), []byte(payload))},
	}
}

// The dry-run catches the campaign's silent-empty classes on a sample,
// in one report: the never-matching typed when (equals="true" against a
// boolean) with its value-family hint, the join that never resolves,
// and healthy stages with countable keys and readable samples.
func TestDryRunReportAndFindings(t *testing.T) {
	p := dryRunFixture(t, `
[projection]
db         = "testdb"
table      = "t"
primaryKey = "id"

[[projection.columns]]
name = "id"
type = "VARCHAR(64)"

[[projection.columns]]
name = "v"
type = "VARCHAR(64)"

[[projection.columns]]
name = "amount"
type = "VARCHAR(64)"

[[projection.stage]]
name    = "live"
from    = "txns"
keyPath = "$.id"
emit    = [ { field = "job", from = "$.jobId" } ]

[[projection.stage]]
name    = "joined"
from    = "visits"
keyPath = "$.id"
emit    = [ { field = "id", from = "$.id" } ]

[[projection.stage.join]]
topic = "workareas"
on    = "$.waId"

[[projection.source]]
from = "live"
[[projection.source.rules]]
set = [ { column = "v", from = "$.job" } ]
`)

	rep, err := p.DryRun(context.Background(), feedOf(
		rowActual(1, "txns", "t1", `{"id":"t1","jobId":"j1"}`),
		rowActual(2, "txns", "t2", `{"id":"t2","jobId":"j2"}`),
		// visits reference a workarea dimension that never arrives.
		rowActual(3, "visits", "v1", `{"id":"v1","waId":"w9"}`),
	), cluster.DryRunOptions{})
	require.NoError(t, err)

	require.Equal(t, 3, rep.Entries)

	// The healthy stage: counted, keyed, sampled.
	live := rep.Stages["live"]
	require.Equal(t, int64(2), live.Inputs)
	require.Equal(t, 2, live.Keys)
	require.NotEmpty(t, live.Samples, "value-shaped bugs are invisible in counts — samples are the point")
	require.Contains(t, string(live.Samples[0]), `"job"`)

	// The join that never resolved: counted per join and named in findings.
	joined := rep.Stages["joined"]
	require.Len(t, joined.Joins, 1)
	require.Equal(t, int64(0), joined.Joins[0].Hits)
	require.Equal(t, int64(1), joined.Joins[0].Misses)
	requireFinding(t, rep.Findings, "never resolved a dimension row")
}

// The typed-when class: a rule whose string literal faces boolean
// values matches zero events, and the hint names the families actually
// seen — the field's exact bug, diagnosed from a sample instead of a
// finished replay's diff.
func TestDryRunTypedWhenHint(t *testing.T) {
	p := dryRunFixture(t, `
[projection]
db         = "testdb"
table      = "t"
primaryKey = "id"

[[projection.columns]]
name = "id"
type = "VARCHAR(64)"

[[projection.columns]]
name = "amount"
type = "VARCHAR(64)"

[[projection.source]]
topic   = "billing"
keyPath = "$.id"
[[projection.source.rules]]
when = [ { path = "$.billed", equals = "true" } ]
set  = [ { column = "amount", from = "$.amount" } ]
`)
	rep, err := p.DryRun(context.Background(), feedOf(
		rowActual(1, "billing", "b1", `{"id":"b1","billed":true,"amount":5}`),
		rowActual(2, "billing", "b2", `{"id":"b2","billed":false,"amount":7}`),
	), cluster.DryRunOptions{})
	require.NoError(t, err)

	require.Len(t, rep.Sources, 1)
	billing := rep.Sources[0]
	require.Equal(t, int64(2), billing.Seen)
	require.Equal(t, int64(2), billing.Matched, "the source-level when is empty — rules carry the filter")
	require.Len(t, billing.RuleMatches, 1)
	require.Equal(t, int64(0), billing.RuleMatches[0])
	require.NotEmpty(t, billing.Hints)
	require.Contains(t, strings.Join(billing.Hints, "\n"), "boolean×2",
		"the hint must say the values were boolean while the literal was a string")
	requireFinding(t, rep.Findings, "matched ZERO")
}

// A topic whose region the sample never covered reports zero inputs
// with the interpretation, not a bare zero.
func TestDryRunZeroInputFinding(t *testing.T) {
	p := dryRunFixture(t, `
[projection]
db         = "testdb"
table      = "t"
primaryKey = "id"

[[projection.columns]]
name = "id"
type = "VARCHAR(64)"

[[projection.columns]]
name = "v"
type = "VARCHAR(64)"

[[projection.stage]]
name    = "live"
from    = "txns"
keyPath = "$.id"
emit    = [ { field = "v", from = "$.id" } ]

[[projection.source]]
from = "live"
[[projection.source.rules]]
set = [ { column = "v", from = "$.v" } ]
`)
	rep, err := p.DryRun(context.Background(), feedOf(
		rowActual(1, "unrelated", "x", `{"id":"x"}`),
	), cluster.DryRunOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(0), rep.Stages["live"].Inputs)
	requireFinding(t, rep.Findings, "folded zero inputs")
	requireFinding(t, rep.Findings, "fromIndex")
	requireFinding(t, rep.Findings, "coverage:")
}

// One undecodable entity dead-letters and the rehearsal CONTINUES —
// mirroring the live worker (aborting on entry N of a bounded sample
// surfaced in the field as generic mid-size-run failures).
func TestDryRunDeadLettersAndContinues(t *testing.T) {
	p := dryRunFixture(t, `
[projection]
db         = "testdb"
table      = "t"
primaryKey = "id"

[[projection.columns]]
name = "id"
type = "VARCHAR(64)"

[[projection.columns]]
name = "v"
type = "VARCHAR(64)"

[[projection.stage]]
name    = "live"
from    = "txns"
keyPath = "$.id"
emit    = [ { field = "v", from = "$.id" } ]

[[projection.source]]
from = "live"
[[projection.source.rules]]
set = [ { column = "v", from = "$.v" } ]
`)
	rep, err := p.DryRun(context.Background(), feedOf(
		rowActual(1, "txns", "a", `{"id":"a"}`),
		rowActual(2, "txns", "bad", `{not json`),
		rowActual(3, "txns", "c", `{"id":"c"}`),
	), cluster.DryRunOptions{})
	require.NoError(t, err, "a permanent per-entry failure must not abort the rehearsal")
	require.Equal(t, 3, rep.Entries)
	require.Equal(t, 1, rep.DeadLetters)
	require.Equal(t, 2, rep.Stages["live"].Keys, "the healthy entries still folded")
	requireFinding(t, rep.Findings, "dead-lettered")
}

func requireFinding(t *testing.T, findings []string, substr string) {
	t.Helper()
	for _, f := range findings {
		if strings.Contains(f, substr) {
			return
		}
	}
	t.Fatalf("no finding contains %q; findings: %v", substr, findings)
}
