package sql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// CASE on every expr site, through the real TOML path (the liveSet
// lesson: every vocabulary word gets one): a table set entry, a reshape
// emit, a fold arm, the when expr arm, and a merge emit.
const caseExprTOML = `
[projection]
db         = "testdb"
table      = "t"
primaryKey = "id"

[[projection.columns]]
name = "id"
type = "VARCHAR(64)"

[[projection.columns]]
name = "band"
type = "VARCHAR(16)"

[[projection.columns]]
name = "done_hours"
type = "DECIMAL(12,2)"

[[projection.stages]]
name    = "visits"
from    = "visit-topic"
keyPath = "$.id"
when    = [ { expr = "case $.kind when 'visit' then true when 'call' then $.billable else false end" } ]
emit    = [ { field = "job",   from = "$.job" },
            { field = "hours", expr = "case when $.hours < 0 then null else $.hours end" },
            { field = "done",  expr = "$.status = 'done'" } ]

[[projection.stages]]
name    = "by-job"
from    = "visits"
keyPath = "$.job"
reduce  = "aggregate"
emit    = [ { field = "done_hours", sum = "case when $.done then $.hours else 0 end" } ]

[[projection.stages]]
name    = "jobs"
from    = "job-topic"
keyPath = "$.id"
emit    = [ { field = "hours", from = "$.hours" } ]

[[projection.stages]]
name  = "m"
merge = [ "jobs", { stage = "by-job", as = "agg" } ]
emit  = [ { field = "done_hours", expr = "case when $.agg.done_hours is null then 0 else $.agg.done_hours end" } ]

[[projection.sources]]
from = "m"
[[projection.sources.rules]]
set = [ { column = "done_hours", from = "$.done_hours" } ]

[[projection.sources]]
topic   = "job-topic"
keyPath = "$.id"
rowOwner = true
[[projection.sources.rules]]
set = [ { column = "band", expr = "case when $.hours < 8 then 'small' when $.hours < 40 then 'medium' else 'large' end" } ]
`

func TestCaseExprTOMLDecodesOnEverySite(t *testing.T) {
	v, err := cluster.ParseConfigBytes("toml", []byte(caseExprTOML))
	require.NoError(t, err)
	cfg, err := parseProjectionConfigFields(v, nil)
	require.NoError(t, err)
	cfg.applyDefaults()
	require.NoError(t, validateProjectionConfig(cfg))

	// A malformed CASE is a loud admission error on a table set entry…
	bad := func(from, to string) error {
		src := []byte(replaceOnce(t, caseExprTOML, from, to))
		v, err := cluster.ParseConfigBytes("toml", src)
		require.NoError(t, err)
		cfg, err := parseProjectionConfigFields(v, nil)
		if err == nil {
			cfg.applyDefaults()
			err = validateProjectionConfig(cfg)
		}
		return err
	}
	require.ErrorContains(t, bad(`else 'large' end" } ]`, `else 'large'" } ]`), "expected when, else, or end")
	// …on a fold arm (an undominated quotient in a result branch)…
	require.ErrorContains(t, bad(`then $.hours else 0 end`, `then $.hours / 2 else 0 end`), "round(...) or trunc(...)")
	// …and on a stage when (mixed literal result families).
	require.ErrorContains(t, bad(`else false end`, `else 0 end`), "mix literal types")
}

func replaceOnce(t *testing.T, s, from, to string) string {
	t.Helper()
	require.Contains(t, s, from, "fixture lacks the text to replace")
	return strings.Replace(s, from, to, 1)
}
