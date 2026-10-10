package sql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// Aggregate expressions and having through the real TOML path — the
// ticket's acceptance stage: margin, an avg, a distinct count, a constant,
// and a having, in one stage.
const aggregateExprTOML = `
[projection]
db         = "testdb"
table      = "jobs"
primaryKey = "job"

[[projection.columns]]
name = "job"
type = "VARCHAR(64)"

[[projection.columns]]
name = "margin"
type = "DECIMAL(12,4)"

[[projection.stages]]
name    = "by-job"
from    = "visits"
keyPath = "$.job"
reduce  = "aggregate"
emit    = [ { field = "margin",   expr = "round((sum($.rev) - sum($.cost)) / nullif(sum($.rev), 0), 4)" },
            { field = "avg_rate", expr = "round(avg($.rate), 2)" },
            { field = "techs",    expr = "count(distinct $.tech_id)" },
            { field = "done",     expr = "count(*) filter (where $.status = 'done')" },
            { field = "source",   expr = "'teamup'" },
            { field = "hours",    sum  = "$.hours" } ]
having  = "sum($.hours) > 0"

[[projection.sources]]
from = "by-job"
[[projection.sources.rules]]
set = [ { column = "margin", from = "$.margin" } ]
`

func TestAggregateExpressionTOML(t *testing.T) {
	parse := func(src string) (*ProjectionConfig, error) {
		v, err := cluster.ParseConfigBytes("toml", []byte(src))
		require.NoError(t, err)
		cfg, err := parseProjectionConfigFields(v, nil)
		if err == nil {
			cfg.applyDefaults()
			err = validateProjectionConfig(cfg)
		}
		return cfg, err
	}
	cfg, err := parse(aggregateExprTOML)
	require.NoError(t, err)
	require.Equal(t, "sum($.hours) > 0", cfg.Stages[0].Having)

	bad := func(from, to string) error {
		require.Contains(t, aggregateExprTOML, from, "fixture lacks the text to replace")
		_, err := parse(strings.Replace(aggregateExprTOML, from, to, 1))
		return err
	}
	require.ErrorContains(t, bad(`round(avg($.rate), 2)`, `avg($.rate)`), "avg(...) is an exact quotient")
	require.ErrorContains(t, bad(`having  = "sum($.hours) > 0"`, `having  = "$.hours > 0"`), "outside an aggregate")
	require.ErrorContains(t, bad(`reduce  = "aggregate"`, `reduce  = "latest"`), "")

	// The having is declared content: editing it is a different computation.
	edited, err := parse(strings.Replace(aggregateExprTOML, `"sum($.hours) > 0"`, `"sum($.hours) > 1"`, 1))
	require.NoError(t, err)
	require.NotEqual(t, cfg.projectionShapeFingerprint(), edited.projectionShapeFingerprint())
}
