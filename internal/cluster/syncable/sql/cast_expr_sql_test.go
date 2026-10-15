package sql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// Casts, math, and between on every expr site, through the real TOML
// path — including the ticket's acceptance columns: a clamp over a
// stringified number and the pilot's array_agg(DISTINCT Status::int).
const castExprTOML = `
[projection]
db         = "testdb"
table      = "t"
primaryKey = "id"

[[projection.columns]]
name = "id"
type = "VARCHAR(64)"

[[projection.columns]]
name = "qty"
type = "DECIMAL(12,2)"

[[projection.columns]]
name = "statuses"
type = "JSONB"

[[projection.stages]]
name    = "tasks"
from    = "task-topic"
keyPath = "$.id"
when    = [ { expr = "try_cast($.Status as integer) between 0 and 9" } ]
emit    = [ { field = "job",    from = "$.job" },
            { field = "status", expr = "$.Status::integer" } ]

[[projection.stages]]
name    = "by-job"
from    = "tasks"
keyPath = "$.job"
reduce  = "aggregate"
emit    = [ { field = "statuses", collect = "$.status", distinct = true },
            { field = "hi",       max = "greatest($.status, 0)" } ]

[[projection.stages]]
name    = "jobs"
from    = "job-topic"
keyPath = "$.id"
emit    = [ { field = "qty", from = "$.qty" } ]

[[projection.stages]]
name  = "m"
merge = [ "jobs", { stage = "by-job", as = "agg" } ]
emit  = [ { field = "statuses", from = "$.agg.statuses" },
          { field = "half",     expr = "mod(coalesce($.agg.hi, 0), 2) = 0" } ]

[[projection.sources]]
from = "m"
[[projection.sources.rules]]
set = [ { column = "statuses", from = "$.statuses" } ]

[[projection.sources]]
topic    = "job-topic"
keyPath  = "$.id"
rowOwner = true
[[projection.sources.rules]]
set = [ { column = "qty", expr = "greatest(cast($.qty as numeric), 0)" } ]
`

func TestCastExprTOMLDecodesOnEverySite(t *testing.T) {
	parse := func(src string) error {
		v, err := cluster.ParseConfigBytes("toml", []byte(src))
		require.NoError(t, err)
		cfg, err := parseProjectionConfigFields(v, nil)
		if err == nil {
			cfg.applyDefaults()
			err = validateProjectionConfig(cfg)
		}
		return err
	}
	require.NoError(t, parse(castExprTOML))

	bad := func(from, to string) error {
		require.Contains(t, castExprTOML, from, "fixture lacks the text to replace")
		return parse(strings.Replace(castExprTOML, from, to, 1))
	}
	require.ErrorContains(t, bad(`$.Status::integer`, `$.Status::float`), "unknown cast type")
	require.ErrorContains(t, bad(`between 0 and 9`, `between 0 9`), "expected and in between")
	require.ErrorContains(t, bad(`cast($.qty as numeric), 0)`, `cast($.qty / 3 as numeric), 0)`), "round(...) or trunc(...)")
}
