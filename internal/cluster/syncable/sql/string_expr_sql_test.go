package sql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// The string functions and like on every expr site, through the real TOML
// path: a table set entry, a reshape emit, a fold arm, the when expr arm,
// and a merge emit.
const stringExprTOML = `
[projection]
db         = "testdb"
table      = "t"
primaryKey = "id"

[[projection.columns]]
name = "id"
type = "VARCHAR(64)"

[[projection.columns]]
name = "display_name"
type = "VARCHAR(256)"

[[projection.columns]]
name = "label"
type = "VARCHAR(256)"

[[projection.stages]]
name    = "lines"
from    = "line-topic"
keyPath = "$.id"
when    = [ { expr = "$.sku like 'SVC-%' and $.kind not in ('void', 'draft')" } ]
emit    = [ { field = "job",  from = "$.job" },
            { field = "code", expr = "upper(trim($.code))" } ]

[[projection.stages]]
name    = "codes"
from    = "lines"
keyPath = "$.job"
reduce  = "aggregate"
emit    = [ { field = "codes", collect = "lower(substring($.code, 1, 3))", distinct = true } ]

[[projection.stages]]
name    = "jobs"
from    = "job-topic"
keyPath = "$.id"
emit    = [ { field = "name", from = "$.name" } ]

[[projection.stages]]
name  = "m"
merge = [ "jobs", { stage = "codes", as = "c" } ]
emit  = [ { field = "label", expr = "concat($.jobs.name, ' (', length($.jobs.name), ')')" } ]

[[projection.sources]]
from = "m"
[[projection.sources.rules]]
set = [ { column = "label", from = "$.label" } ]

[[projection.sources]]
topic    = "job-topic"
keyPath  = "$.id"
rowOwner = true
[[projection.sources.rules]]
set = [ { column = "display_name", expr = "trim(coalesce($.first, '') || ' ' || coalesce($.last, ''))" } ]
`

func TestStringExprTOMLDecodesOnEverySite(t *testing.T) {
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
	require.NoError(t, parse(stringExprTOML))

	bad := func(from, to string) error {
		require.Contains(t, stringExprTOML, from, "fixture lacks the text to replace")
		return parse(strings.Replace(stringExprTOML, from, to, 1))
	}
	require.ErrorContains(t, bad(`upper(trim($.code))`, `upper(trim($.code), 'x')`), "upper takes 1–1 arguments")
	require.ErrorContains(t, bad(`'SVC-%' and`, `'SVC-%' | `), "concatenation is ||")
	require.ErrorContains(t, bad(`substring($.code, 1, 3)`, `reverse($.code)`), "the closed set is")
}
