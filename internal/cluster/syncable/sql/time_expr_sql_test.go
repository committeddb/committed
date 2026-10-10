package sql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// Date/time on every expr site, through the real TOML path: a table set
// entry, a reshape emit, a fold arm, the when expr arm, and a merge emit.
const timeExprTOML = `
[projection]
db         = "testdb"
table      = "t"
primaryKey = "id"

[[projection.columns]]
name = "id"
type = "VARCHAR(64)"

[[projection.columns]]
name = "due"
type = "TIMESTAMPTZ"

[[projection.columns]]
name = "first_month"
type = "DATE"

[[projection.stages]]
name    = "paid"
from    = "payment-topic"
keyPath = "$.id"
when    = [ { expr = "cast($.paid_at as timestamptz) >= '2024-01-01' and extract(dow from cast($.paid_at as date)) between 1 and 5" } ]
emit    = [ { field = "customer", from = "$.customer" },
            { field = "month",    expr = "date_trunc('month', $.paid_at::timestamptz)" } ]

[[projection.stages]]
name    = "by-customer"
from    = "paid"
keyPath = "$.customer"
reduce  = "aggregate"
emit    = [ { field = "first", min = "cast($.month as date)" } ]

[[projection.stages]]
name    = "customers"
from    = "customer-topic"
keyPath = "$.id"
emit    = [ { field = "since", from = "$.since" } ]

[[projection.stages]]
name  = "m"
merge = [ "customers", { stage = "by-customer", as = "agg" } ]
emit  = [ { field = "first", expr = "coalesce(cast($.agg.first as date), cast($.customers.since as date))" } ]

[[projection.sources]]
from = "m"
[[projection.sources.rules]]
set = [ { column = "first_month", from = "$.first" } ]

[[projection.sources]]
topic    = "customer-topic"
keyPath  = "$.id"
rowOwner = true
[[projection.sources.rules]]
set = [ { column = "due", expr = "cast($.created as timestamptz) + interval '30 days'" } ]
`

func TestTimeExprTOMLDecodesOnEverySite(t *testing.T) {
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
	require.NoError(t, parse(timeExprTOML))

	bad := func(from, to string) error {
		require.Contains(t, timeExprTOML, from, "fixture lacks the text to replace")
		return parse(strings.Replace(timeExprTOML, from, to, 1))
	}
	require.ErrorContains(t, bad(`date_trunc('month'`, `date_trunc('fortnight'`), `unit "fortnight" is not recognized`)
	require.ErrorContains(t, bad(`extract(dow from`, `extract(dow`), "expected from in extract")
	require.ErrorContains(t, bad(`as timestamptz) + interval`, `as timestamptx) + interval`), "unknown cast type")
}
