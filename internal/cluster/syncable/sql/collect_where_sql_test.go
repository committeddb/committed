package sql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// collect / distinct / where admit through the real TOML path (every
// vocabulary word gets one) — the job_division_groups + visits shapes.
func TestCollectAndEmitWhereTOMLDecode(t *testing.T) {
	toml := `
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
name    = "groups"
from    = "workareas"
keyPath = "$.job"
reduce  = "aggregate"
emit    = [
  { field = "divisions", collect = "coalesce(nullif($.group, ''), $.name)", distinct = true },
  { field = "n", count = true },
  { field = "reviewed", count = true, where = [ { path = "$.billed", equals = "true" } ] },
  { field = "billedHours", sum = "$.hours", where = [ { expr = "$.billed = 'true' and $.hours > 0" } ] },
]

[[projection.source]]
topic   = "x"
keyPath = "$.id"
[[projection.source.rules]]
set = [ { column = "v", from = "$.v" } ]
`
	v, err := cluster.ParseConfigBytes("toml", []byte(toml))
	require.NoError(t, err)
	cfg, err := parseProjectionConfigFields(v, nil)
	require.NoError(t, err)
	em := cfg.Stages[0].Emit
	require.Len(t, em, 4)
	require.Contains(t, em[0].Collect, "nullif($.group, '')")
	require.True(t, em[0].Distinct)
	require.Len(t, em[2].Where, 1)
	require.Equal(t, "$.billed", em[2].Where[0].Path)
	require.Contains(t, em[3].Where[0].Expr, "$.hours > 0")

	cfg.applyDefaults()
	require.NoError(t, validateProjectionConfig(cfg))
}
