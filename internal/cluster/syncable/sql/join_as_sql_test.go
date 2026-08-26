package sql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// as / optional admit through the real TOML path — the alive_pw shape:
// one stage, filter-and-pull joins, no scaffolding stages.
func TestJoinAsTOMLDecodes(t *testing.T) {
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
name    = "alive"
from    = "workareas"
keyPath = "$.id"
emit    = [
  { field = "tenant", from = "$.project.tenantId" },
  { field = "hasCustomer", expr = "$.cust is not null" },
]

[[projection.stage.join]]
topic = "projects"
on    = "$.projectId"
as    = "project"
where = [ { path = "$.sold", equals = "true" } ]

[[projection.stage.join]]
topic    = "customers"
on       = "$.custId"
as       = "cust"
optional = true

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
	js := cfg.Stages[0].Joins
	require.Len(t, js, 2)
	require.Equal(t, "project", js[0].As)
	require.False(t, js[0].Optional)
	require.Equal(t, "cust", js[1].As)
	require.True(t, js[1].Optional)

	cfg.applyDefaults()
	require.NoError(t, validateProjectionConfig(cfg))
}
