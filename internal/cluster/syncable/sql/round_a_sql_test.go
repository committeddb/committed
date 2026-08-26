package sql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// fan / field / topic merge sides admit through the real TOML path
// (every vocabulary word gets one — the liveSet lesson).
func TestRoundATOMLDecodes(t *testing.T) {
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
name    = "els"
from    = "txn-events"
keyPath = "$.wa"
fan     = [
  { forEach = "$.elements[*]", when = [ { path = "$.type", equals = "created" } ] },
  { forEach = "$.added[*]",    when = [ { path = "$.type", equals = "elements-added" } ] },
]
elementKey = "$.id"
normalize  = "lower"
reduce  = "aggregate"
emit    = [ { field = "total", sum = "$.amount" } ]

[[projection.stage]]
name    = "latest-prop"
from    = "proposals"
keyPath = "$.id"
emit    = [ { field = "pid", from = "$.projectId" }, { field = "amount", from = "$.amount" } ]

[[projection.stage]]
name    = "cand"
from    = "projects"
keyPath = "$.id"
emit    = [ { field = "amount", expr = "coalesce($.lp.amount, 0)" } ]

[[projection.stage.join]]
from     = "latest-prop"
on       = "$.id"
field    = "$.pid"
as       = "lp"
optional = true

[[projection.stage]]
name  = "quoted"
merge = [ "els", { topic = "workareas", keyPath = "$.Id", normalize = "lower", as = "wa" } ]
emit  = [ { field = "v", expr = "coalesce($.wa.quotedPrice, 0) - coalesce($.els.total, 0)" } ]

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

	require.Len(t, cfg.Stages[0].Fan, 2)
	require.Equal(t, "$.added[*]", cfg.Stages[0].Fan[1].ForEach)
	require.Equal(t, "elements-added", cfg.Stages[0].Fan[1].When[0].Equals)

	require.Equal(t, "$.pid", cfg.Stages[2].Joins[0].Field)
	require.Equal(t, "lp", cfg.Stages[2].Joins[0].As)

	me := cfg.Stages[3].Merge[1]
	require.Equal(t, "workareas", me.Topic)
	require.Equal(t, []string{"$.Id"}, me.KeyPath)
	require.Equal(t, "lower", me.Normalize)
	require.Equal(t, "wa", me.As)

	cfg.applyDefaults()
	require.NoError(t, validateProjectionConfig(cfg))
}
