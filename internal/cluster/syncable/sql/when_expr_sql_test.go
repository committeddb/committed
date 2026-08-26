package sql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

const whenExprTOML = `
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
name    = "quoted-open"
from    = "quotes"
keyPath = "$.wa"
when    = [ { expr = "$.quoted - coalesce($.invoiced, 0) > 0 and $.pricing in (0, 2)" } ]
emit    = [ { field = "remaining", expr = "$.quoted - coalesce($.invoiced, 0)" } ]

[[projection.source]]
topic   = "x"
keyPath = "$.id"
[[projection.source.rules]]
set = [ { column = "v", from = "$.v" } ]
`

// The expr when arm admits through the real TOML path (the liveSet
// lesson: every vocabulary word gets one), and a malformed expression
// is a loud admission error, not a stored time bomb.
func TestWhenExprTOMLDecodes(t *testing.T) {
	v, err := cluster.ParseConfigBytes("toml", []byte(whenExprTOML))
	require.NoError(t, err)
	cfg, err := parseProjectionConfigFields(v, nil)
	require.NoError(t, err)
	require.Len(t, cfg.Stages, 1)
	require.Len(t, cfg.Stages[0].When, 1)
	require.Contains(t, cfg.Stages[0].When[0].Expr, "coalesce($.invoiced, 0)")

	cfg.applyDefaults()
	require.NoError(t, validateProjectionConfig(cfg))

	// Malformed expr: loud at admission.
	bad := []byte(`
[projection]
db         = "testdb"
table      = "t"
primaryKey = "id"

[[projection.columns]]
name = "id"
type = "VARCHAR(64)"

[[projection.stage]]
name    = "s"
from    = "quotes"
keyPath = "$.wa"
when    = [ { expr = "$.quoted >" } ]
emit    = [ { field = "id", from = "$.wa" } ]

[[projection.source]]
topic   = "x"
keyPath = "$.id"
[[projection.source.rules]]
set = [ { column = "id", from = "$.id" } ]
`)
	v, err = cluster.ParseConfigBytes("toml", bad)
	require.NoError(t, err)
	cfg, err = parseProjectionConfigFields(v, nil)
	if err == nil {
		cfg.applyDefaults()
		err = validateProjectionConfig(cfg)
	}
	require.Error(t, err)
}

// Two configs differing only in a when expr must fingerprint apart —
// the expr is declared content (the any-typed-arms MarshalJSON covers
// it), so an edited gate forces the store reset + re-derive.
func TestWhenExprChangesFingerprint(t *testing.T) {
	parse := func(toml string) *ProjectionConfig {
		v, err := cluster.ParseConfigBytes("toml", []byte(toml))
		require.NoError(t, err)
		cfg, err := parseProjectionConfigFields(v, nil)
		require.NoError(t, err)
		cfg.applyDefaults()
		require.NoError(t, validateProjectionConfig(cfg))
		return cfg
	}
	a := parse(whenExprTOML)
	b := parse(whenExprTOML)
	require.Equal(t, a.projectionShapeFingerprint(), b.projectionShapeFingerprint(), "same declaration, same identity")

	c := parse(string([]byte(whenExprTOML))) // fresh copy…
	c.Stages[0].When[0].Expr = "$.quoted - coalesce($.invoiced, 0) > 1"
	require.NotEqual(t, a.projectionShapeFingerprint(), c.projectionShapeFingerprint(), "an edited gate is a different computation")
}
