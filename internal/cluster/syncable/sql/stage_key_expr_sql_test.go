package sql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// A computed stage key through the real TOML path — the monthly rollup in
// one stage, no scaffolding reshape.
const stageKeyExprTOML = `
[projection]
db         = "testdb"
table      = "revenue"
primaryKey = ["customer", "month"]

[[projection.columns]]
name = "customer"
type = "VARCHAR(64)"

[[projection.columns]]
name = "month"
type = "TIMESTAMPTZ"

[[projection.columns]]
name = "total"
type = "DECIMAL(12,2)"

[[projection.stages]]
name    = "monthly"
from    = "payments"
keyPath = [ "$.customer", "date_trunc('month', $.paid_at::timestamptz)" ]
reduce  = "aggregate"
emit    = [ { field = "total", sum = "$.amount" } ]

[[projection.sources]]
from = "monthly"
[[projection.sources.rules]]
set = [ { column = "total", from = "$.total" } ]
`

func TestStageKeyExpressionTOML(t *testing.T) {
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
	require.NoError(t, parse(stageKeyExprTOML))
	require.ErrorContains(t, parse(strings.Replace(stageKeyExprTOML, "'month'", "'fortnight'", 1)), "neither a path nor a valid expression")
}

// A table source keys rows by a path. An expression there would parse as a
// jsonpath function call and silently resolve nothing, so it is rejected
// with a pointer to stage keys — while the jsonpath library's bare forms,
// which resolve today, stay legal.
func TestTableSourceKeyPathRejectsExpressions(t *testing.T) {
	src := func(kp string) *ProjectionConfig {
		return &ProjectionConfig{Sources: []ProjectionSource{{
			Topic: "e", KeyPath: []string{kp},
			Rules: []ProjectionRule{{Set: []ProjectionSet{{Column: "v", From: "$.v"}}}},
		}}}
	}
	for _, kp := range []string{"lower($.email)", "date_trunc('month', $.at::timestamptz)", "lower(email)"} {
		require.ErrorContains(t, validateProjectionJSONPaths(src(kp)), "compute it in a stage's keyPath", kp)
	}
	for _, kp := range []string{"$.id", "tenant_id", "a.b"} {
		require.NoError(t, validateProjectionJSONPaths(src(kp)), kp)
	}
}
