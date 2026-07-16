package sql_test

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

func TestJSONNumberPaths(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want []string
	}{
		{"flat: only float-syntax leaves, ints/strings skipped", `{"d":1.50,"n":42,"s":"x"}`, []string{`$."d"`}},
		{"array + object mix", `{"items":[1.5,3],"whole":100.0}`, []string{`$."items"[0]`, `$."whole"`}},
		{"nested object", `{"nested":{"p":9.9}}`, []string{`$."nested"."p"`}},
		{"top-level array", `[1.5,2]`, []string{`$[0]`}},
		{"key needing quoting", `{"a.b c":1.5}`, []string{`$."a.b c"`}},
		{"no fractional numbers", `{"a":1,"b":"x"}`, nil},
		{"parse error", `{bad`, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sql.JSONNumberPaths([]byte(tt.raw))
			sort.Strings(got)
			sort.Strings(tt.want)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestRenderMySQLJSONByType(t *testing.T) {
	tests := []struct {
		name  string
		raw   string
		types map[string]string
		want  string
	}{
		{"decimal kept exact", `{"d": 1.50}`, map[string]string{`$."d"`: "DECIMAL"}, `{"d":1.50}`},
		{"double normalized", `{"d": 1.50}`, map[string]string{`$."d"`: "DOUBLE"}, `{"d":1.5}`},
		{"decimal past 2^53 kept exact", `{"d": 9007199254740993.5}`, map[string]string{`$."d"`: "DECIMAL"}, `{"d":9007199254740993.5}`},
		{"whole double collapses", `{"w": 100.0}`, map[string]string{`$."w"`: "DOUBLE"}, `{"w":100}`},
		{"mixed per-leaf", `{"dec": 1.50, "dbl": 1.5}`, map[string]string{`$."dec"`: "DECIMAL", `$."dbl"`: "DOUBLE"}, `{"dbl":1.5,"dec":1.50}`},
		{"array leaves", `{"items": [9.99, 3.14]}`, map[string]string{`$."items"[0]`: "DECIMAL", `$."items"[1]`: "DOUBLE"}, `{"items":[9.99,3.14]}`},
		{"integer untouched", `{"n": 42}`, map[string]string{}, `{"n":42}`},
		{"unresolved leaf kept exact", `{"d": 1.50}`, map[string]string{}, `{"d":1.50}`},
		{"parse error returns raw", `{bad`, nil, `{bad`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sql.RenderMySQLJSONByType([]byte(tt.raw), tt.types)
			require.Equal(t, tt.want, string(got))
		})
	}
}
