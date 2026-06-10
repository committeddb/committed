package cluster_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/config"
)

func parseTOML(t *testing.T, data string) *cluster.ParsedConfig {
	t.Helper()
	c, err := cluster.ParseConfigBytes("text/toml", []byte(data))
	require.NoError(t, err)
	return c
}

// The reason this type exists: user data in map keys survives parsing
// byte-exact. The previous pipeline lowercased every map key at read
// time ($.eventType silently became $.eventtype).
func TestParsedConfigPreservesMapKeyCase(t *testing.T) {
	c := parseTOML(t, `
[rules]
matchers = { "$.eventType" = "tenant.created", "$.Tier" = "prod" }
`)
	m, ok := c.Get("rules.matchers").(map[string]any)
	require.True(t, ok)
	require.Equal(t, map[string]any{
		"$.eventType": "tenant.created",
		"$.Tier":      "prod",
	}, m)
}

// Committed's own field names match case-insensitively at lookup time
// — the load-bearing tolerance stored configs may depend on — without
// rewriting the underlying data.
func TestParsedConfigCaseInsensitiveLookup(t *testing.T) {
	c := parseTOML(t, "[SQL]\nTopic = \"simple\"\nPRIMARYKEY = \"pk\"\n")

	require.Equal(t, "simple", c.GetString("sql.topic"))
	require.Equal(t, "pk", c.GetString("sql.primaryKey"))
	require.Equal(t, "pk", c.GetString("SQL.PrimaryKey"), "lookup paths are case-insensitive too")
	require.True(t, c.IsSet("sql.topic"))
	require.False(t, c.IsSet("sql.nope"))
}

// Exact key matches win over case-insensitive ones, deterministically.
func TestParsedConfigExactMatchWins(t *testing.T) {
	c := parseTOML(t, "[s]\n\"Topic\" = \"upper\"\n\"topic\" = \"lower\"\n")
	require.Equal(t, "lower", c.GetString("s.topic"))
	require.Equal(t, "upper", c.GetString("s.Topic"))
}

func TestParsedConfigCoercions(t *testing.T) {
	c := parseTOML(t, `
[v]
i = 5
f = 5.0
istr = "7"
b = true
bstr = "true"
bnum = 1
falsy = false
s = "text"
`)
	require.Equal(t, 5, c.GetInt("v.i"), "TOML integer (int64)")
	require.Equal(t, 5, c.GetInt("v.f"), "float")
	require.Equal(t, 7, c.GetInt("v.istr"), "numeric string")
	require.Equal(t, 1, c.GetInt("v.b"), "bool")
	require.Equal(t, 0, c.GetInt("v.missing"))

	require.True(t, c.GetBool("v.b"))
	require.True(t, c.GetBool("v.bstr"))
	require.True(t, c.GetBool("v.bnum"))
	require.False(t, c.GetBool("v.falsy"))
	require.False(t, c.GetBool("v.missing"))
	require.True(t, c.IsSet("v.falsy"), "presence, not truthiness")

	require.Equal(t, "text", c.GetString("v.s"))
	require.Equal(t, "5", c.GetString("v.i"))
	require.Equal(t, "true", c.GetString("v.b"))
	require.Equal(t, "", c.GetString("v.missing"))
}

func TestParsedConfigStringSliceAndMap(t *testing.T) {
	c := parseTOML(t, `
[sql]
Tables = ["public.Orders", "public.customers"]

[sql.Postgres]
Slot_Name   = "my_slot"
Publication = "my_pub"
`)
	require.Equal(t, []string{"public.Orders", "public.customers"}, c.GetStringSlice("sql.tables"),
		"slice elements are user data — case preserved")
	require.Equal(t, map[string]string{
		"slot_name":   "my_slot",
		"publication": "my_pub",
	}, c.GetStringMapString("sql.postgres"),
		"option-table keys lowercase for canonical lookup")
	require.Nil(t, c.GetStringSlice("sql.missing"))
	require.Empty(t, c.GetStringMapString("sql.missing"))
}

// UnmarshalKey matches struct fields case-insensitively (mapstructure)
// while values keep their case.
func TestParsedConfigUnmarshalKey(t *testing.T) {
	c := parseTOML(t, `
[[SQL.Mappings]]
JsonPath = "$.camelCase"
Column   = "pk"
Type     = "TEXT"
`)
	type mapping struct {
		JsonPath string `mapstructure:"jsonPath"`
		Column   string `mapstructure:"column"`
		SQLType  string `mapstructure:"type"`
	}
	var mappings []mapping
	require.NoError(t, c.UnmarshalKey("sql.mappings", &mappings))
	require.Equal(t, []mapping{{JsonPath: "$.camelCase", Column: "pk", SQLType: "TEXT"}}, mappings)

	var absent []mapping
	require.NoError(t, c.UnmarshalKey("sql.absent", &absent))
	require.Nil(t, absent)
}

func TestParsedConfigJSONMimeType(t *testing.T) {
	c, err := cluster.ParseConfigBytes("application/json",
		[]byte(`{"sql": {"topic": "simple", "version": 3, "keys": {"$.eventType": "x"}}}`))
	require.NoError(t, err)
	require.Equal(t, "simple", c.GetString("sql.topic"))
	require.Equal(t, 3, c.GetInt("sql.version"), "JSON number (float64)")
	require.Equal(t, map[string]any{"$.eventType": "x"}, c.Get("sql.keys"), "JSON map keys preserved")
}

func TestParsedConfigParseErrors(t *testing.T) {
	_, err := cluster.ParseConfigBytes("text/toml", []byte("not = valid = toml"))
	require.Error(t, err)
	_, err = cluster.ParseConfigBytes("application/json", []byte("{nope"))
	require.Error(t, err)
}

// The decoded tree is the same shape the ${VAR} interpolation walker
// expects — including inside arrays of tables, where header secrets
// live.
func TestParsedConfigInterpolatesInPlace(t *testing.T) {
	t.Setenv("PARSED_CONFIG_TEST_TOKEN", "tok-123")
	c := parseTOML(t, `
[http]
url = "${PARSED_CONFIG_TEST_TOKEN}"

[[http.headers]]
name  = "X-Auth"
value = "${PARSED_CONFIG_TEST_TOKEN}"
`)
	require.NoError(t, config.Interpolate(c.Values()))
	require.Equal(t, "tok-123", c.GetString("http.url"))

	headers, ok := c.Get("http.headers").([]any)
	require.True(t, ok)
	require.Equal(t, "tok-123", headers[0].(map[string]any)["value"])
}
