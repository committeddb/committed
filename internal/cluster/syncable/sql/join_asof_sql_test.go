package sql

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The as-of ticket's order-pricing model, authored through the real TOML
// path; the docker oracle (dialects/projection_join_asof_docker_test) runs
// the acceptance models against PostgreSQL's LATERAL equivalent.
const asOfJoinTOML = `
[projection]
db         = "testdb"
table      = "priced_orders"
primaryKey = "id"

[[projection.columns]]
name = "id"
type = "VARCHAR(64)"

[[projection.columns]]
name = "price"
type = "DECIMAL(10,2)"

[[projection.stages]]
name    = "priced"
from    = "orders"
keyPath = "$.id"
joins   = [ { topic = "price-changes", field = "$.product_id", on = "$.product_id",
              orderBy = [ "$.effective_from::timestamptz" ], tieBy = [ "$.change_id" ],
              asOf = "$.ordered_at::timestamptz", as = "price", optional = true } ]
emit    = [ { field = "price", from = "$.price.amount" } ]

[[projection.sources]]
from = "priced"
[[projection.sources.rules]]
set = [ { column = "price", from = "$.price" } ]
`

func TestAsOfJoinTOML(t *testing.T) {
	cfg, err := parseWindowTOML(t, asOfJoinTOML)
	require.NoError(t, err)
	j := cfg.Stages[0].Joins[0]
	require.Equal(t, []string{"$.effective_from::timestamptz"}, j.OrderBy)
	require.Equal(t, []string{"$.change_id"}, j.TieBy)
	require.Equal(t, "$.ordered_at::timestamptz", j.AsOf)

	bad := func(from, to string) error {
		t.Helper()
		require.Contains(t, asOfJoinTOML, from, "fixture lacks the text to replace")
		_, err := parseWindowTOML(t, strings.Replace(asOfJoinTOML, from, to, 1))
		return err
	}
	require.ErrorContains(t, bad(`tieBy = [ "$.change_id" ],`, ``), "needs tieBy")
	require.ErrorContains(t, bad(`orderBy = [ "$.effective_from::timestamptz" ]`, `orderBy = [ "$.effective_from::timestamptz desc" ]`), "drop desc")
	require.ErrorContains(t, bad(`asOf = "$.ordered_at::timestamptz"`, `asOf = "$.ordered_at::numeric"`), "cast both alike")
	require.ErrorContains(t, bad(`orderBy = [ "$.effective_from::timestamptz" ], tieBy = [ "$.change_id" ],
              asOf = "$.ordered_at::timestamptz", `, ``), "field addresses a STAGE's emitted field",
		"a topic field join without orderBy (or fan) stays rejected")
	require.NoError(t, bad(`asOf = "$.ordered_at::timestamptz", `, ``), "orderBy alone: the latest version")

	// The ordered vocabulary is declared content; a join without it
	// fingerprints as before.
	bs, err := json.Marshal(cfg.Stages)
	require.NoError(t, err)
	require.Contains(t, string(bs), `"asOf"`)
	plain, err := parseWindowTOML(t, strings.Replace(asOfJoinTOML, `topic = "price-changes", field = "$.product_id", on = "$.product_id",
              orderBy = [ "$.effective_from::timestamptz" ], tieBy = [ "$.change_id" ],
              asOf = "$.ordered_at::timestamptz", as`, `topic = "price-changes", on = "$.product_id", as`, 1))
	require.NoError(t, err)
	bs, err = json.Marshal(plain.Stages)
	require.NoError(t, err)
	for _, k := range []string{`"orderBy"`, `"tieBy"`, `"asOf"`} {
		require.NotContains(t, string(bs), k)
	}
}
