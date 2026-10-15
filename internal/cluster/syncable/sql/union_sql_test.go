package sql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The union ticket's acceptance read models, authored through the real
// TOML path. The docker oracle (dialects/projection_union_docker_test)
// runs the same shapes against PostgreSQL's UNION ALL.
const unionAcceptanceTOML = `
[projection]
db         = "testdb"
table      = "revenue"
primaryKey = "customer"

[[projection.columns]]
name = "customer"
type = "VARCHAR(64)"

[[projection.columns]]
name = "avg_amount"
type = "DECIMAL(18,2)"

# 1. Cross-system revenue: invoices from two systems as rows of one fold.
[[projection.stages]]
name    = "revenue"
from    = [ "invoices-a", "invoices-b" ]
keyPath = "$.customer"
reduce  = "aggregate"
emit = [ { field = "avg_amount", expr = "round(avg($.amount), 2)" },
         { field = "products",   expr = "count(distinct $.product)" },
         { field = "amounts",    expr = "array_agg($.amount order by $.at::timestamptz, source())" } ]

# 2. Account liveness: create and delete events in different topics.
[[projection.stages]]
name       = "accounts"
from       = [ "account-opened", "account-closed" ]
keyPath    = "$.account"
reduce     = "liveSet"
deleteWhen = [ { expr = "source() = 'account-closed'" } ]
emit       = [ { field = "owner", from = "$.owner" } ]

# 3. Last activity of any kind, tied by source().
[[projection.stages]]
name    = "last-activity"
from    = [ "logins", "orders", "tickets" ]
keyPath = "$.user_id"
reduce  = "latest"
orderBy = [ "$.at::timestamptz" ]
tieBy   = [ "source()" ]
emit    = [ { field = "kind", expr = "source()" }, { field = "at", from = "$.at" } ]

# A timeline across event-type topics: a window over a union.
[[projection.stages]]
name        = "order-timeline"
from        = [ "order-placed", "order-shipped", "order-cancelled" ]
reduce      = "window"
partitionBy = "$.order_id"
orderBy     = [ "$.at::timestamptz" ]
tieBy       = [ "source()" ]
emit = [ { field = "kind",            expr = "source()" },
         { field = "secs_since_prev", expr = "extract(epoch from ($.at::timestamptz - lag($.at::timestamptz)))" } ]

[[projection.sources]]
from = "revenue"
[[projection.sources.rules]]
set = [ { column = "avg_amount", from = "$.avg_amount" } ]
`

func TestUnionStagesTOML(t *testing.T) {
	cfg, err := parseWindowTOML(t, unionAcceptanceTOML)
	require.NoError(t, err)
	require.Equal(t, []string{"invoices-a", "invoices-b"}, cfg.Stages[0].FromList)
	require.Empty(t, cfg.Stages[0].From)
	last := cfg.Stages[2]
	require.Equal(t, []string{"$.at::timestamptz"}, last.WindowOrderBy, "latest's list form")
	require.Equal(t, []string{"source()"}, last.WindowTieBy)
	require.Empty(t, last.OrderBy)

	bad := func(from, to string) error {
		t.Helper()
		require.Contains(t, unionAcceptanceTOML, from, "fixture lacks the text to replace")
		_, err := parseWindowTOML(t, strings.Replace(unionAcceptanceTOML, from, to, 1))
		return err
	}
	require.ErrorContains(t, bad(`from    = [ "invoices-a", "invoices-b" ]`, `from    = [ "invoices-a", "invoices-a" ]`), `lists "invoices-a" twice`)
	require.ErrorContains(t, bad(`from    = [ "invoices-a", "invoices-b" ]`, `from    = [ "invoices-a", 7 ]`), "entry 2 is int64")
	require.ErrorContains(t, bad(`from    = [ "invoices-a", "invoices-b" ]`, `from    = [ "invoices-a", "accounts" ]`), "at or after this one")
	require.ErrorContains(t, bad(`expr = "round(avg($.amount), 2)"`, `expr = "source()"`), "source() is outside an aggregate")
	require.ErrorContains(t, bad(`set = [ { column = "avg_amount", from = "$.avg_amount" } ]`,
		`when = [ { expr = "source() = 'revenue'" } ]
set = [ { column = "avg_amount", from = "$.avg_amount" } ]`), "a table rule reads its own source's payload")
	require.ErrorContains(t, bad(`set = [ { column = "avg_amount", from = "$.avg_amount" } ]`,
		`set = [ { column = "avg_amount", expr = "length(source())" } ]`), "a table rule reads its own source's payload")
	require.ErrorContains(t, bad(`tieBy   = [ "source()" ]
emit    = [ { field = "kind"`, `tieBy   = [ "source()" ]
orderByType = "number"
emit    = [ { field = "kind"`), "cast instead")

	// A one-entry list is the single form.
	one, err := parseWindowTOML(t, strings.Replace(unionAcceptanceTOML, `from    = [ "invoices-a", "invoices-b" ]`, `from    = [ "invoices-a" ]`, 1))
	require.NoError(t, err)
	require.Equal(t, "invoices-a", one.Stages[0].From)
	require.Empty(t, one.Stages[0].FromList)
}

// A single source keeps its fingerprint whichever way it is written, and
// latest's single-path orderBy keeps its representation; adding a source
// to a list is a different computation.
func TestUnionFingerprint(t *testing.T) {
	fp := func(src string) string {
		t.Helper()
		cfg, err := parseWindowTOML(t, src)
		require.NoError(t, err)
		return cfg.projectionShapeFingerprint()
	}
	base := fp(unionAcceptanceTOML)
	require.Equal(t,
		fp(strings.Replace(unionAcceptanceTOML, `from    = [ "invoices-a", "invoices-b" ]`, `from    = "invoices-a"`, 1)),
		fp(strings.Replace(unionAcceptanceTOML, `from    = [ "invoices-a", "invoices-b" ]`, `from    = [ "invoices-a" ]`, 1)))
	require.NotEqual(t, base, fp(strings.Replace(unionAcceptanceTOML, `"invoices-a", "invoices-b" ]`, `"invoices-a", "invoices-b", "invoices-c" ]`, 1)))

	single := strings.Replace(unionAcceptanceTOML, `orderBy = [ "$.at::timestamptz" ]
tieBy   = [ "source()" ]`, `orderBy = "$.at"
tieBy   = "$.id"
orderByType = "text"`, 1)
	cfg, err := parseWindowTOML(t, single)
	require.NoError(t, err)
	require.Equal(t, "$.at", cfg.Stages[2].OrderBy, "a bare path stays the single-path form")
	require.Equal(t, "$.id", cfg.Stages[2].TieBy)
	require.Empty(t, cfg.Stages[2].WindowOrderBy)

	_, err = parseWindowTOML(t, strings.Replace(single, `orderBy = "$.at"`, `orderBy = "$.at desc"`, 1))
	require.ErrorContains(t, err, "cast instead", "a direction is the list form, where orderByType does not apply")
}
