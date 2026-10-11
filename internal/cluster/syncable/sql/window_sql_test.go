package sql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/stages"
)

// The window ticket's four acceptance read models, authored through the
// real TOML path. The docker oracle (dialects/projection_window_docker_test)
// runs the same shapes against PostgreSQL's own window functions.
const windowAcceptanceTOML = `
[projection]
db         = "testdb"
table      = "job_status_time"
primaryKey = ["job", "status"]

[[projection.columns]]
name = "job"
type = "VARCHAR(64)"

[[projection.columns]]
name = "status"
type = "VARCHAR(64)"

[[projection.columns]]
name = "secs"
type = "DECIMAL(18,6)"

# 1. Time in status: each status event's span until the next one.
[[projection.stages]]
name        = "spans"
from        = "job-status"
reduce      = "window"
partitionBy = "$.job"
orderBy     = [ "$.at::timestamptz" ]
tieBy       = [ "$.seq" ]
emit = [ { field = "job",    from = "$.job" },
         { field = "status", from = "$.status" },
         { field = "secs",   expr = "extract(epoch from (lead($.at::timestamptz) - $.at::timestamptz))" },
         { field = "prev",   expr = "lag($.status)" },
         { field = "n",      expr = "row_number()" } ]

[[projection.stages]]
name    = "time-in-status"
from    = "spans"
keyPath = [ "$.job", "$.status" ]
reduce  = "aggregate"
emit    = [ { field = "secs", expr = "sum($.secs)" } ]

# 2. Running balance after each transaction.
[[projection.stages]]
name        = "balances"
from        = "txns"
reduce      = "window"
partitionBy = "$.account"
orderBy     = "$.at::timestamptz"
tieBy       = "$.txn_id"
emit = [ { field = "balance", expr = "sum($.amount)" },
         { field = "account", from = "$.account" } ]

# 3. An audit of status changes: the first row's lag is null.
[[projection.stages]]
name        = "changes"
from        = "job-status"
reduce      = "window"
partitionBy = "$.job"
orderBy     = [ "$.at::timestamptz" ]
tieBy       = [ "$.seq" ]
emit = [ { field = "changed", expr = "lag($.status) is distinct from $.status" },
         { field = "from",    expr = "lag($.status)" },
         { field = "to",      from = "$.status" } ]

# 4. Top 3 per group, per row: row_number, then a filter downstream.
[[projection.stages]]
name        = "ranked"
from        = "scores"
reduce      = "window"
partitionBy = [ "$.game" ]
orderBy     = [ "$.points desc", "$.at::timestamptz" ]
tieBy       = [ "$.player" ]
emit = [ { field = "n", expr = "row_number()" }, { field = "player", from = "$.player" }, { field = "game", from = "$.game" } ]

[[projection.stages]]
name    = "podium"
from    = "ranked"
keyPath = [ "$.game", "$.player" ]
when    = [ { expr = "$.n <= 3" } ]
emit    = [ { field = "n", from = "$.n" } ]

[[projection.sources]]
from = "time-in-status"
[[projection.sources.rules]]
set = [ { column = "secs", from = "$.secs" } ]
`

func parseWindowTOML(t *testing.T, src string) (*ProjectionConfig, error) {
	t.Helper()
	v, err := cluster.ParseConfigBytes("toml", []byte(src))
	require.NoError(t, err)
	cfg, err := parseProjectionConfigFields(v, nil)
	if err == nil {
		cfg.applyDefaults()
		err = validateProjectionConfig(cfg)
	}
	return cfg, err
}

func TestWindowStagesTOML(t *testing.T) {
	cfg, err := parseWindowTOML(t, windowAcceptanceTOML)
	require.NoError(t, err)
	spans := cfg.Stages[0]
	require.Equal(t, "window", spans.Reduce)
	require.Equal(t, []string{"$.job"}, spans.PartitionBy)
	require.Equal(t, []string{"$.at::timestamptz"}, spans.WindowOrderBy)
	require.Equal(t, []string{"$.seq"}, spans.WindowTieBy)
	require.Empty(t, spans.OrderBy, "a window's order keys are not latest's single path")
	bal := cfg.Stages[2]
	require.Equal(t, []string{"$.at::timestamptz"}, bal.WindowOrderBy, "a single string is a one-key list")
	require.Equal(t, []string{"$.txn_id"}, bal.WindowTieBy)
	require.Equal(t, []string{"$.points desc", "$.at::timestamptz"}, cfg.Stages[4].WindowOrderBy)

	bad := func(from, to string) error {
		t.Helper()
		require.Contains(t, windowAcceptanceTOML, from, "fixture lacks the text to replace")
		_, err := parseWindowTOML(t, strings.Replace(windowAcceptanceTOML, from, to, 1))
		return err
	}
	require.NoError(t, bad("tieBy       = \"$.txn_id\"\n", ""), "running sums treat ties as peers: no tieBy needed")
	require.ErrorContains(t, bad("tieBy       = [ \"$.player\" ]\n", ""), "declare tieBy")
	require.ErrorContains(t, bad(`partitionBy = "$.account"`, `keyPath = "$.account"`), "keyPath is not used on a window stage")
	require.ErrorContains(t, bad(`expr = "lag($.status) is distinct from $.status"`, `expr = "lag($.status) over (order by $.at)"`), "does not use a frame")
	// latest keeps its single-path orderBy; a list there names the window form.
	latest := strings.Replace(windowAcceptanceTOML, `name    = "podium"
from    = "ranked"
keyPath = [ "$.game", "$.player" ]`, `name    = "podium"
from    = "ranked"
keyPath = [ "$.game", "$.player" ]
reduce  = "latest"
orderBy = [ "$.n" ]
tieBy   = "$.player"`, 1)
	_, err = parseWindowTOML(t, latest)
	require.ErrorContains(t, err, "a list of order keys is the window form")

	// A stage-fed table keyed by a window inherits the window's input key
	// space; a topic-fed window's keys are the producer's, so any arity is
	// the author's contract.
	arity, _, _ := stages.ResolvedKeySpace(cfg.Stages, 0)
	require.Equal(t, stages.KeyArityProducer, arity)
}

// The window declaration is declared content: editing the order is a
// different computation, and a config without window stages fingerprints
// exactly as before the vocabulary existed.
func TestWindowFingerprint(t *testing.T) {
	cfg, err := parseWindowTOML(t, windowAcceptanceTOML)
	require.NoError(t, err)
	edited, err := parseWindowTOML(t, strings.Replace(windowAcceptanceTOML, `"$.points desc"`, `"$.points"`, 1))
	require.NoError(t, err)
	require.NotEqual(t, cfg.projectionShapeFingerprint(), edited.projectionShapeFingerprint())
}

// The frames ticket's acceptance models, through TOML: a moving average,
// a login-velocity flag, and a whole-partition last_value.
const windowFramesTOML = `
[projection]
db         = "testdb"
table      = "fraud_signals"
primaryKey = "id"

[[projection.columns]]
name = "id"
type = "VARCHAR(64)"

[[projection.columns]]
name = "hour_n"
type = "INT"

[[projection.stages]]
name        = "readings"
from        = "sensor-readings"
reduce      = "window"
partitionBy = "$.sensor"
orderBy     = "$.at::timestamptz"
tieBy       = "$.id"
emit = [ { field = "ma7", expr = "round(avg($.reading) over (rows between 6 preceding and current row), 2)" } ]

[[projection.stages]]
name        = "velocity"
from        = "logins"
reduce      = "window"
partitionBy = "$.user_id"
orderBy     = "$.at::timestamptz"
emit = [ { field = "hour_n", expr = "count(*) over (range between interval '1 hour' preceding and current row)" } ]

[[projection.stages]]
name    = "fraud"
from    = "velocity"
keyPath = "$.hour_n"
when    = [ { expr = "$.hour_n >= 5" } ]
emit    = [ { field = "hour_n", from = "$.hour_n" } ]

[[projection.stages]]
name        = "final"
from        = "job-status"
reduce      = "window"
partitionBy = "$.job"
orderBy     = "$.at::timestamptz"
tieBy       = "$.seq"
emit = [ { field = "final", expr = "last_value($.status) over (rows between unbounded preceding and unbounded following)" } ]

[[projection.sources]]
from = "fraud"
[[projection.sources.rules]]
set = [ { column = "hour_n", from = "$.hour_n" } ]
`

func TestWindowFramesTOML(t *testing.T) {
	_, err := parseWindowTOML(t, windowFramesTOML)
	require.NoError(t, err)
	bad := func(from, to string) error {
		t.Helper()
		require.Contains(t, windowFramesTOML, from, "fixture lacks the text to replace")
		_, err := parseWindowTOML(t, strings.Replace(windowFramesTOML, from, to, 1))
		return err
	}
	require.ErrorContains(t, bad(`interval '1 hour' preceding`, `1 preceding`), "is an interval")
	require.ErrorContains(t, bad(`orderBy     = "$.at::timestamptz"
emit = [ { field = "hour_n"`, `orderBy     = [ "$.at::timestamptz", "$.id" ]
emit = [ { field = "hour_n"`), "exactly one orderBy key")
	require.ErrorContains(t, bad(`rows between 6 preceding and current row`, `rows between current row and 6 preceding`), "cannot have preceding rows")
}
