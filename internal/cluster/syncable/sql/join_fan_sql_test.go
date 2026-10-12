package sql

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The fan ticket's acceptance read model, authored through the real TOML
// path; the docker oracle (dialects/projection_join_fan_docker_test) runs
// it against PostgreSQL's own join.
const fanJoinTOML = `
[projection]
db         = "testdb"
table      = "user_permissions"
primaryKey = ["user_id", "permission"]

[[projection.columns]]
name = "user_id"
type = "VARCHAR(64)"

[[projection.columns]]
name = "permission"
type = "VARCHAR(64)"

[[projection.columns]]
name = "groups"
type = "TEXT"

[[projection.stages]]
name    = "group-perms"
from    = "grants"
keyPath = "$.id"
emit    = [ { field = "group_id", from = "$.group_id" }, { field = "permission", from = "$.permission" } ]

[[projection.stages]]
name    = "user-perms"
from    = "memberships"
joins   = [ { from = "group-perms", field = "$.group_id", on = "$.group_id", as = "perm", fan = true } ]
keyPath = [ "$.user_id", "$.perm.permission" ]
reduce  = "aggregate"
emit    = [ { field = "groups", expr = "string_agg($.group_id, ',' order by $.group_id)" } ]

[[projection.sources]]
from = "user-perms"
[[projection.sources.rules]]
set = [ { column = "groups", from = "$.groups" } ]
`

func TestFanJoinTOML(t *testing.T) {
	cfg, err := parseWindowTOML(t, fanJoinTOML)
	require.NoError(t, err)
	require.True(t, cfg.Stages[1].Joins[0].Fan)

	bad := func(from, to string) error {
		t.Helper()
		require.Contains(t, fanJoinTOML, from, "fixture lacks the text to replace")
		_, err := parseWindowTOML(t, strings.Replace(fanJoinTOML, from, to, 1))
		return err
	}
	require.ErrorContains(t, bad(`as = "perm", fan = true`, `fan = true`), "name them with as")
	require.ErrorContains(t, bad(`field = "$.group_id", on`, `on`), "fan needs field")
	require.NoError(t, bad(`{ from = "group-perms", field`, `{ topic = "grants", field`), "a topic join may declare field when it fans")
	require.ErrorContains(t, bad(`as = "perm", fan = true`, `as = "perm"`), `addresses join alias "perm"`,
		"without fan the alias is refold-only, so keying by it is rejected")

	// fan is declared content; a config without it fingerprints as before.
	nofan, err := parseWindowTOML(t, strings.Replace(strings.Replace(fanJoinTOML, `, fan = true`, ``, 1),
		`keyPath = [ "$.user_id", "$.perm.permission" ]`, `keyPath = [ "$.user_id", "$.group_id" ]`, 1))
	require.NoError(t, err)
	require.NotEqual(t, cfg.projectionShapeFingerprint(), nofan.projectionShapeFingerprint())
	bs, err := json.Marshal(nofan.Stages)
	require.NoError(t, err)
	require.NotContains(t, string(bs), `"fan"`, "an undeclared fan adds nothing to the fingerprint")
}
