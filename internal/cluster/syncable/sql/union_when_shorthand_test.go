package sql_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

// A when shorthand on a union stage resolves against the discriminator
// every listed topic shares; topics that disagree must spell the clause.
func TestUnionStageWhenShorthand(t *testing.T) {
	const toml = `
[projection]
db         = "testdb"
table      = "accounts"
primaryKey = "account"

[[projection.columns]]
name = "account"
type = "VARCHAR(64)"

[[projection.columns]]
name = "owner"
type = "VARCHAR(64)"

[[projection.stages]]
name    = "opened"
from    = [ "accounts-a", "accounts-b" ]
keyPath = "$.account"
when    = "account.opened"
emit    = [ { field = "owner", from = "$.owner" } ]

[[projection.sources]]
from = "opened"
[[projection.sources.rules]]
set = [ { column = "owner", from = "$.owner" } ]
`
	v := readConfig(t, "toml", strings.NewReader(toml))
	storage := &typeResolvingStorage{
		TestDatabaseStorage: *projectionStorage(),
		types: map[string]*cluster.Type{
			"accounts-a": {ID: "accounts-a", EntityKind: cluster.EntityKindEvent, Discriminator: "$.type"},
			"accounts-b": {ID: "accounts-b", EntityKind: cluster.EntityKindEvent, Discriminator: "$.type"},
		},
	}
	config, err := (&sql.ProjectionSyncableParser{}).ParseConfig(v, storage)
	require.NoError(t, err)
	require.Equal(t, []sql.WhenClause{{Path: "$.type", Equals: "account.opened"}}, config.Stages[0].When)

	storage.types["accounts-b"].Discriminator = "$.kind"
	_, err = (&sql.ProjectionSyncableParser{}).ParseConfig(v, storage)
	require.ErrorContains(t, err, "discriminators differ")
	require.ErrorContains(t, err, "{ path, equals }")
}
