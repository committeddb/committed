package sql

import (
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// singleSourceProjectionTOML is the flat single-source form: topic, keyPath
// and rules directly under [projection].
const singleSourceProjectionTOML = `
[projection]
topic      = "orders"
db         = "testdb"
table      = "orders"
primaryKey = "order_id"
keyPath    = "$.order_id"

[[projection.columns]]
name = "order_id"
type = "VARCHAR(64)"

[[projection.rules]]
set = [ { column = "order_id", from = "$.order_id" } ]
`

// The [projection] vocabulary is exactly the keys the parser reads, in both
// the flat single-source form and the sources-and-stages form: a key read but
// not declared would be refused at POST, and one declared but never read
// would be accepted and ignored. Every other config section has this check;
// projectionSectionKeys is hand-maintained, so this is what keeps it honest.
func TestProjectionVocabulary_EqualsTheReads(t *testing.T) {
	read := map[string]bool{}
	for _, doc := range []string{singleSourceProjectionTOML, aggregateExprTOML} {
		observed := cluster.ObserveConfigReads(func() {
			v, err := cluster.ParseConfigBytes("toml", []byte(doc))
			require.NoError(t, err)
			p := &ProjectionSyncableParser{}
			_, err = parseProjectionConfigFields(v, nil)
			require.NoError(t, err)
			_ = p.TopicsFromConfig(v)
			_ = p.DatabasesFromConfig(v)
			_, err = p.SchemaFromConfig(v, nil)
			require.NoError(t, err)
		})
		for _, k := range observed[projectionSection] {
			read[k] = true
		}
	}
	undeclared, unread := cluster.VocabularyDiff(slices.Collect(maps.Keys(projectionSectionKeys)), slices.Collect(maps.Keys(read)))
	require.Empty(t, undeclared, "[projection]: keys read but not declared")
	require.Empty(t, unread, "[projection]: keys declared but never read")
}
