package sql_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

// The plain sql syncable consumes the single topic at sql.topic — read straight
// from the config with no Init.
func TestTopicsFromConfig_Syncable(t *testing.T) {
	v := readConfig(t, "toml", strings.NewReader(`
[syncable]
name = "sink"
type = "sql"

[sql]
db = "mydb"
topic = "movies"
table = "movies"
primaryKey = "tconst"

[[sql.mappings]]
jsonPath = "$.tconst"
column = "tconst"
`))
	require.Equal(t, []string{"movies"}, (&sql.SyncableParser{}).TopicsFromConfig(v))
}

// A multi-source projection consumes every distinct source topic; a topic-level
// re-key on any of them must find this projection.
func TestTopicsFromConfig_ProjectionMultiSource(t *testing.T) {
	v := readConfig(t, "toml", strings.NewReader(`
[syncable]
name = "denorm"
type = "sql-projection"

[sql-projection]
db = "mydb"
table = "denorm"
primaryKey = "id"

[[sql-projection.source]]
topic = "movies"
keyPath = "$.id"

[[sql-projection.source]]
topic = "ratings"
keyPath = "$.movieId"

[[sql-projection.source]]
topic = "movies"
keyPath = "$.id"
`))
	// Distinct, order-preserving: movies once (deduped), then ratings.
	require.Equal(t, []string{"movies", "ratings"}, (&sql.ProjectionSyncableParser{}).TopicsFromConfig(v))
}

// The back-compat single-source projection shape names its topic at the top
// level rather than in a [[source]] block.
func TestTopicsFromConfig_ProjectionBackCompat(t *testing.T) {
	v := readConfig(t, "toml", strings.NewReader(`
[syncable]
name = "legacy"
type = "sql-projection"

[sql-projection]
db = "mydb"
table = "legacy"
primaryKey = "id"
topic = "movies"
keyPath = "$.id"
`))
	require.Equal(t, []string{"movies"}, (&sql.ProjectionSyncableParser{}).TopicsFromConfig(v))
}
