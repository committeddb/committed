package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// syncableWith builds a bare *Syncable carrying only a config — enough to
// exercise ValidateReplace, which reads config identity/schema and touches no
// db handle.
func syncableWith(topic, table string, cols ...[2]string) *Syncable {
	c := &Config{Topic: topic, Table: table, PrimaryKey: "id"}
	for _, col := range cols {
		c.Mappings = append(c.Mappings, Mapping{Column: col[0], SQLType: col[1]})
	}
	return &Syncable{config: c}
}

// syncableWithDB is syncableWith plus an explicit destination-database id, for
// exercising the database-repoint identity guard.
func syncableWithDB(dbID, topic, table string, cols ...[2]string) *Syncable {
	s := syncableWith(topic, table, cols...)
	s.config.DatabaseID = dbID
	return s
}

// TestValidateReplace_TableRenameRejected is the A2 headline: a re-POST that
// renames the destination table inherits the SyncableIndex checkpoint by id, so
// the new table would be materialized only from the checkpoint forward (old
// table orphaned, pre-checkpoint history missing). It must be rejected, not
// silently applied.
func TestValidateReplace_TableRenameRejected(t *testing.T) {
	prior := syncableWith("orders", "orders_sink", [2]string{"id", "VARCHAR(128)"})
	next := syncableWith("orders", "orders_sink_v2", [2]string{"id", "VARCHAR(128)"})

	err := next.ValidateReplace(prior)
	require.Error(t, err, "a table rename must be rejected: the inherited checkpoint is stale for the new table")
	var idErr *IdentityChangeError
	require.ErrorAs(t, err, &idErr)
	require.Equal(t, []string{"table"}, idErr.ChangedFields)
	require.Equal(t, identityChangeCode, idErr.Code())
	require.Contains(t, idErr.Error(), "deleting and recreating")
	require.NotContains(t, idErr.Error(), "%!", "no format mishaps")
}

// TestValidateReplace_TopicRepointRejected: same table, different consumed topic
// — the checkpoint indexes into the OLD topic's stream, so consuming a new topic
// from that index skips its history. Must be rejected.
func TestValidateReplace_TopicRepointRejected(t *testing.T) {
	prior := syncableWith("orders", "sink", [2]string{"id", "VARCHAR(128)"})
	next := syncableWith("shipments", "sink", [2]string{"id", "VARCHAR(128)"})

	var idErr *IdentityChangeError
	require.ErrorAs(t, next.ValidateReplace(prior), &idErr)
	require.Equal(t, []string{"topic"}, idErr.ChangedFields)
}

// TestValidateReplace_TopicAndTableRejected reports both changed fields.
func TestValidateReplace_TopicAndTableRejected(t *testing.T) {
	prior := syncableWith("orders", "sink", [2]string{"id", "VARCHAR(128)"})
	next := syncableWith("shipments", "sink_v2", [2]string{"id", "VARCHAR(128)"})

	var idErr *IdentityChangeError
	require.ErrorAs(t, next.ValidateReplace(prior), &idErr)
	require.ElementsMatch(t, []string{"topic", "table"}, idErr.ChangedFields)
}

// TestValidateReplace_DatabaseRepointRejected: same topic + table, different
// destination database. The SyncableIndex checkpoint is inherited by id, so the
// new database would be materialized only from the checkpoint forward (its
// pre-checkpoint history is never written). Must be rejected.
func TestValidateReplace_DatabaseRepointRejected(t *testing.T) {
	prior := syncableWithDB("warehouse-a", "orders", "sink", [2]string{"id", "VARCHAR(128)"})
	next := syncableWithDB("warehouse-b", "orders", "sink", [2]string{"id", "VARCHAR(128)"})

	var idErr *IdentityChangeError
	require.ErrorAs(t, next.ValidateReplace(prior), &idErr)
	require.Equal(t, []string{"database"}, idErr.ChangedFields)
	require.Equal(t, "warehouse-a", idErr.OldDatabase)
	require.Equal(t, "warehouse-b", idErr.NewDatabase)
	require.Contains(t, idErr.Error(), "warehouse-a")
	require.NotContains(t, idErr.Error(), "%!", "no format mishaps")
}

// TestValidateReplace_SameDatabaseIsNil: re-POSTing with the same destination
// database (and same topic + table) is not an identity change.
func TestValidateReplace_SameDatabaseIsNil(t *testing.T) {
	prior := syncableWithDB("warehouse-a", "orders", "sink", [2]string{"id", "VARCHAR(128)"})
	next := syncableWithDB("warehouse-a", "orders", "sink", [2]string{"id", "VARCHAR(128)"})
	require.NoError(t, next.ValidateReplace(prior))
}

// TestValidateReplace_IdenticalIsNil: an identical re-POST is a true no-op.
func TestValidateReplace_IdenticalIsNil(t *testing.T) {
	prior := syncableWith("orders", "sink", [2]string{"id", "VARCHAR(128)"})
	next := syncableWith("orders", "sink", [2]string{"id", "VARCHAR(128)"})
	require.NoError(t, next.ValidateReplace(prior))
}

// TestValidateReplace_SameIdentitySchemaChangeStillRebuild: identity unchanged,
// columns changed — the existing schema guard still fires (regression), and the
// identity guard does NOT swallow it.
func TestValidateReplace_SameIdentitySchemaChangeStillRebuild(t *testing.T) {
	prior := syncableWith("orders", "sink", [2]string{"id", "VARCHAR(128)"})
	next := syncableWith("orders", "sink", [2]string{"id", "VARCHAR(128)"}, [2]string{"tier", "TEXT"})

	err := next.ValidateReplace(prior)
	require.Error(t, err)
	var schemaErr *SchemaChangeError
	require.ErrorAs(t, err, &schemaErr, "a same-identity column change is still a schema rebuild, not an identity change")
	require.Equal(t, []string{"tier"}, schemaErr.AddedColumns)
}

// TestIdentityChange_TopicOrderInsensitive: a projection whose source topics are
// reordered (same set) is not an identity change.
func TestIdentityChange_TopicOrderInsensitive(t *testing.T) {
	old := SyncableIdentity{Topics: []string{"a", "b"}, Table: "t"}
	next := SyncableIdentity{Topics: []string{"b", "a"}, Table: "t"}
	require.Nil(t, identityChange(old, next))
}

// TestIdentityChange_SourceTopicSetChanged: adding/removing a source topic is an
// identity change (the multi-source projection consumes a different set).
func TestIdentityChange_SourceTopicSetChanged(t *testing.T) {
	old := SyncableIdentity{Topics: []string{"a", "b"}, Table: "t"}
	next := SyncableIdentity{Topics: []string{"a", "c"}, Table: "t"}
	change := identityChange(old, next)
	require.NotNil(t, change)
	require.Equal(t, []string{"topic"}, change.ChangedFields)
}
