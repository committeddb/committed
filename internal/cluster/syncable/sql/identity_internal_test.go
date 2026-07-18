package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// comparableWith builds a *schemaComparable from a plain-syncable config — enough
// to exercise SchemaChange, which compares config identity/schema and touches no
// db handle.
func comparableWith(topic, table string, cols ...[2]string) *schemaComparable {
	c := &Config{Topic: topic, Table: table, PrimaryKey: "id"}
	for _, col := range cols {
		c.Mappings = append(c.Mappings, Mapping{Column: col[0], SQLType: col[1]})
	}
	return &schemaComparable{schema: schemaOf(c), identity: identityOf(c)}
}

// comparableWithDB is comparableWith plus an explicit destination-database id, for
// exercising the database-repoint identity guard.
func comparableWithDB(dbID, topic, table string, cols ...[2]string) *schemaComparable {
	c := &Config{DatabaseID: dbID, Topic: topic, Table: table, PrimaryKey: "id"}
	for _, col := range cols {
		c.Mappings = append(c.Mappings, Mapping{Column: col[0], SQLType: col[1]})
	}
	return &schemaComparable{schema: schemaOf(c), identity: identityOf(c)}
}

// TestSchemaChange_TableRenameRejected is the A2 headline: a re-POST that renames
// the destination table inherits the SyncableIndex checkpoint by id, so the new
// table would be materialized only from the checkpoint forward (old table
// orphaned, pre-checkpoint history missing). It must be rejected, not silently
// applied.
func TestSchemaChange_TableRenameRejected(t *testing.T) {
	prior := comparableWith("orders", "orders_sink", [2]string{"id", "VARCHAR(128)"})
	next := comparableWith("orders", "orders_sink_v2", [2]string{"id", "VARCHAR(128)"})

	err := next.SchemaChange(prior)
	require.Error(t, err, "a table rename must be rejected: the inherited checkpoint is stale for the new table")
	var idErr *IdentityChangeError
	require.ErrorAs(t, err, &idErr)
	require.Equal(t, []string{"table"}, idErr.ChangedFields)
	require.Equal(t, identityChangeCode, idErr.Code())
	require.Contains(t, idErr.Error(), "deleting and recreating")
	require.NotContains(t, idErr.Error(), "%!", "no format mishaps")
}

// TestSchemaChange_TopicRepointRejected: same table, different consumed topic —
// the checkpoint indexes into the OLD topic's stream, so consuming a new topic
// from that index skips its history. Must be rejected.
func TestSchemaChange_TopicRepointRejected(t *testing.T) {
	prior := comparableWith("orders", "sink", [2]string{"id", "VARCHAR(128)"})
	next := comparableWith("shipments", "sink", [2]string{"id", "VARCHAR(128)"})

	var idErr *IdentityChangeError
	require.ErrorAs(t, next.SchemaChange(prior), &idErr)
	require.Equal(t, []string{"topic"}, idErr.ChangedFields)
}

// TestSchemaChange_TopicAndTableRejected reports both changed fields.
func TestSchemaChange_TopicAndTableRejected(t *testing.T) {
	prior := comparableWith("orders", "sink", [2]string{"id", "VARCHAR(128)"})
	next := comparableWith("shipments", "sink_v2", [2]string{"id", "VARCHAR(128)"})

	var idErr *IdentityChangeError
	require.ErrorAs(t, next.SchemaChange(prior), &idErr)
	require.ElementsMatch(t, []string{"topic", "table"}, idErr.ChangedFields)
}

// TestSchemaChange_DatabaseRepointRejected: same topic + table, different
// destination database. The SyncableIndex checkpoint is inherited by id, so the
// new database would be materialized only from the checkpoint forward (its
// pre-checkpoint history is never written). Must be rejected.
func TestSchemaChange_DatabaseRepointRejected(t *testing.T) {
	prior := comparableWithDB("warehouse-a", "orders", "sink", [2]string{"id", "VARCHAR(128)"})
	next := comparableWithDB("warehouse-b", "orders", "sink", [2]string{"id", "VARCHAR(128)"})

	var idErr *IdentityChangeError
	require.ErrorAs(t, next.SchemaChange(prior), &idErr)
	require.Equal(t, []string{"database"}, idErr.ChangedFields)
	require.Equal(t, "warehouse-a", idErr.OldDatabase)
	require.Equal(t, "warehouse-b", idErr.NewDatabase)
	require.Contains(t, idErr.Error(), "warehouse-a")
	require.NotContains(t, idErr.Error(), "%!", "no format mishaps")
}

// TestSchemaChange_SameDatabaseIsNil: re-POSTing with the same destination
// database (and same topic + table) is not an identity change.
func TestSchemaChange_SameDatabaseIsNil(t *testing.T) {
	prior := comparableWithDB("warehouse-a", "orders", "sink", [2]string{"id", "VARCHAR(128)"})
	next := comparableWithDB("warehouse-a", "orders", "sink", [2]string{"id", "VARCHAR(128)"})
	require.NoError(t, next.SchemaChange(prior))
}

// TestSchemaChange_IdenticalIsNil: an identical re-POST is a true no-op.
func TestSchemaChange_IdenticalIsNil(t *testing.T) {
	prior := comparableWith("orders", "sink", [2]string{"id", "VARCHAR(128)"})
	next := comparableWith("orders", "sink", [2]string{"id", "VARCHAR(128)"})
	require.NoError(t, next.SchemaChange(prior))
}

// TestSchemaChange_SameIdentitySchemaChangeStillRebuild: identity unchanged,
// columns changed — the schema guard still fires (regression), and the identity
// guard does NOT swallow it.
func TestSchemaChange_SameIdentitySchemaChangeStillRebuild(t *testing.T) {
	prior := comparableWith("orders", "sink", [2]string{"id", "VARCHAR(128)"})
	next := comparableWith("orders", "sink", [2]string{"id", "VARCHAR(128)"}, [2]string{"tier", "TEXT"})

	err := next.SchemaChange(prior)
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
