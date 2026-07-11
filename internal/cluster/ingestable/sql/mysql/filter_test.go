package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWatches_CaseInsensitive is the B1 regression: a mixed-case `tables` config
// must still match the binlog's table name (which arrives in the source's stored
// case), or the row filter drops every streamed change — total, silent CDC data
// loss after the initial snapshot. Exercises the full fix: lowerAll at
// construction plus the case-insensitive match.
func TestWatches_CaseInsensitive(t *testing.T) {
	h := &MySQLEventHandler{schema: "appdb", tables: lowerAll([]string{"Users", "OrderItems"})}

	require.True(t, h.watches("appdb", "Users"), "exact config case matches")
	require.True(t, h.watches("appdb", "users"), "lowercase binlog name matches the uppercase config")
	require.True(t, h.watches("appdb", "ORDERITEMS"), "any case matches")
	require.False(t, h.watches("appdb", "payments"), "an unwatched table does not match")
}

// TestWatches_SchemaScoped is the cross-database-contamination regression:
// MySQL's binlog is server-wide, so a bare table-name filter would ingest
// otherdb.users into a topic configured for appdb.users — silent data
// contamination / cross-tenant bleed, and it disagrees with the DSN-scoped
// snapshot. The filter must scope to the handler's configured database.
func TestWatches_SchemaScoped(t *testing.T) {
	h := &MySQLEventHandler{schema: "appdb", tables: lowerAll([]string{"Users"})}

	require.True(t, h.watches("appdb", "users"), "configured database + watched table matches")
	require.True(t, h.watches("APPDB", "Users"), "database match is case-insensitive, like the table match")
	require.False(t, h.watches("otherdb", "users"), "the same table name in another database must NOT match")
	require.False(t, h.watches("appdb", "payments"), "an unwatched table in the configured database does not match")
}

// TestWatches_NoConfiguredSchema documents the fallback: a handler whose
// connection string named no database (empty DSN path — an unusual config the
// DSN-scoped snapshot can't handle either) keeps the table-name-only match
// rather than dropping every row.
func TestWatches_NoConfiguredSchema(t *testing.T) {
	h := &MySQLEventHandler{tables: lowerAll([]string{"Users"})}

	require.True(t, h.watches("anydb", "users"), "no configured database → table match alone")
	require.False(t, h.watches("anydb", "payments"), "still filters by table name")
}
