package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWatches_CaseInsensitive is the B1 regression: a mixed-case `tables` config
// must still match the binlog's table name (which arrives in the source's stored
// case), or the row filter drops every streamed change — total, silent CDC data
// loss after the initial snapshot. Exercises the full fix: resolveTableRefs
// lowercases at construction and watches matches case-insensitively.
func TestWatches_CaseInsensitive(t *testing.T) {
	h := &MySQLEventHandler{tableRefs: resolveTableRefs([]string{"Users", "OrderItems"}, "appdb")}

	require.True(t, h.watches("appdb", "Users"), "exact config case matches")
	require.True(t, h.watches("appdb", "users"), "lowercase binlog name matches the uppercase config")
	require.True(t, h.watches("appdb", "ORDERITEMS"), "any case matches")
	require.False(t, h.watches("appdb", "payments"), "an unwatched table does not match")
}

// TestWatches_SchemaScoped is the cross-database-contamination regression:
// MySQL's binlog is server-wide, so a bare table-name filter would ingest
// otherdb.users into a topic configured for appdb.users — silent data
// contamination / cross-tenant bleed, and it disagrees with the DSN-scoped
// snapshot. A bare `tables` entry scopes to the handler's configured database.
func TestWatches_SchemaScoped(t *testing.T) {
	h := &MySQLEventHandler{tableRefs: resolveTableRefs([]string{"Users"}, "appdb")}

	require.True(t, h.watches("appdb", "users"), "configured database + watched table matches")
	require.True(t, h.watches("APPDB", "Users"), "database match is case-insensitive, like the table match")
	require.False(t, h.watches("otherdb", "users"), "the same table name in another database must NOT match")
	require.False(t, h.watches("appdb", "payments"), "an unwatched table in the configured database does not match")
}

// TestWatches_NoConfiguredSchema documents the fallback: a bare entry whose
// connection string named no database (empty DSN path — an unusual config the
// DSN-scoped snapshot can't handle either) keeps the table-name-only match rather
// than dropping every row.
func TestWatches_NoConfiguredSchema(t *testing.T) {
	h := &MySQLEventHandler{tableRefs: resolveTableRefs([]string{"Users"}, "")}

	require.True(t, h.watches("anydb", "users"), "no configured database → table match alone")
	require.False(t, h.watches("anydb", "payments"), "still filters by table name")
}

// TestWatches_SchemaQualified pins schema-qualified `tables` support: a
// `schema.table` entry matches that table only in its own schema, independent of
// (and even outside) the DSN's default database.
func TestWatches_SchemaQualified(t *testing.T) {
	h := &MySQLEventHandler{tableRefs: resolveTableRefs([]string{"shop.Widget", "Users"}, "appdb")}

	require.True(t, h.watches("shop", "widget"), "qualified entry matches its own schema, case-insensitively")
	require.True(t, h.watches("SHOP", "Widget"), "schema and table match are both case-insensitive")
	require.False(t, h.watches("appdb", "widget"), "qualified entry does NOT match the DSN database")
	require.False(t, h.watches("otherschema", "widget"), "qualified entry does NOT match a third schema")
	require.True(t, h.watches("appdb", "users"), "a bare entry alongside it still scopes to the DSN database")
	require.False(t, h.watches("shop", "users"), "the bare entry does not leak into the qualified schema")
}

// TestResolveTableRefs pins the resolution: bare entries take the DSN database,
// qualified entries keep their own schema, an empty DSN database leaves a bare
// entry schema-less (matches any schema), and everything is lowercased.
func TestResolveTableRefs(t *testing.T) {
	refs := resolveTableRefs([]string{"Users", "Shop.Widget"}, "appdb")
	require.Equal(t, []tableRef{
		{schema: "appdb", table: "users"},
		{schema: "shop", table: "widget"},
	}, refs)

	bareNoDB := resolveTableRefs([]string{"Users"}, "")
	require.Equal(t, []tableRef{{schema: "", table: "users"}}, bareNoDB,
		"a bare entry with no DSN database resolves to an empty schema (matches any)")
}

func TestSplitQualifiedTable(t *testing.T) {
	s, tbl := splitQualifiedTable("shop.widget")
	require.Equal(t, "shop", s)
	require.Equal(t, "widget", tbl)

	s, tbl = splitQualifiedTable("widget")
	require.Equal(t, "", s, "a bare entry has no schema qualifier")
	require.Equal(t, "widget", tbl)
}
