package dialects_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

// specialConfig is a config whose table and columns are all identifiers that the
// pre-quoting code interpolated raw and so broke on: a hyphenated table, a
// space-containing column, and a reserved word (select). They must round-trip as
// quoted identifiers, not be mangled or rejected.
func specialConfig() *sql.Config {
	return &sql.Config{
		Table: "Order-Items",
		Mappings: []sql.Mapping{
			{Column: "User Name", SQLType: "VARCHAR(64)"},
			{Column: "select", SQLType: "INT"},
		},
		Indexes:    []sql.Index{{IndexName: "by-name", ColumnNames: "User Name, select"}},
		PrimaryKey: "User Name",
		KeyColumn:  "User Name",
	}
}

// TestPostgres_QuotesSpecialIdentifiers proves every identifier position in the
// PostgreSQL write path (DDL, upsert, delete) quotes a config identifier that
// would otherwise be a syntax error or resolve to the wrong (case-folded) name.
func TestPostgres_QuotesSpecialIdentifiers(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	cfg := specialConfig()

	ddl := d.CreateDDL(cfg)
	require.Contains(t, ddl, `CREATE TABLE IF NOT EXISTS "Order-Items"`)
	require.Contains(t, ddl, `"User Name" VARCHAR(64)`)
	require.Contains(t, ddl, `"select" INT`)
	require.Contains(t, ddl, `PRIMARY KEY ("User Name")`)
	require.Contains(t, ddl, `CREATE INDEX IF NOT EXISTS "by-name" ON "Order-Items" ("User Name","select");`)

	upsert := d.CreateSQL(cfg)
	require.Contains(t, upsert, `INSERT INTO "Order-Items"("User Name","select")`)
	require.Contains(t, upsert, `ON CONFLICT ("User Name") DO UPDATE SET "User Name"=EXCLUDED."User Name","select"=EXCLUDED."select"`)

	del := d.CreateDeleteSQL(cfg)
	require.Equal(t, `DELETE FROM "Order-Items" WHERE "User Name" = $1`, del)
}

// TestMySQL_QuotesSpecialIdentifiers is the MySQL mirror: backtick quoting for the
// same special identifiers.
func TestMySQL_QuotesSpecialIdentifiers(t *testing.T) {
	d := &dialects.MySQLDialect{}
	cfg := specialConfig()

	ddl := d.CreateDDL(cfg)
	require.Contains(t, ddl, "CREATE TABLE IF NOT EXISTS `Order-Items`")
	require.Contains(t, ddl, "`User Name` VARCHAR(64)")
	require.Contains(t, ddl, "`select` INT")
	require.Contains(t, ddl, "PRIMARY KEY (`User Name`)")
	require.Contains(t, ddl, "INDEX `by-name` (`User Name`,`select`)")

	upsert := d.CreateSQL(cfg)
	require.Contains(t, upsert, "INSERT INTO `Order-Items`(`User Name`,`select`)")
	require.Contains(t, upsert, "`User Name`=?,`select`=?")

	del := d.CreateDeleteSQL(cfg)
	require.Equal(t, "DELETE FROM `Order-Items` WHERE `User Name` = ?", del)
}

// TestQuoting_DoublesEmbeddedDelimiter is the injection defense at the identifier
// level: a table/column carrying the dialect's own quote character cannot break
// out of its quotes — the delimiter is doubled.
func TestQuoting_DoublesEmbeddedDelimiter(t *testing.T) {
	pgCfg := &sql.Config{
		Table:      `we"ird`,
		Mappings:   []sql.Mapping{{Column: `c"ol`, SQLType: "TEXT"}},
		PrimaryKey: `c"ol`,
	}
	require.Contains(t, (&dialects.PostgreSQLDialect{}).CreateDDL(pgCfg), `CREATE TABLE IF NOT EXISTS "we""ird" ("c""ol" TEXT`)

	myCfg := &sql.Config{
		Table:      "we`ird",
		Mappings:   []sql.Mapping{{Column: "c`ol", SQLType: "TEXT"}},
		PrimaryKey: "c`ol",
	}
	require.Contains(t, (&dialects.MySQLDialect{}).CreateDDL(myCfg), "CREATE TABLE IF NOT EXISTS `we``ird` (`c``ol` TEXT")
}

// enrichedSpec has an aggregate enrichment whose JSON keys (OnField / Output /
// Source) all contain a single quote — the character that lands inside a '...'
// path/key literal in the materialize subquery. A raw interpolation would let the
// key break out of the literal; the fix doubles the quote.
func enrichedSpec() sql.AggregateSpec {
	return sql.AggregateSpec{
		Table:      "orders",
		PrimaryKey: "id",
		Column:     "items",
		Sidecar:    "orders__items",
		Enrichments: []sql.AggregateEnrichment{{
			Dimension: "orders__lookup_cust",
			OnField:   "cust'id",
			Selects:   []sql.AggregateEnrichmentField{{Output: "na'me", Source: "so'urce"}},
		}},
	}
}

// TestPostgres_EscapesEnrichmentJSONKeyLiterals proves the projection enrichment
// subquery doubles a single quote in every JSON-key literal and quotes the
// dimension/sidecar tables.
func TestPostgres_EscapesEnrichmentJSONKeyLiterals(t *testing.T) {
	got := (&dialects.PostgreSQLDialect{}).CreateAggregateMaterializeSQL(enrichedSpec())

	// The dimension and sidecar are quoted identifiers.
	require.Contains(t, got, `"orders__lookup_cust"`)
	require.Contains(t, got, `"orders__items"`)
	// Every JSON key is escaped inside its literal — the ' is doubled.
	require.Contains(t, got, `->>'cust''id'`)
	require.Contains(t, got, `'na''me'`)
	require.Contains(t, got, `->'so''urce'`)
	// The unescaped breakout ("cust'id' = ...", i.e. a lone ' closing the literal
	// early) must not appear.
	require.NotContains(t, got, `->>'cust'id'`)
}

// TestMySQL_EscapesEnrichmentJSONKeyLiterals is the MySQL mirror: single quotes in
// the '$.key' path literals are doubled and the tables are backtick-quoted.
func TestMySQL_EscapesEnrichmentJSONKeyLiterals(t *testing.T) {
	got := (&dialects.MySQLDialect{}).CreateAggregateMaterializeSQL(enrichedSpec())

	require.Contains(t, got, "`orders__lookup_cust`")
	require.Contains(t, got, "`orders__items`")
	require.Contains(t, got, `->>'$.cust''id'`)
	require.Contains(t, got, `'na''me'`)
	require.Contains(t, got, `'$.so''urce'`)
	require.NotContains(t, got, `->>'$.cust'id'`)
}

// backslashSpec has enrichment JSON keys ending in a backslash — the character
// MySQL (default sql_mode, NO_BACKSLASH_ESCAPES off) treats as an escape inside a
// '...' literal, so a raw `k\` would escape the literal's closing quote and malform
// the subquery. PostgreSQL (standard_conforming_strings=on) treats it literally, so
// the two dialects must escape it differently.
func backslashSpec() sql.AggregateSpec {
	s := enrichedSpec()
	s.Enrichments[0].OnField = `k\`
	s.Enrichments[0].Selects = []sql.AggregateEnrichmentField{{Output: `out\`, Source: `src\`}}
	return s
}

// TestMySQL_EscapesEnrichmentJSONKeyBackslash pins the S5 fix: MySQL must double a
// backslash in every JSON-key literal (not only the single quote), or a
// `\`-terminated key escapes the closing quote of its '$.<key>' literal — malforming
// the subquery, and breaking a legitimate backslash key. Pre-fix (EscapeStringLiteral,
// which leaves `\` untouched) the doubled forms below are absent and this fails.
func TestMySQL_EscapesEnrichmentJSONKeyBackslash(t *testing.T) {
	got := (&dialects.MySQLDialect{}).CreateAggregateMaterializeSQL(backslashSpec())
	require.Contains(t, got, `'$.k\\'`, "OnField backslash must be doubled for MySQL")
	require.Contains(t, got, `'out\\'`, "Output backslash must be doubled for MySQL")
	require.Contains(t, got, `'$.src\\'`, "Source backslash must be doubled for MySQL")
}

// TestPostgres_LeavesEnrichmentBackslashLiteral pins the dialect difference: with
// standard_conforming_strings a backslash is literal, so the PG form must NOT double
// it — doubling would corrupt a legitimate backslash key.
func TestPostgres_LeavesEnrichmentBackslashLiteral(t *testing.T) {
	got := (&dialects.PostgreSQLDialect{}).CreateAggregateMaterializeSQL(backslashSpec())
	require.Contains(t, got, `->>'k\'`, "PG must leave the backslash literal (single)")
	require.NotContains(t, got, `->>'k\\'`, "PG must NOT double the backslash")
}

// TestAffectedParents_EscapesOnFieldLiteral covers the other JSON-key literal
// site — the dimension-change fan-out query.
func TestAffectedParents_EscapesOnFieldLiteral(t *testing.T) {
	spec := enrichedSpec()
	pg := (&dialects.PostgreSQLDialect{}).CreateAggregateAffectedParentsSQL(spec, "cust'id")
	require.Contains(t, pg, `element->>'cust''id'`)
	require.Contains(t, pg, `FROM "orders__items"`)

	my := (&dialects.MySQLDialect{}).CreateAggregateAffectedParentsSQL(spec, "cust'id")
	require.Contains(t, my, `element->>'$.cust''id'`)
	require.Contains(t, my, "FROM `orders__items`")
}
