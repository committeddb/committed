package sqlident_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/sqlident"
)

func TestIdent_QuotesAndDoublesDelimiter(t *testing.T) {
	require.Equal(t, `"orders"`, sqlident.Postgres.Ident("orders"))
	require.Equal(t, "`orders`", sqlident.MySQL.Ident("orders"))

	// Mixed case and hyphen are preserved verbatim inside the quotes (they must
	// work end-to-end, not be mangled or rejected).
	require.Equal(t, `"OrderItems-2024"`, sqlident.Postgres.Ident("OrderItems-2024"))
	require.Equal(t, "`OrderItems-2024`", sqlident.MySQL.Ident("OrderItems-2024"))

	// The injection defense: an embedded delimiter is doubled so the name cannot
	// break out of its quotes.
	require.Equal(t, `"a""b"`, sqlident.Postgres.Ident(`a"b`))
	require.Equal(t, "`a``b`", sqlident.MySQL.Ident("a`b"))

	// The other dialect's delimiter is an ordinary character, not escaped.
	require.Equal(t, "`a\"b`", sqlident.MySQL.Ident(`a"b`))
	require.Equal(t, "\"a`b\"", sqlident.Postgres.Ident("a`b"))
}

func TestTable_QuotesEachSchemaQualifiedPart(t *testing.T) {
	require.Equal(t, `"orders"`, sqlident.Postgres.Table("orders"))
	require.Equal(t, `"public"."orders"`, sqlident.Postgres.Table("public.orders"))
	require.Equal(t, "`analytics`.`orders`", sqlident.MySQL.Table("analytics.orders"))

	// A delimiter embedded in one part is still doubled part-by-part.
	require.Equal(t, `"a""b"."c"`, sqlident.Postgres.Table(`a"b.c`))
}

func TestColumns_QuotesCommaSeparatedListTrimmingSpaces(t *testing.T) {
	require.Equal(t, `"a","b"`, sqlident.Postgres.Columns("a,b"))
	require.Equal(t, `"a","b"`, sqlident.Postgres.Columns("a, b"))
	require.Equal(t, "`a`,`b`,`c`", sqlident.MySQL.Columns(" a , b ,c "))
	require.Equal(t, `"created-at"`, sqlident.Postgres.Columns("created-at"))
}

func TestEscapeStringLiteral_DoublesSingleQuotes(t *testing.T) {
	require.Equal(t, "plain", sqlident.EscapeStringLiteral("plain"))
	require.Equal(t, "O''Brien", sqlident.EscapeStringLiteral("O'Brien"))
	// A JSON-key breakout attempt: the closing quote is neutralized by doubling.
	require.Equal(t, "x'','' ; DROP", sqlident.EscapeStringLiteral("x',' ; DROP"))
	// The PostgreSQL form leaves a backslash literal (standard_conforming_strings=on):
	// doubling it would corrupt a legitimate backslash-bearing key.
	require.Equal(t, `k\`, sqlident.EscapeStringLiteral(`k\`))
}

func TestEscapeStringLiteralMySQL_DoublesBackslashAndQuote(t *testing.T) {
	require.Equal(t, "plain", sqlident.EscapeStringLiteralMySQL("plain"))
	require.Equal(t, "O''Brien", sqlident.EscapeStringLiteralMySQL("O'Brien"))
	// Under MySQL's default sql_mode a trailing backslash would escape the closing
	// quote of the surrounding literal; doubling it keeps the value contained.
	require.Equal(t, `k\\`, sqlident.EscapeStringLiteralMySQL(`k\`))
	// Mixed backslash + quote: both are doubled (the two passes are independent).
	require.Equal(t, `a\\''b`, sqlident.EscapeStringLiteralMySQL(`a\'b`))
}

func TestValidIdent(t *testing.T) {
	// Accepted: the special-but-legitimate names that must work end-to-end.
	for _, s := range []string{"orders", "Order-Items", "created at", "select", `a"b`, "a`b", "café"} {
		require.Truef(t, sqlident.ValidIdent(s), "%q should be a valid identifier", s)
	}
	// Rejected: empty/whitespace and control characters.
	for _, s := range []string{"", "   ", "a\x00b", "a\nb", "a\tb", "a\x7fb"} {
		require.Falsef(t, sqlident.ValidIdent(s), "%q should be an invalid identifier", s)
	}
}

func TestValidTypeExpr(t *testing.T) {
	// Accepted: every standard type shape.
	for _, s := range []string{
		"INT", "text", "VARCHAR(255)", "NUMERIC(10,2)", "NUMERIC(10, 2)",
		"TIMESTAMP WITH TIME ZONE", "DOUBLE PRECISION", "bigint",
	} {
		require.Truef(t, sqlident.ValidTypeExpr(s), "%q should be a valid type", s)
	}
	// Rejected: the metacharacters an injection needs, plus empty.
	for _, s := range []string{
		"", "  ", "INT; DROP TABLE x", "TEXT'--", `VARCHAR(255)"`,
		"INT`", "enum('a','b')", "INT--comment", "money$",
	} {
		require.Falsef(t, sqlident.ValidTypeExpr(s), "%q should be an invalid type", s)
	}
}
