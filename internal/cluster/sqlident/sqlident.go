// Package sqlident quotes and validates SQL identifiers and free-text type
// expressions for committed's destination-SQL paths (the syncable write path and
// the ingest read path). A config-supplied table/column name with a hyphen, space,
// mixed case, reserved word, or even an embedded quote character is rendered safely
// instead of interpolated raw into DDL/DML.
//
// It is a dependency-free leaf package (it imports no committed code) so both the
// syncable dialects and the ingest dialects can share one seam without an import
// cycle. Row VALUES are always bound as parameters ($N / ?); this package handles
// the identifier and type positions that cannot be parameterized.
package sqlident

import "strings"

// Quoter renders identifiers for one SQL dialect. PostgreSQL delimits identifiers
// with double quotes and escapes an embedded " by doubling it; MySQL uses backticks
// and doubles an embedded `. Doubling the delimiter is what stops a name from
// breaking out of its quotes.
type Quoter struct{ delim byte }

var (
	// Postgres quotes identifiers with "..." (doubling an embedded ").
	Postgres = Quoter{delim: '"'}
	// MySQL quotes identifiers with `...` (doubling an embedded `).
	MySQL = Quoter{delim: '`'}
)

// Ident quotes a single identifier, doubling any embedded delimiter. Promoted from
// the ingest-side quoteIdent so the write and read paths escape identically.
func (q Quoter) Ident(s string) string {
	d := string(q.delim)
	return d + strings.ReplaceAll(s, d, d+d) + d
}

// Table quotes a possibly schema-qualified table name, quoting each dot-separated
// part ("schema.orders" -> "schema"."orders"). A bare name has no dot and quotes as
// one identifier. Promoted from the ingest-side quoteTable.
func (q Quoter) Table(s string) string {
	parts := strings.Split(s, ".")
	for i, p := range parts {
		parts[i] = q.Ident(p)
	}
	return strings.Join(parts, ".")
}

// Columns quotes a comma-separated identifier list ("a, b" -> "a","b"), trimming
// surrounding whitespace on each element. Used for composite index column lists
// (SchemaIndex.Columns is a comma-separated list).
func (q Quoter) Columns(s string) string {
	parts := strings.Split(s, ",")
	for i, p := range parts {
		parts[i] = q.Ident(strings.TrimSpace(p))
	}
	return strings.Join(parts, ",")
}

// EscapeStringLiteral doubles single quotes so s can be safely embedded inside a
// '...' SQL string literal — e.g. a projection enrichment's JSON key/path, which
// pgAggSubquery interpolates into `->>'...'`. This is the PostgreSQL form: with
// the default standard_conforming_strings=on a backslash is literal, so doubling
// the single quote is sufficient. MySQL's default sql_mode treats backslash as an
// escape inside a literal, so it needs EscapeStringLiteralMySQL — do NOT use this
// for MySQL. It does NOT add the surrounding quotes (the caller owns the literal's
// delimiters and any JSON-path prefix).
func EscapeStringLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// EscapeStringLiteralMySQL is the MySQL form of EscapeStringLiteral. Under MySQL's
// default sql_mode (NO_BACKSLASH_ESCAPES off) a backslash is an escape character
// inside a '...' literal, so a value ending in '\' would escape the closing quote
// and run the literal on — malforming the surrounding SQL (e.g. mysqlAggSubquery's
// `->>'$.<key>'`), and breaking a legitimate JSON key that merely contains a
// backslash. Doubling both '\' and ' contains any value. The two replacements are
// independent (neither introduces a character the other operates on), so order
// does not matter. Like EscapeStringLiteral it does not add the surrounding quotes.
func EscapeStringLiteralMySQL(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	return strings.ReplaceAll(s, "'", "''")
}

// ValidIdent reports whether s is acceptable as a config-supplied SQL identifier:
// non-empty after trimming and free of NUL and other control characters, which are
// never legitimate in a table/column name and quote unreliably across drivers.
// Printable specials — hyphen, space, reserved words, mixed case, even a quote
// character — are ACCEPTED because Quoter renders them safely; this matches the
// ingest side's acceptance. It is a POST-time gate that turns garbage into a clean
// 400 rather than a deferred driver error.
func ValidIdent(s string) bool {
	if strings.TrimSpace(s) == "" {
		return false
	}
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// ValidTypeExpr reports whether s is acceptable as a free-text SQL column type
// expression. Unlike an identifier a type cannot be quoted (it is a keyword phrase
// such as VARCHAR(255) or TIMESTAMP WITH TIME ZONE), so it is validated by a
// conservative charset — letters, digits, spaces, underscore, parentheses and
// commas — which admits every standard type while rejecting the quote, semicolon,
// and comment metacharacters an injection needs. Empty (after trim) is invalid.
func ValidTypeExpr(s string) bool {
	if strings.TrimSpace(s) == "" {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == ' ', r == '_', r == '(', r == ')', r == ',':
		default:
			return false
		}
	}
	return true
}
