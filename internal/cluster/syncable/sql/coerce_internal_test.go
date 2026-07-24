package sql

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCoerceForColumn covers the typed-payload → sink-column coercion: a
// JSON-native scalar must be rendered as the form its declared column accepts,
// since the driver will not bridge a number into a text column (or vice versa).
func TestCoerceForColumn(t *testing.T) {
	// A numeric value mapped into a text column binds as its exact digits —
	// this is the regression that broke the CDC round trip (number into a TEXT
	// sink: pgx "cannot find encode plan").
	require.Equal(t, "1", coerceForColumn(json.Number("1"), "TEXT"))
	require.Equal(t, "1", coerceForColumn(json.Number("1"), "VARCHAR(25)"))
	require.Equal(t, "9007199254740993", coerceForColumn(json.Number("9007199254740993"), "TEXT"),
		"json.Number preserves digits beyond float64's exact range")
	require.Equal(t, "100.00", coerceForColumn(json.Number("100.00"), "TEXT"),
		"a decimal's exact source text survives, trailing zeros and all")

	// The same numeric value mapped into a numeric column binds as a native
	// scalar the driver's numeric codec accepts. An integer binds as int64
	// regardless of the column being exact or approximate.
	require.Equal(t, int64(1), coerceForColumn(json.Number("1"), "INTEGER"))
	require.Equal(t, int64(42), coerceForColumn(json.Number("42"), "BIGINT"))
	require.Equal(t, int64(7), coerceForColumn(json.Number("7"), "double precision"))

	// A BIGINT UNSIGNED value above 2^63 is an integer that overflows int64, so
	// Int64 fails — but it must still bind as its exact digits, NOT round through
	// float64. This is the silent MySQL-UNSIGNED corruption the coercion guards
	// against: 10000000000000000001 as a float64 collapses to 1e19 (…000), and
	// 18446744073709551615 (2^64-1) rounds up to …616. The earlier precision
	// tickets only covered values through 2^53+1, which still parse as int64.
	require.Equal(t, "10000000000000000001",
		coerceForColumn(json.Number("10000000000000000001"), "BIGINT UNSIGNED"),
		"a BIGINT UNSIGNED above 2^63 must survive as exact digits, not the 1e19 float64 approximation")
	require.Equal(t, "18446744073709551615",
		coerceForColumn(json.Number("18446744073709551615"), "BIGINT UNSIGNED"),
		"2^64-1 must not round up through float64 to 18446744073709551616")

	// A non-integer into an EXACT-numeric column (DECIMAL/NUMERIC/MONEY) binds as
	// its source digits — a float64 round trip would corrupt a value the type
	// exists to store exactly. This is the sql-syncable-decimal-float64 regression.
	require.Equal(t, "1.5", coerceForColumn(json.Number("1.5"), "NUMERIC(15,2)"))
	require.Equal(t, "7922816251426433.75",
		coerceForColumn(json.Number("7922816251426433.75"), "DECIMAL(30,2)"),
		"a high-precision decimal survives; float64 would round it to 7922816251426434")
	require.Equal(t, "12.34", coerceForColumn(json.Number("12.34"), "MONEY"))

	// A non-integer into an APPROXIMATE column (FLOAT/DOUBLE/REAL) is an IEEE
	// float already, so float64 is its native, correct form.
	require.Equal(t, 1.5, coerceForColumn(json.Number("1.5"), "double precision"))
	require.Equal(t, 2.5, coerceForColumn(json.Number("2.5"), "REAL"))

	// Strings pass through for text columns.
	require.Equal(t, "AMERICA", coerceForColumn("AMERICA", "TEXT"))

	// Booleans: native for a bool column, text for a text column.
	require.Equal(t, true, coerceForColumn(true, "BOOLEAN"))
	require.Equal(t, "true", coerceForColumn(true, "TEXT"))
	require.Equal(t, "false", coerceForColumn(false, "VARCHAR(5)"))

	// An embedded JSON object mapped into a text/json column binds as compact
	// JSON text.
	obj := map[string]any{"a": json.Number("1")}
	require.Equal(t, `{"a":1}`, coerceForColumn(obj, "JSONB"))
	require.Equal(t, "raw", coerceForColumn(json.RawMessage(`raw`), "TEXT"))

	// A base64 payload string mapped into a binary column decodes back to raw
	// bytes and binds as []byte — the form both pgx and go-sql-driver write to a
	// binary column. 0xDEADBEEF ⇄ "3q2+7w==".
	require.Equal(t, []byte{0xDE, 0xAD, 0xBE, 0xEF}, coerceForColumn("3q2+7w==", "BYTEA"))
	require.Equal(t, []byte{0xDE, 0xAD, 0xBE, 0xEF}, coerceForColumn("3q2+7w==", "BLOB"))
	require.Equal(t, []byte("hello"), coerceForColumn("aGVsbG8=", "VARBINARY(32)"))
	// A value that isn't valid base64 — e.g. a legacy mapping sending Postgres's
	// "\x…" hex straight into a bytea column — falls back to text so Postgres's own
	// bytea parser still handles it, unchanged.
	require.Equal(t, `\xdeadbeef`, coerceForColumn(`\xdeadbeef`, "BYTEA"))

	// SQL NULL passes through untouched regardless of column type.
	require.Nil(t, coerceForColumn(nil, "TEXT"))
	require.Nil(t, coerceForColumn(nil, "INTEGER"))
	require.Nil(t, coerceForColumn(nil, "BYTEA"))
}

func TestColumnIsBinary(t *testing.T) {
	for _, ty := range []string{
		"BYTEA", "bytea", "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB",
		"BINARY(16)", "VARBINARY(255)", "varbinary",
	} {
		require.Truef(t, columnIsBinary(ty), "%q should be binary", ty)
	}
	for _, ty := range []string{
		"TEXT", "VARCHAR(25)", "INT", "JSON", "JSONB", "UUID", "", "  ",
	} {
		require.Falsef(t, columnIsBinary(ty), "%q should not be binary", ty)
	}
}

func TestColumnIsNumericOrBool(t *testing.T) {
	for _, ty := range []string{
		"INT", "integer", "INT4", "BIGINT", "smallint", "TINYINT",
		"DECIMAL(15,2)", "NUMERIC(10,0)", "double precision", "REAL",
		"FLOAT", "MONEY", "SERIAL", "BOOL", "BOOLEAN",
	} {
		require.Truef(t, columnIsNumericOrBool(ty), "%q should be numeric/bool", ty)
	}
	for _, ty := range []string{
		"TEXT", "VARCHAR(25)", "CHAR(1)", "CHARACTER VARYING", "UUID",
		"JSON", "JSONB", "DATE", "TIMESTAMP", "TIMESTAMPTZ", "INTERVAL",
		"", "  ",
	} {
		require.Falsef(t, columnIsNumericOrBool(ty), "%q should bind as text", ty)
	}
}

// TestColumnIsApproximateFloat pins the set of columns that bind a non-integer
// value — or an integer too large for int64 — as a native float64. Only the
// approximate IEEE-float types qualify; the integer types and the exact
// DECIMAL/NUMERIC/MONEY family bind their source digits as a string, so an
// int64-overflowing BIGINT UNSIGNED or a high-precision decimal reaches the
// driver uncorrupted.
func TestColumnIsApproximateFloat(t *testing.T) {
	for _, ty := range []string{
		"FLOAT", "float", "DOUBLE", "double precision", "REAL", "FLOAT4", "FLOAT8",
	} {
		require.Truef(t, columnIsApproximateFloat(ty), "%q should be approximate-float", ty)
	}
	for _, ty := range []string{
		"DECIMAL(15,2)", "NUMERIC(10,0)", "DEC(5)", "FIXED", "NUMBER", "MONEY", "numeric",
		"INT", "BIGINT", "TEXT", "", "  ",
	} {
		require.Falsef(t, columnIsApproximateFloat(ty), "%q should not be approximate-float", ty)
	}
}
