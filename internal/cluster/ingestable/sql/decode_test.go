package sql_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// TestJSONValue covers the text/typed -> JSON rendering: numbers become JSON
// numbers (exact source text preserved via json.Number), bools become JSON
// bools, json columns embed rather than double-encode, everything else and
// every malformed value falls back to a string, and SQL NULL is JSON null.
func TestJSONValue(t *testing.T) {
	// marshal renders the value as it would land in a payload, so the assertions
	// are on the actual JSON bytes (number vs string vs embedded object).
	marshal := func(v any) string {
		bs, err := json.Marshal(v)
		require.NoError(t, err)
		return string(bs)
	}

	for _, tc := range []struct {
		name string
		raw  any
		cat  sql.JSONCategory
		want string
	}{
		{"int text", "1994", sql.CatNumber, `1994`},
		{"numeric keeps trailing zero", "9.30", sql.CatNumber, `9.30`},
		{"negative", "-5", sql.CatNumber, `-5`},
		{"big integer beyond float64 stays exact", "900719925474099267", sql.CatNumber, `900719925474099267`},
		{"NaN is not a JSON number -> string", "NaN", sql.CatNumber, `"NaN"`},
		{"empty number -> string", "", sql.CatNumber, `""`},
		{"bool t", "t", sql.CatBool, `true`},
		{"bool f", "f", sql.CatBool, `false`},
		{"bool true word", "true", sql.CatBool, `true`},
		{"non-bool in bool column -> string", "maybe", sql.CatBool, `"maybe"`},
		{"json embeds", `{"a":1,"b":[2]}`, sql.CatJSON, `{"a":1,"b":[2]}`},
		// Canonicalization (snapshot==CDC parity): keys sorted recursively,
		// insignificant whitespace removed, array order and exact number text kept.
		{"json canonicalizes key order", `{"zebra":1,"apple":2}`, sql.CatJSON, `{"apple":2,"zebra":1}`},
		{"json canonicalizes nested keys + spacing", `{"b": {"y": 1, "x": 2}}`, sql.CatJSON, `{"b":{"x":2,"y":1}}`},
		{"json preserves big int exactly", `{"n":900719925474099267}`, sql.CatJSON, `{"n":900719925474099267}`},
		// canonicalJSON preserves number TEXT (UseNumber) — it sorts keys but does
		// NOT reformat numbers; a whole double's ".0" survives here (Postgres jsonb
		// precision is preserved). MySQL-snapshot number normalization is separate
		// (CanonicalizeMySQLJSONNumbers) — see TestCanonicalizeMySQLJSONNumbers.
		{"json preserves a whole double's .0", `{"x":100.0}`, sql.CatJSON, `{"x":100.0}`},
		{"json preserves a high-precision decimal", `{"x":1.234567890123456789}`, sql.CatJSON, `{"x":1.234567890123456789}`},
		{"json keeps array order", `{"a":[3,1,2]}`, sql.CatJSON, `{"a":[3,1,2]}`},
		{"invalid json -> string", `{not json`, sql.CatJSON, `"{not json"`},
		{"json with trailing garbage -> string", `{"a":1} x`, sql.CatJSON, `"{\"a\":1} x"`},
		{"text passes through", "Morgan Freeman", sql.CatText, `"Morgan Freeman"`},
		{"number text in text column stays string", "1", sql.CatText, `"1"`},
		{"[]byte numeric", []byte("42"), sql.CatNumber, `42`},
		{"already-typed int64 passes through", int64(7), sql.CatNumber, `7`},
		{"already-typed bool passes through", true, sql.CatBool, `true`},
		{"already-typed float passes through", 9.3, sql.CatNumber, `9.3`},
		{"nil is null", nil, sql.CatNumber, `null`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, marshal(sql.JSONValue(tc.raw, tc.cat)))
		})
	}
}

// TestCanonicalizeMySQLJSONNumbers pins the MySQL-snapshot-only number
// normalization: float-syntax numbers (whole doubles, exponents) collapse to Go's
// float form (matching the go-mysql CDC path); integers stay exact; invalid input
// passes through. Deliberately NOT applied to Postgres (jsonb precision preserved).
func TestCanonicalizeMySQLJSONNumbers(t *testing.T) {
	for _, tc := range []struct{ name, in, want string }{
		{"whole double -> int", `{"x":100.0}`, `{"x":100}`},
		{"negative zero", `{"x":-0.0}`, `{"x":-0}`},
		{"exponent expands", `{"x":1e2}`, `{"x":100}`},
		{"real decimal kept", `{"x":1.5}`, `{"x":1.5}`},
		{"big integer exact (no float rounding)", `{"x":900719925474099267}`, `{"x":900719925474099267}`},
		{"nested + array normalized", `{"a":{"b":2.0},"c":[3.0,4]}`, `{"a":{"b":2},"c":[3,4]}`},
		{"invalid passes through", `{not json`, `{not json`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, string(sql.CanonicalizeMySQLJSONNumbers([]byte(tc.in))))
		})
	}
}
