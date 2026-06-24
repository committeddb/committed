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
		{"invalid json -> string", `{not json`, sql.CatJSON, `"{not json"`},
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
