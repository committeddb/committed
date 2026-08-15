package sql

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBuildEntityJSON_MixedCaseColumnResolves is the mixed-case-column
// regression: every decode path keys its value/category maps by LOWERCASED column
// name, so a mapping whose configured column is mixed-case (column = "CreatedAt",
// as Postgres quoted identifiers and case-sensitive MySQL produce) must still
// resolve the value — not silently emit a null field.
func TestBuildEntityJSON_MixedCaseColumnResolves(t *testing.T) {
	mappings := []Mapping{
		{JsonName: "createdAt", SQLColumn: "CreatedAt"}, // quoted CamelCase source column
		{JsonName: "id", SQLColumn: "ID"},
	}
	// The decode maps as every ingest path builds them: lowercased keys.
	values := map[string]any{"createdat": "2026-07-08T00:00:00Z", "id": "42"}
	cats := map[string]JSONCategory{"createdat": CatText, "id": CatNumber}

	out := BuildEntityJSON(mappings, values, cats)

	require.Equal(t, "2026-07-08T00:00:00Z", out["createdAt"],
		"a mixed-case configured column must resolve against the lowercased decode map, not emit null")
	require.Equal(t, json.Number("42"), out["id"], "category is honored through the case-insensitive lookup")
}

// The JSON-column hint: a source string column holding JSON (SQL Server
// nvarchar, MySQL varchar — undetectable as JSON from type metadata) decodes
// as a real, canonicalized JSON value when its mapping carries the hint, so
// every downstream consumer gets structure (projection jsonPaths traverse
// it) instead of one escaped string. Invalid JSON in a hinted column falls
// back to the string per the CatJSON contract — never an invalid payload —
// and unhinted columns are untouched.
func TestBuildEntityJSON_JSONHintOverridesCategory(t *testing.T) {
	mappings := []Mapping{
		{JsonName: "eventData", SQLColumn: "EventData", JSONHint: true},
		{JsonName: "note", SQLColumn: "Note"},
		{JsonName: "broken", SQLColumn: "Broken", JSONHint: true},
	}
	values := map[string]any{
		"eventdata": `{"z":1,"a":{"n":2.50}}`,
		"note":      `{"looks":"like json but unhinted"}`,
		"broken":    `{not json`,
	}
	cats := map[string]JSONCategory{"eventdata": CatText, "note": CatText, "broken": CatText}

	out := BuildEntityJSON(mappings, values, cats)

	bs, err := json.Marshal(out["eventData"])
	require.NoError(t, err)
	require.JSONEq(t, `{"a":{"n":2.50},"z":1}`, string(bs),
		"a hinted column decodes as real JSON, canonicalized (sorted keys, exact numbers)")
	require.Equal(t, `{"looks":"like json but unhinted"}`, out["note"],
		"an unhinted string column stays a string even when it looks like JSON")
	require.Equal(t, `{not json`, out["broken"],
		"invalid JSON in a hinted column falls back to the string — never an invalid payload")
}
