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
