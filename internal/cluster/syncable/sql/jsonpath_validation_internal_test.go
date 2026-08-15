package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestValidateMappings_RejectsInvalidJSONPath: a plain syncable mapping with a
// syntactically-invalid (or empty) jsonPath is rejected at parse, not left to
// dead-letter every Actual of the topic once the worker runs.
func TestValidateMappings_RejectsInvalidJSONPath(t *testing.T) {
	require.NoError(t, validateMappings([]Mapping{{JsonPath: "$.wid", Column: "wid", SQLType: "VARCHAR(64)"}}))
	require.NoError(t, validateMappings([]Mapping{{JsonPath: "$", Column: "doc", SQLType: "JSON"}}))

	for _, bad := range []string{"", "$.[bad", "not a path"} {
		err := validateMappings([]Mapping{{JsonPath: bad, Column: "wid", SQLType: "VARCHAR(64)"}})
		require.Error(t, err, bad)
		require.Contains(t, err.Error(), "invalid jsonpath")
	}
}

// TestValidateProjectionJSONPaths pins that every configured jsonpath position is
// compiled at parse — keyPath, when (source + rule), set from, aggregate
// elementKey and element from, lookup field from — while a value/null set and an
// enriched field (no from) are legitimately skipped.
func TestValidateProjectionJSONPaths(t *testing.T) {
	valid := &ProjectionConfig{Sources: []ProjectionSource{{
		Topic:   "e",
		KeyPath: []string{"$.id"},
		When:    []WhenClause{{Path: "$.type"}},
		Rules: []ProjectionRule{{
			When: []WhenClause{{Path: "$.sub"}},
			Set:  []ProjectionSet{{Column: "v", From: "$.v"}, {Column: "n", Null: true}},
		}},
	}}}
	require.NoError(t, validateProjectionJSONPaths(valid), "all-valid paths (and a null set) must pass")

	for _, tc := range []struct {
		name string
		src  ProjectionSource
	}{
		{"keyPath", ProjectionSource{Topic: "e", KeyPath: []string{"$.[bad"}}},
		{"source when", ProjectionSource{Topic: "e", When: []WhenClause{{Path: "$.[bad"}}}},
		{"rule when", ProjectionSource{Topic: "e", Rules: []ProjectionRule{{When: []WhenClause{{Path: "$.[bad"}}}}}},
		{"set from", ProjectionSource{Topic: "e", Rules: []ProjectionRule{{Set: []ProjectionSet{{Column: "v", From: "$.[bad"}}}}}},
		{"aggregate elementKey", ProjectionSource{Topic: "e", Aggregate: &ProjectionAggregate{ElementKey: "$.[bad"}}},
		{"aggregate element from", ProjectionSource{Topic: "e", Aggregate: &ProjectionAggregate{Element: []ProjectionElementField{{Field: "f", From: "$.[bad"}}}}},
		{"lookup field from", ProjectionSource{Topic: "e", Lookup: &ProjectionLookup{Fields: []ProjectionElementField{{Field: "f", From: "$.[bad"}}}}},
	} {
		t.Run("rejects "+tc.name, func(t *testing.T) {
			err := validateProjectionJSONPaths(&ProjectionConfig{Sources: []ProjectionSource{tc.src}})
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid jsonpath")
		})
	}

	// An enriched element field pulls from a dimension (lookup/on/select) and has
	// no from path — it must be skipped, not rejected for an empty path.
	enriched := &ProjectionConfig{Sources: []ProjectionSource{{Topic: "e", Aggregate: &ProjectionAggregate{
		Element: []ProjectionElementField{{Field: "name", Lookup: "dim", On: "custId", Select: "name"}},
	}}}}
	require.NoError(t, validateProjectionJSONPaths(enriched))
}

// TestValidateProjectionConfig_WiresJSONPathCheck proves the jsonpath pass runs as
// part of the real projection validation entry point, not only in isolation: a
// structurally-valid projection with a syntactically-broken when-path (which the
// structural checks pass, since it is non-empty) is rejected.
func TestValidateProjectionConfig_WiresJSONPathCheck(t *testing.T) {
	c := &ProjectionConfig{
		Topic:      "e",
		Table:      "t",
		PrimaryKey: []string{"id"},
		Columns:    []ProjectionColumn{{Name: "id", SQLType: "VARCHAR(64)"}, {Name: "v", SQLType: "VARCHAR(64)"}},
		Rules: []ProjectionRule{{
			When: []WhenClause{{Path: "$.[bad", Equals: "x"}},
			Set:  []ProjectionSet{{Column: "v", From: "$.v"}},
		}},
	}
	c.applyDefaults()
	err := validateProjectionConfig(c)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid jsonpath")
}
