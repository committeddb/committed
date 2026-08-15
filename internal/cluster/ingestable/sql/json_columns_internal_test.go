package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// applyJSONColumnHints resolves jsonColumns onto mappings case-insensitively
// (the tolerance every column reference gets), works after map-all expansion
// (inferred mappings), and rejects a name matching no mapped column loudly —
// a typo'd hint would otherwise silently leave the column an escaped string,
// the exact failure the hint exists to fix.
func TestApplyJSONColumnHints(t *testing.T) {
	t.Run("marks the matching mapping, case-insensitively", func(t *testing.T) {
		spec := &TopicSpec{
			JSONColumns: []string{"eventdata"},
			Mappings: []Mapping{
				{JsonName: "eventData", SQLColumn: "EventData"},
				{JsonName: "note", SQLColumn: "Note"},
			},
		}
		require.NoError(t, applyJSONColumnHints(spec))
		require.True(t, spec.Mappings[0].JSONHint)
		require.False(t, spec.Mappings[1].JSONHint, "only the named column is hinted")
	})

	t.Run("works on map-all-inferred mappings", func(t *testing.T) {
		spec := &TopicSpec{Tables: []string{"events"}, JSONColumns: []string{"EventData"}}
		require.NoError(t, expandMapAllColumns(spec, map[string][]string{
			"events": {"id", "EventData"},
		}))
		require.NoError(t, applyJSONColumnHints(spec))
		require.False(t, spec.Mappings[0].JSONHint)
		require.True(t, spec.Mappings[1].JSONHint)
	})

	t.Run("unknown column is a loud FieldError", func(t *testing.T) {
		spec := &TopicSpec{
			JSONColumns: []string{"EventDta"}, // typo
			Mappings:    []Mapping{{JsonName: "eventData", SQLColumn: "EventData"}},
		}
		err := applyJSONColumnHints(spec)
		require.Error(t, err)
		require.Contains(t, err.Error(), `"EventDta"`)
		require.Contains(t, err.Error(), "not a mapped column")
	})
}
