package sql_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	sql "github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

func TestCompositeKey_SingleColumnIsBareValue(t *testing.T) {
	// Single-column keys are unchanged from the original behavior so existing
	// entity keys stay byte-stable.
	require.Equal(t, "tt0111161", sql.CompositeKey(map[string]any{"tconst": "tt0111161"}, []string{"tconst"}))
	require.Equal(t, "5", sql.CompositeKey(map[string]any{"pk": 5}, []string{"pk"}))
}

func TestCompositeKey_MultiColumnIsJSONArray(t *testing.T) {
	m := map[string]any{"tconst": "tt0111161", "ordering": "2"}
	require.Equal(t, `["tt0111161","2"]`, sql.CompositeKey(m, []string{"tconst", "ordering"}))
}

func TestCompositeKey_DistinctRowsNeverAlias(t *testing.T) {
	// Two principals of the same movie get distinct keys (the bug this fixes:
	// keyed by tconst alone they collided and one was silently dropped).
	k1 := sql.CompositeKey(map[string]any{"tconst": "tt0111161", "ordering": "1"}, []string{"tconst", "ordering"})
	k2 := sql.CompositeKey(map[string]any{"tconst": "tt0111161", "ordering": "2"}, []string{"tconst", "ordering"})
	require.NotEqual(t, k1, k2)

	// And the classic delimiter-aliasing pair ("x|"+"y" vs "x"+"|y") stays
	// distinct because JSON quotes each value.
	a := sql.CompositeKey(map[string]any{"a": "x|", "b": "y"}, []string{"a", "b"})
	b := sql.CompositeKey(map[string]any{"a": "x", "b": "|y"}, []string{"a", "b"})
	require.NotEqual(t, a, b)
}

func TestDecodeCompositeCursor_RoundTrip(t *testing.T) {
	// Single column: the cursor is the bare value.
	got, err := sql.DecodeCompositeCursor("tt0468569", 1)
	require.Nil(t, err)
	require.Equal(t, []string{"tt0468569"}, got)

	// Composite: the cursor decodes back to the per-column boundary values.
	cols := []string{"tconst", "ordering"}
	key := sql.CompositeKey(map[string]any{"tconst": "tt0111161", "ordering": "2"}, cols)
	got, err = sql.DecodeCompositeCursor(key, len(cols))
	require.Nil(t, err)
	require.Equal(t, []string{"tt0111161", "2"}, got)
}

func TestDecodeCompositeCursor_Errors(t *testing.T) {
	_, err := sql.DecodeCompositeCursor("not json", 2)
	require.Error(t, err)

	_, err = sql.DecodeCompositeCursor(`["only-one"]`, 2)
	require.Error(t, err, "arity mismatch must error rather than silently page wrong")
}
