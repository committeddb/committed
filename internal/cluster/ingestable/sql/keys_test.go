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

func TestCompositeKey_CaseInsensitiveColumnLookup(t *testing.T) {
	// The decode map is keyed by lowercased column names, so a primaryKey
	// configured in another case (primaryKey = "ID" vs column id) must still
	// resolve to the value — not miss and collapse every row onto key "<nil>".
	m := map[string]any{"id": 42} // decode map: lowercased key
	require.Equal(t, "42", sql.CompositeKey(m, []string{"ID"}))
	require.Equal(t, "42", sql.CompositeKey(m, []string{"Id"}))

	// Composite key, mixed case on both columns.
	mc := map[string]any{"tconst": "tt1", "ordering": "2"}
	require.Equal(t, `["tt1","2"]`, sql.CompositeKey(mc, []string{"TConst", "Ordering"}))
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

func TestCompositeKey_BinaryValuesNeverCollide(t *testing.T) {
	// A BINARY/VARBINARY/BLOB column value with non-UTF-8 bytes must not collapse
	// distinct tuples onto one key. json.Marshal replaces every invalid byte with
	// U+FFFD, so "\xff\xfe" and "\xfe\xff" both mangled to the same string —
	// silent row loss (one overwrites the other; a delete tombstones the wrong key).
	cols := []string{"tenant", "blob"}
	k1 := sql.CompositeKey(map[string]any{"tenant": "acme", "blob": "\xff\xfe"}, cols)
	k2 := sql.CompositeKey(map[string]any{"tenant": "acme", "blob": "\xfe\xff"}, cols)
	require.NotEqual(t, k1, k2, "distinct binary composite values must not collide")
}

func TestDecodeCompositeCursor_BinaryRoundTrip(t *testing.T) {
	// A resume cursor built from a binary composite key must recover the EXACT
	// bytes, or the (c1,c2) > (?,?) keyset boundary is wrong and a mid-snapshot
	// restart skips or duplicates rows.
	cols := []string{"tenant", "blob"}
	raw := "\x00\xff\x10\xfe\x7f" // non-UTF-8, includes NUL and high bytes
	key := sql.CompositeKey(map[string]any{"tenant": "acme", "blob": raw}, cols)
	got, err := sql.DecodeCompositeCursor(key, len(cols))
	require.NoError(t, err)
	require.Equal(t, []string{"acme", raw}, got, "binary composite cursor must round-trip byte-for-byte")
}

func TestCompositeKey_TextEncodingUnchanged(t *testing.T) {
	// The fix must NOT change the bytes of an existing text/number composite key
	// (all values valid UTF-8) — a changed encoding would re-key every row
	// downstream. It stays the JSON-array form and round-trips.
	cols := []string{"tconst", "ordering"}
	key := sql.CompositeKey(map[string]any{"tconst": "tt0111161", "ordering": "2"}, cols)
	require.Equal(t, `["tt0111161","2"]`, key)
	got, err := sql.DecodeCompositeCursor(key, len(cols))
	require.NoError(t, err)
	require.Equal(t, []string{"tt0111161", "2"}, got)
}

func TestDecodeCompositeCursor_Errors(t *testing.T) {
	_, err := sql.DecodeCompositeCursor("not json", 2)
	require.Error(t, err)

	_, err = sql.DecodeCompositeCursor(`["only-one"]`, 2)
	require.Error(t, err, "arity mismatch must error rather than silently page wrong")
}

func TestDecodeCompositeCursor_ErrorOmitsPKValue(t *testing.T) {
	// The cursor IS the composite primary key = PII. A decode error bubbles into a
	// Warn log on snapshot retry/reconnect, so its message must not echo the value —
	// only a non-PII classifier (arity numbers, the parse error without the input).
	const pii = "ssn-123-45-6789"

	// Arity mismatch — the realistic trigger: primaryKey arity changed via re-POST
	// mid-snapshot, so a 2-value cursor is decoded expecting 3.
	_, err := sql.DecodeCompositeCursor(`["`+pii+`","secret"]`, 3)
	require.Error(t, err)
	require.NotContains(t, err.Error(), pii, "arity-mismatch error must not echo the PK cursor value")

	// Malformed cursor.
	_, err = sql.DecodeCompositeCursor(`["`+pii+`" bad`, 2)
	require.Error(t, err)
	require.NotContains(t, err.Error(), pii, "decode error must not echo the PK cursor value")
}
