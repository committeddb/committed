package stages

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// The field case: SQL Server CDC renders GUID payload fields UPPERCASE
// while the app's serializer writes them lowercase — normalize = "lower"
// on the key declarations folds both renderings onto one key.
func TestStageKeyPathNormalize(t *testing.T) {
	stages := []Stage{{
		Name: "by-guid", From: "events", KeyPath: []string{"$.guid"},
		Normalize: NormalizeLower,
		Reduce:    "aggregate", Emit: []Emit{{Field: "n", Count: true}},
	}}
	require.NoError(t, ValidateShapes(stages))
	g := BuildGraph(stages)
	store := stageStoreForTest(t)

	fold := func(key, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, "events", []byte(key), decodePayload(t, payload))
		}))
	}
	get := func(key string) string {
		var got string
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			v, err := tx.GetOut("by-guid", []byte(key))
			got = string(v)
			return err
		}))
		return got
	}

	// The CDC side renders uppercase, the app side lowercase — one key.
	fold("e1", `{"guid":"ABC-123-DEF"}`)
	fold("e2", `{"guid":"abc-123-def"}`)
	require.Equal(t, `{"n":2}`, get("abc-123-def"), "both renderings fold onto the lowercase key")
	require.Empty(t, get("ABC-123-DEF"), "no uppercase shadow key")
}

// A topic join's normalize covers BOTH sides: the on rendering and the
// topic's entity-key rendering in this join's dimension rows — an
// UPPERCASE-keyed CDC dimension row matches a lowercase reference, and
// the dimension's delete addresses the same normalized key.
func TestTopicJoinNormalize(t *testing.T) {
	stages := []Stage{{
		Name: "billable", From: "pairs", KeyPath: []string{"$.id"},
		Joins: []Join{{
			Topic: "billed", On: []string{"$.pairId"}, Absent: true,
			Normalize: NormalizeLower,
		}},
		Emit: []Emit{{Field: "id", From: "$.id"}},
	}}
	require.NoError(t, ValidateShapes(stages))
	g := BuildGraph(stages)
	store := stageStoreForTest(t)

	fold := func(topic, key, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, topic, []byte(key), decodePayload(t, payload))
		}))
	}
	del := func(topic, key string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicDeleteNow(tx, topic, []byte(key))
		}))
	}
	get := func(key string) string {
		var got string
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			v, err := tx.GetOut("billable", []byte(key))
			got = string(v)
			return err
		}))
		return got
	}

	// The pair references its billed record in lowercase; nothing billed
	// yet — vacuous absence, participates.
	fold("pairs", "p1", `{"id":"p1","pairId":"guid-aa"}`)
	require.NotEmpty(t, get("p1"))

	// The billed record arrives from CDC with an UPPERCASE entity key:
	// presence — the pair retracts (the anti-join's hard half, across
	// the case seam).
	fold("billed", "GUID-AA", `{"state":"posted"}`)
	require.Empty(t, get("p1"), "an UPPERCASE dimension key must suppress a lowercase reference")

	// The billed record is deleted (same uppercase key): heals back in.
	del("billed", "GUID-AA")
	require.NotEmpty(t, get("p1"), "the dimension delete must address the same normalized key")
}

// Admission: only "lower" (or absent) is a valid normalize.
func TestNormalizeValidated(t *testing.T) {
	bad := []Stage{{
		Name: "s", From: "t", KeyPath: []string{"$.id"},
		Normalize: "upper",
		Emit:      []Emit{{Field: "id", From: "$.id"}},
	}}
	err := ValidateShapes(bad)
	require.Error(t, err)
	require.Contains(t, err.Error(), `normalize "upper" is not supported`)

	badJoin := []Stage{{
		Name: "s", From: "t", KeyPath: []string{"$.id"},
		Joins: []Join{{Topic: "d", On: []string{"$.x"}, Normalize: "fold"}},
		Emit:  []Emit{{Field: "id", From: "$.id"}},
	}}
	err = ValidateShapes(badJoin)
	require.Error(t, err)
	require.Contains(t, err.Error(), `normalize "fold" is not supported`)
}
