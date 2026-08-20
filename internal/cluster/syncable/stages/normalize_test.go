package stages

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

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

// A forEach path resolving to a PRESENT non-array fans zero elements —
// silently correct for one foreign event, catastrophically silent as a
// steady state (the field case: a serialized-JSON string column, two
// full 36M replays of plausible empty output). A long consecutive run
// warns exactly once, naming the stage, the path, and the remedy; an
// array-shaped input resets the run.
func TestStageForEachNonArrayWarns(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	prev := zap.L()
	zap.ReplaceGlobals(zap.New(core))
	defer zap.ReplaceGlobals(prev)

	stages := []Stage{{
		Name: "fan", From: "txns", ForEach: "$.EventData.elements[*]",
		KeyPath: []string{"$.wa"}, ElementKey: "$.id",
		Reduce: "aggregate", Emit: []Emit{{Field: "n", Count: true}},
	}}
	require.NoError(t, ValidateShapes(stages))
	g := BuildGraph(stages)
	store := stageStoreForTest(t)

	fold := func(key, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, "txns", []byte(key), decodePayload(t, payload))
		}))
	}

	// 999 string-valued EventData payloads: under the threshold, silent.
	for i := 0; i < 999; i++ {
		fold(fmt.Sprintf("t%d", i), `{"EventData":"{\"elements\":[{\"id\":\"e1\",\"wa\":\"w1\"}]}"}`)
	}
	require.Zero(t, logs.FilterMessageSnippet("non-array").Len())

	// One array-shaped input resets the run — an EMPTY array is healthy
	// too (the container probe tells it apart from a string traversal)...
	fold("ok0", `{"EventData":{"elements":[]}}`)
	fold("ok1", `{"EventData":{"elements":[{"id":"e9","wa":"w9"}]}}`)
	// ...so 999 more misses stay under the threshold again.
	for i := 0; i < 999; i++ {
		fold(fmt.Sprintf("u%d", i), `{"EventData":"{}"}`)
	}
	require.Zero(t, logs.FilterMessageSnippet("non-array").Len(), "a healthy input mid-run resets the counter")

	// The 1000th consecutive miss fires the warning exactly once.
	fold("u999", `{"EventData":"{}"}`)
	warns := logs.FilterMessageSnippet("forEach path has not resolved to an array").All()
	require.Len(t, warns, 1)
	require.Equal(t, "fan", warns[0].ContextMap()["stage"])
	require.Equal(t, "unresolved", warns[0].ContextMap()["value_type"])
}

// The jti-b4 field defect, pinned: a fan-then-fold stage with normalized
// keys, anti-joined by a stage-fed consumer whose referencing values
// carry the OTHER rendering (UPPERCASE CDC GUIDs) and whose join —
// correctly — declares nothing. The join must inherit the producer's
// normalize, or the reference silently never matches and the anti-join
// suppresses nothing (three full 36M replays' worth of superset rows).
func TestStageJoinInheritsProducerNormalize(t *testing.T) {
	stages := []Stage{
		{
			Name: "billed-pairs", From: "txn-events",
			ForEach: "$.EventData.elements[*]", ElementKey: "$.Id",
			KeyPath:   []string{"$parent.JobId", "$.WorkAreaId"},
			Normalize: NormalizeLower,
			Reduce:    "aggregate", Emit: []Emit{{Field: "n", Count: true}},
		},
		{
			Name: "completed", From: "visits", KeyPath: []string{"$.jobId", "$.waId"},
			Normalize: NormalizeLower,
			Emit:      []Emit{{Field: "jobId", From: "$.jobId"}, {Field: "waId", From: "$.waId"}},
		},
		{
			Name: "unbilled", From: "completed", KeyPath: []string{"$.jobId", "$.waId"},
			Normalize: NormalizeLower,
			Joins:     []Join{{From: "billed-pairs", On: []string{"$.jobId", "$.waId"}, Absent: true}},
			Emit:      []Emit{{Field: "j", From: "$.jobId"}},
		},
	}
	require.NoError(t, ValidateShapes(stages))
	g := BuildGraph(stages)
	store := stageStoreForTest(t)

	fold := func(topic, key, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, topic, []byte(key), decodePayload(t, payload))
		}))
	}
	get := func(stage, key string) string {
		var got string
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			v, err := tx.GetOut(stage, []byte(key))
			got = string(v)
			return err
		}))
		return got
	}
	pair := OutKey([]string{"j-1", "w-1"})

	// The completed pair arrives with UPPERCASE values; nothing billed —
	// participates.
	fold("visits", "v1", `{"jobId":"J-1","waId":"W-1"}`)
	require.NotEmpty(t, get("unbilled", pair))

	// The billing element arrives (also UPPERCASE payload, lowered by the
	// producer's normalize): the anti-join must suppress — the inherited
	// normalize renders the UPPERCASE reference into the producer's key
	// space.
	fold("txn-events", "t1", `{"JobId":"J-1","EventData":{"elements":[{"Id":"e1","WorkAreaId":"W-1","perVisitCharge":25}]}}`)
	require.NotEmpty(t, get("billed-pairs", pair), "the fan-then-fold produces the pair")
	require.Empty(t, get("unbilled", pair), "billed-pair arrival suppresses across the case seam")

	// The transaction deletes: billing retracts, the pair heals back in —
	// phase 4's centerpiece trigger, both directions.
	require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
		return g.FoldTopicDeleteNow(tx, "txn-events", []byte("t1"))
	}))
	require.NotEmpty(t, get("unbilled", pair), "billing retraction un-suppresses")

	// And the graph never wrote its resolution back into the caller's
	// config (the store fingerprint reads it).
	require.Empty(t, stages[2].Joins[0].Normalize, "inheritance resolves on the graph's copy, not the config")
}

// Declaring normalize on a stage join is an admission error naming the
// right place.
func TestStageJoinNormalizeDeclarationRejected(t *testing.T) {
	stages := []Stage{
		{
			Name: "pairs", From: "billing", KeyPath: []string{"$.job"},
			Normalize: NormalizeLower,
			Emit:      []Emit{{Field: "job", From: "$.job"}},
		},
		{
			Name: "consumer", From: "jobs", KeyPath: []string{"$.id"},
			Joins: []Join{{From: "pairs", On: []string{"$.jobId"}, Normalize: NormalizeLower}},
			Emit:  []Emit{{Field: "id", From: "$.id"}},
		},
	}
	err := ValidateShapes(stages)
	require.Error(t, err)
	require.Contains(t, err.Error(), "normalize is not declarable on a stage join")
	require.Contains(t, err.Error(), `inherits stage "pairs"`)
}

// The number-rendering sibling of the GUID-case bug (jti-b5): the same
// logical pair key arrives as CDC-rendered `5.0000` on one side and
// jsonColumns-decoded `5` on the other. Key identity must canonicalize
// numeric renderings — source digits are exactness for VALUES, but for
// KEYS they are just bytes, and byte-exact comparison silently never
// matches (an anti-join that suppresses nothing, in either direction).
func TestNumericKeyPartsCanonicalize(t *testing.T) {
	stages := []Stage{
		{
			Name: "billed-pairs", From: "billing",
			KeyPath:   []string{"$.job", "$.wa"},
			Normalize: NormalizeLower,
			Reduce:    "aggregate", Emit: []Emit{{Field: "n", Count: true}},
		},
		{
			Name: "unbilled", From: "pairs", KeyPath: []string{"$.jobId", "$.waId"},
			Normalize: NormalizeLower,
			Joins:     []Join{{From: "billed-pairs", On: []string{"$.jobId", "$.waId"}, Absent: true}},
			Emit:      []Emit{{Field: "j", From: "$.jobId"}},
		},
	}
	require.NoError(t, ValidateShapes(stages))
	g := BuildGraph(stages)
	store := stageStoreForTest(t)

	fold := func(topic, key, payload string) {
		require.NoError(t, store.Update(func(tx *stagestore.Tx) error {
			return g.FoldTopicUpsertNow(tx, topic, []byte(key), decodePayload(t, payload))
		}))
	}
	get := func(stage, key string) string {
		var got string
		require.NoError(t, store.View(func(tx *stagestore.Tx) error {
			v, err := tx.GetOut(stage, []byte(key))
			got = string(v)
			return err
		}))
		return got
	}
	pair := OutKey([]string{"guid-a", "5"})

	// The pair references workarea 5 with the app's rendering.
	fold("pairs", "p1", `{"jobId":"GUID-A","waId":5}`)
	require.NotEmpty(t, get("unbilled", pair), "unbilled before any billing (canonical numeric key)")

	// The billing row arrives with the CDC rendering: 5.0000 — the SAME
	// logical pair. It must land on the same canonical key and suppress.
	fold("billing", "b1", `{"job":"GUID-A","wa":5.0000}`)
	require.NotEmpty(t, get("billed-pairs", pair), "5.0000 folds onto the canonical key 5")
	require.Empty(t, get("unbilled", pair), "the CDC-rendered pair suppresses the app-rendered reference")

	// And a fractional value keeps its significant digits (identity, not
	// truncation): 5.25 is NOT 5.
	fold("billing", "b2", `{"job":"GUID-B","wa":5.25}`)
	require.NotEmpty(t, get("billed-pairs", OutKey([]string{"guid-b", "5.25"})))
	require.Empty(t, get("billed-pairs", OutKey([]string{"guid-b", "5"})), "5.25 must not collapse onto 5")
}

// The canonical digit renderer, exhaustively at its edges: exponents,
// leading/trailing zeros, signed zero, and non-numeric pass-through.
func TestCanonicalKeyPart(t *testing.T) {
	cases := map[string]string{
		"5":                       "5",
		"5.0000":                  "5",
		"05":                      "5",
		"0.500":                   "0.5",
		"5.25":                    "5.25",
		"-5.0":                    "-5",
		"-0.000":                  "0",
		"0":                       "0",
		"5e2":                     "500",
		"5E+2":                    "500",
		"1e-3":                    "0.001",
		"5.25e1":                  "52.5",
		"441744":                  "441744",
		"12345678901234567890.10": "12345678901234567890.1",
	}
	for in, want := range cases {
		require.Equal(t, want, CanonicalKeyPart(json.Number(in)), "json.Number(%s)", in)
	}
	require.Equal(t, "5", CanonicalKeyPart(float64(5)))
	require.Equal(t, "5.25", CanonicalKeyPart(float64(5.25)))
	require.Equal(t, "007", CanonicalKeyPart("007"), "a STRING key is text, never re-parsed as a number")
	require.Equal(t, "GUID-A", CanonicalKeyPart("GUID-A"))
}
