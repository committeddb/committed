package cluster

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCensusFoldShapesAndOrdinals pins the census's durable state: distinct
// shapes with counts and first/last row ordinals — the interleaved-shapes
// evidence the errata workflow later needs.
func TestCensusFoldShapesAndOrdinals(t *testing.T) {
	tc := &TopicCensus{}
	opts := CensusOptions{}
	require.NoError(t, tc.Fold([]byte(`{"caption":"a","size":1}`), opts))        // shape A, row 1
	require.NoError(t, tc.Fold([]byte(`{"caption":"b","size":2}`), opts))        // shape A, row 2
	require.NoError(t, tc.Fold([]byte(`{"caption":"c","ai":{"m":"v9"}}`), opts)) // shape B, row 3
	require.NoError(t, tc.Fold([]byte(`{"caption":"d","size":9}`), opts))        // shape A, row 4

	require.Equal(t, uint64(4), tc.Rows)
	require.Len(t, tc.Shapes, 2)

	var a, b *ShapeCensus
	for _, s := range tc.Shapes {
		if contains(s.Shape, "$.size:number") {
			a = s
		} else {
			b = s
		}
	}
	require.Equal(t, uint64(3), a.Count)
	require.Equal(t, uint64(1), a.FirstRow)
	require.Equal(t, uint64(4), a.LastRow, "the interleaved shape's range spans its sightings")
	require.Equal(t, uint64(1), b.Count)
	require.Equal(t, uint64(3), b.FirstRow)

	// The derived per-path view: union types, summed counts, merged ranges.
	paths := map[string]*PathCensus{}
	for _, pc := range tc.PathsView() {
		paths[pc.Path] = pc
	}
	require.Equal(t, uint64(4), paths["$.caption"].Count, "every row carries caption")
	require.Equal(t, []string{"string"}, paths["$.caption"].Types)
	require.Equal(t, uint64(3), paths["$.size"].Count)
	require.Equal(t, uint64(1), paths["$.ai.m"].Count)
	require.Equal(t, uint64(3), paths["$.ai.m"].FirstRow)

	// A non-JSON payload errors (the worker logs and skips, never disturbs
	// ingest).
	require.Error(t, tc.Fold([]byte("not json"), opts))
}

// TestCensusValueTracking pins the opt-in bounded distinct-value tracking:
// values accumulate sorted up to the limit, then the path overflows and drops
// its set (no unbounded PII accumulation).
func TestCensusValueTracking(t *testing.T) {
	tc := &TopicCensus{}
	opts := CensusOptions{TrackValues: true, ValueLimit: 3}
	for _, v := range []string{"cc", "arr", "cc", "publicdomain"} {
		require.NoError(t, tc.Fold([]byte(`{"license":"`+v+`"}`), opts))
	}
	pv := tc.Values["$.license"]
	require.NotNil(t, pv)
	require.False(t, pv.Overflowed)
	require.Equal(t, []string{"arr", "cc", "publicdomain"}, pv.Values, "distinct, sorted, deduped")

	// The fourth distinct value crosses the limit: overflow, set dropped.
	require.NoError(t, tc.Fold([]byte(`{"license":"gpl"}`), opts))
	require.True(t, pv.Overflowed)
	require.Nil(t, pv.Values)

	// Default (no TrackValues): no values are ever kept — the PII posture.
	tc2 := &TopicCensus{}
	require.NoError(t, tc2.Fold([]byte(`{"license":"cc"}`), CensusOptions{}))
	require.Nil(t, tc2.Values)
}

// TestIngestableCensusRoundTrip pins the replicated record's wire round-trip
// and its system-type classification.
func TestIngestableCensusRoundTrip(t *testing.T) {
	tc := &TopicCensus{}
	require.NoError(t, tc.Fold([]byte(`{"a":"x"}`), CensusOptions{TrackValues: true}))
	c := &IngestableCensus{ID: "ing-1", RefreshEpoch: 2, Topics: map[string]*TopicCensus{"photos": tc}}

	bs, err := c.Marshal()
	require.NoError(t, err)
	got := &IngestableCensus{}
	require.NoError(t, got.Unmarshal(bs))
	require.Equal(t, c, got)

	e, err := NewIngestableCensusEntity(c)
	require.NoError(t, err)
	require.True(t, IsIngestableCensus(e.Type.ID))
	require.True(t, IsInternal(e.Type.ID), "the census record is internal — syncables never see it")
	state, ok := reservedSystemClass(e.Type.ID)
	require.True(t, ok)
	require.Equal(t, compatUngated, state, "observability record: an older node skips it")
}

// TestDraftTypeSchema pins the drafter: nested properties mirroring paths,
// additionalProperties:false at every object level, arrays, type unions
// where shapes disagreed, and enum for tracked low-cardinality strings —
// and the draft must COMPILE and behave as the tripwire's contract (the
// blessing flow's whole point).
func TestDraftTypeSchema(t *testing.T) {
	tc := &TopicCensus{}
	opts := CensusOptions{TrackValues: true, ValueLimit: 4}
	require.NoError(t, tc.Fold([]byte(`{"caption":"a","license":"cc","tags":["x"],"meta":{"w":3}}`), opts))
	require.NoError(t, tc.Fold([]byte(`{"caption":null,"license":"arr","tags":["y"],"meta":{"w":4}}`), opts))
	// license repeats (the enum heuristic needs repeat evidence — a closed
	// domain, not merely a small census); caption stays all-distinct.
	require.NoError(t, tc.Fold([]byte(`{"caption":"c","license":"cc","tags":["z"],"meta":{"w":5}}`), opts))

	draft, err := tc.DraftTypeSchema()
	require.NoError(t, err)

	var schema map[string]any
	require.NoError(t, json.Unmarshal(draft, &schema))
	require.Equal(t, false, schema["additionalProperties"])
	props := schema["properties"].(map[string]any)

	require.ElementsMatch(t, []any{"null", "string"}, props["caption"].(map[string]any)["type"],
		"shapes disagreed on caption: the draft carries the union")
	require.ElementsMatch(t, []any{"arr", "cc"}, props["license"].(map[string]any)["enum"],
		"a low-cardinality tracked string drafts as an enum")
	tags := props["tags"].(map[string]any)
	require.Equal(t, "array", tags["type"])
	require.Equal(t, "string", tags["items"].(map[string]any)["type"])
	meta := props["meta"].(map[string]any)
	require.Equal(t, false, meta["additionalProperties"])
	require.Equal(t, "number", meta["properties"].(map[string]any)["w"].(map[string]any)["type"])

	// caption must NOT draft an enum: its values never repeat (free text) and
	// its type union includes null.
	require.NotContains(t, props["caption"].(map[string]any), "enum")
}

// BenchmarkCensusFoldSeenShape pins the fast-path claim: folding a row whose
// shape is already known costs one signature (unmarshal + walk + hash) and a
// counter bump — no merging — so billion-row tables pay only the signature.
func BenchmarkCensusFoldSeenShape(b *testing.B) {
	tc := &TopicCensus{}
	payload := []byte(`{"caption":"a","size":3,"tags":["x","y"],"meta":{"w":1,"h":2}}`)
	require.NoError(b, tc.Fold(payload, CensusOptions{}))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tc.Fold(payload, CensusOptions{})
	}
	if len(tc.Shapes) != 1 {
		b.Fatalf("fast path violated: %d shapes", len(tc.Shapes))
	}
}

// TestCensusFoldManyShapesStaysBounded sanity-checks that shape count tracks
// DISTINCT shapes, not rows (the accumulator's size is shape-bounded).
func TestCensusFoldManyShapesStaysBounded(t *testing.T) {
	tc := &TopicCensus{}
	for i := 0; i < 500; i++ {
		require.NoError(t, tc.Fold(fmt.Appendf(nil, `{"caption":"row-%d"}`, i), CensusOptions{}))
	}
	require.Equal(t, uint64(500), tc.Rows)
	require.Len(t, tc.Shapes, 1, "500 rows, one shape")
}
