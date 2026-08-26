package stages

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The field's jti-v2 anomaly: quoted_completed_date (the pipeline's
// only value pulled through a named TOPIC join alias) came back NULL
// on all 5,983 rows — but ONLY in the full s10-pairs combination:
// stage-fed input + composite key + filtering stage join + named topic
// join. Four field probes pass each ingredient in isolation; this test
// assembles the whole shape.
func TestNamedTopicJoinPullFullCombination(t *testing.T) {
	sts := []Stage{
		{
			Name: "base", From: "raw", KeyPath: []string{"$.visit", "$.wa"}, Normalize: "lower", KeyType: []string{"text", "number"},
			Emit: []Emit{
				{Field: "visit", From: "$.visit"},
				{Field: "wa", From: "$.wa"},
				{Field: "fid", From: "$.fid"},
			},
		},
		{
			Name: "flt", From: "flt-topic", KeyPath: []string{"$.visit", "$.wa"}, Normalize: "lower",
			KeyType: []string{"text", "number"},
			Emit:    []Emit{{Field: "ok", From: "$.ok"}},
		},
		{
			Name: "pairs", From: "base", KeyPath: []string{"$.visit", "$.wa"}, Normalize: "lower",
			KeyType: []string{"text", "number"},
			Joins: []Join{
				{From: "flt", On: []string{"$.visit", "$.wa"}},
				{Topic: "timecards", On: []string{"$.visit"}, As: "tc", Normalize: "lower", Where: []WhenClause{{Path: "$.DeletedAtUtc", Null: true}}},
			},
			Emit: []Emit{
				{Field: "date", From: "$.tc.DateUtc"},
				{Field: "visit", From: "$.visit"},
			},
		},
	}
	fold, get, _ := harness(t, sts, "pairs")

	fold("flt-topic", "f1", `{"visit":"V-9","wa":7,"ok":true}`)
	fold("timecards", "V-9", `{"DateUtc":"2026-03-01T00:00:00Z","DeletedAtUtc":null}`)
	fold("raw", "r1", `{"visit":"V-9","wa":7,"fid":"f1"}`)

	out := get(OutKey([]string{"v-9", "7"}))
	require.Contains(t, out, `"2026-03-01T00:00:00Z"`,
		"the named topic join's pull must survive the full combination (stage-fed + composite key + filtering stage join)")
}
