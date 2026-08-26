package stages

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Field-addressed stage joins — the lp-bypid killer: join a prior stage
// by a field it EMITS instead of its key. The engine synthesizes the
// re-key stage the author used to write by hand (same machinery, same
// cost — later fusable), so the config says what SQL says: JOIN ON any
// column.
func TestJoinFieldAddressesEmittedField(t *testing.T) {
	sts := []Stage{
		{
			Name: "latest-prop", From: "proposals", KeyPath: []string{"$.id"},
			Emit: []Emit{
				{Field: "pid", From: "$.projectId"},
				{Field: "amount", From: "$.amount"},
			},
		},
		{
			Name: "cand", From: "projects", KeyPath: []string{"$.id"},
			Joins: []Join{{From: "latest-prop", On: []string{"$.id"}, Field: "$.pid", As: "lp", Optional: true}},
			Emit: []Emit{
				{Field: "amount", Expr: "coalesce($.lp.amount, 0)"},
			},
		},
	}
	fold, get, del := harness(t, sts, "cand")

	fold("projects", "j1", `{"id":"J1"}`)
	require.Equal(t, `{"amount":0}`, get("J1"), "no proposal yet — LEFT lookup scopes null")

	// The proposal keys by ITS id but is addressed by the pid it emits.
	fold("proposals", "p9", `{"id":"p9","projectId":"J1","amount":50}`)
	require.Equal(t, `{"amount":50}`, get("J1"), "the field-addressed lookup resolves without a hand-written re-key stage")

	// The producer's emitted field CHANGES: the old addressing retracts,
	// the new one resolves — dependents refold through the synthesis.
	fold("proposals", "p9", `{"id":"p9","projectId":"J2","amount":50}`)
	require.Equal(t, `{"amount":0}`, get("J1"), "re-addressed away")

	fold("projects", "j2", `{"id":"J2"}`)
	require.Equal(t, `{"amount":50}`, get("J2"))

	del("proposals", "p9")
	require.Equal(t, `{"amount":0}`, get("J2"), "producer retraction propagates")
}

// Topic merge sides — the pw-quoted killer: a merge side may be a TOPIC
// with a declared key space, without a hand-written lift stage.
func TestMergeTopicSide(t *testing.T) {
	sts := []Stage{
		{
			Name: "sums", From: "payments", KeyPath: []string{"$.wa"}, Normalize: "lower",
			Reduce: "aggregate",
			Emit:   []Emit{{Field: "paid", Sum: "$.amount"}},
		},
		{
			Name: "quoted", Merge: []MergeEntry{
				{Stage: "sums"},
				{Topic: "workareas", KeyPath: []string{"$.Id"}, Normalize: "lower", As: "wa"},
			},
			Emit: []Emit{{Field: "remaining", Expr: "coalesce($.wa.quotedPrice, 0) - coalesce($.sums.paid, 0)"}},
		},
	}
	fold, get, _ := harness(t, sts, "quoted")

	// The topic side alone creates the key (full outer).
	fold("workareas", "W1", `{"Id":"W1","quotedPrice":100}`)
	require.Equal(t, `{"remaining":100}`, get(OutKey([]string{"w1"})))

	// The stage side folds in — same normalized key space.
	fold("payments", "m1", `{"wa":"w1","amount":30}`)
	require.Equal(t, `{"remaining":70}`, get(OutKey([]string{"w1"})))
}

func TestSyntheticValidates(t *testing.T) {
	// field on a TOPIC join is rejected (topics are addressed by entity key).
	err := ValidateShapes([]Stage{{
		Name: "s", From: "t", KeyPath: []string{"$.id"},
		Joins: []Join{{Topic: "d", On: []string{"$.r"}, Field: "$.x"}},
		Emit:  []Emit{{Field: "v", From: "$.v"}},
	}})
	require.ErrorContains(t, err, "field")

	// A topic merge side requires its key space (keyPath) and a
	// path-safe alias.
	err = ValidateShapes([]Stage{
		{Name: "a", From: "t", KeyPath: []string{"$.id"}, Emit: []Emit{{Field: "v", From: "$.v"}}},
		{Name: "m", Merge: []MergeEntry{{Stage: "a"}, {Topic: "workareas"}}, Emit: []Emit{{Field: "v", From: "$.a.v"}}},
	})
	require.ErrorContains(t, err, "keyPath")

	// User stage names may not use the synthetic bracket form.
	err = ValidateShapes([]Stage{{
		Name: "s[x]", From: "t", KeyPath: []string{"$.id"},
		Emit: []Emit{{Field: "v", From: "$.v"}},
	}})
	require.ErrorContains(t, err, "reserved")
}
