package stages

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The field's jti-v2 bug, made unrepresentable: a consumer path into a
// STAGE-produced object (declared shape) referencing a field the
// producer never emits can never resolve — admission rejects it with
// the producer, path, and emit set named. Topic payloads (dynamic
// shape) keep missing-field-is-null semantics; this guard restores the
// declared/dynamic distinction, it is not a special case.
func TestStageEmitSetPathGuard(t *testing.T) {
	prod := Stage{
		Name: "s10-wa", From: "t", KeyPath: []string{"$.k"},
		Emit: []Emit{{Field: "job", From: "$.job"}},
	}
	prod2 := Stage{
		Name: "other", From: "t2", KeyPath: []string{"$.k"},
		Emit: []Emit{{Field: "cdate", From: "$.cdate"}},
	}

	// The exact field shape: a merge side alias reading a never-emitted
	// field.
	err := ValidateShapes([]Stage{prod, prod2, {
		Name: "c-open-wa", Merge: []MergeEntry{{Stage: "s10-wa", As: "s10w"}, {Stage: "other"}},
		Emit: []Emit{{Field: "cdate", From: "$.s10w.cdate"}},
	}})
	require.ErrorContains(t, err, "never emits")
	require.ErrorContains(t, err, "s10-wa")
	require.ErrorContains(t, err, "job")

	// Stage-FED consumer: an emit path outside the upstream's emit set.
	err = ValidateShapes([]Stage{prod, {
		Name: "c", From: "s10-wa", KeyPath: []string{"$.job"},
		Emit: []Emit{{Field: "v", From: "$.cdate"}},
	}})
	require.ErrorContains(t, err, "never emits")

	// Named STAGE-join alias: same rule.
	err = ValidateShapes([]Stage{prod, {
		Name: "c", From: "t3", KeyPath: []string{"$.id"},
		Joins: []Join{{From: "s10-wa", On: []string{"$.ref"}, As: "w", Optional: true}},
		Emit:  []Emit{{Field: "v", From: "$.w.cdate"}},
	}})
	require.ErrorContains(t, err, "never emits")

	// ALLOWED: bare alias presence test; topic side (dynamic); nested
	// below an emitted field; $parent; topic-fed paths; expr reading an
	// emitted field.
	require.NoError(t, ValidateShapes([]Stage{prod, prod2, {
		Name: "ok", Merge: []MergeEntry{
			{Stage: "s10-wa", As: "s10w"},
			{Stage: "other"},
			{Topic: "raw", KeyPath: []string{"$.k"}, As: "r"},
		},
		When: []WhenClause{{Path: "$.s10w", NotNull: true}, {Expr: "$.other.cdate is not null"}},
		Emit: []Emit{
			{Field: "a", From: "$.s10w.job"},
			{Field: "b", From: "$.r.anything"},
			{Field: "c", From: "$.s10w.job.deep.path"},
		},
	}}))

	// Field-addressed join synthetic resolves to its SOURCE stage's
	// emit set (round-23 sugar must not be a blind spot).
	err = ValidateShapes([]Stage{prod, {
		Name: "c", From: "t3", KeyPath: []string{"$.id"},
		Joins: []Join{{From: "s10-wa", On: []string{"$.ref"}, Field: "$.job", As: "w"}},
		Emit:  []Emit{{Field: "v", From: "$.w.cdate"}},
	}})
	require.ErrorContains(t, err, "never emits")
}
