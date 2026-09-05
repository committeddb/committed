package interpretation

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// brokenPredicateReg builds a registry with one predicate restatement whose
// program compiles (admission accepts it) but errors at runtime on any
// payload whose contact is not a string — the compiles-but-fails-every-row
// shape the per-restatement tracker exists to classify.
func brokenPredicateReg(t *testing.T, fromVersion int) *Registry {
	t.Helper()
	return reg(t, cluster.AppliedRestatement{Index: 5, Restatement: cluster.Restatement{
		ID: "e1", TypeID: "person", FromIndex: 1, ToIndex: 1 << 40,
		FromVersion: fromVersion, ReadAsVersion: 2,
		Predicate: `(.contact | ascii_downcase) == "alice"`,
	}})
}

// TestPredicateRun_FlipsToConfigShaped pins the interpretation member of the
// ambiguity class: a restatement predicate erroring across consecutive distinct
// rows with no clean evaluation is config-shaped — the classification flips
// from Permanent (dead-letter) to transient (the worker wedges) at the
// evidence threshold, and a retry of the same row stays wedged.
func TestPredicateRun_FlipsToConfigShaped(t *testing.T) {
	r := brokenPredicateReg(t, 0)
	ctx := context.Background()
	bad := []byte(`{"contact": 5}`)

	for i := 1; i < cluster.AmbiguityEvidenceThreshold; i++ {
		_, err := r.EffectiveVersion(ctx, "person", uint64(10+i), 1, bad)
		require.ErrorIs(t, err, cluster.ErrPermanent, "failure %d may still be entry-specific", i)
		require.NotErrorIs(t, err, cluster.ErrConfigShaped)
	}
	last := uint64(10 + cluster.AmbiguityEvidenceThreshold)
	_, err := r.EffectiveVersion(ctx, "person", last, 1, bad)
	require.ErrorIs(t, err, cluster.ErrConfigShaped, "the threshold-th distinct row establishes the predicate config-shaped")
	require.NotErrorIs(t, err, cluster.ErrPermanent, "config-shaped must wedge, not dead-letter")

	_, err = r.EffectiveVersion(ctx, "person", last, 1, bad)
	require.ErrorIs(t, err, cluster.ErrConfigShaped, "retrying the wedged row must stay wedged")
}

// TestPredicateCleanEvalResetsEvidence: a clean evaluation — match or
// no-match — proves the predicate can read the data and resets the run, so
// scattered rows the program genuinely can't evaluate keep dead-lettering.
func TestPredicateCleanEvalResetsEvidence(t *testing.T) {
	r := brokenPredicateReg(t, 0)
	ctx := context.Background()

	for i := 1; i <= 3*cluster.AmbiguityEvidenceThreshold; i++ {
		payload := []byte(`{"contact": "BOB"}`) // evaluates cleanly, no match
		if i%2 == 0 {
			payload = []byte(`{"contact": 5}`) // genuinely unevaluable row
		}
		_, err := r.EffectiveVersion(ctx, "person", uint64(i), 1, payload)
		if i%2 == 0 {
			require.ErrorIs(t, err, cluster.ErrPermanent, "isolated unevaluable rows keep dead-lettering (row %d)", i)
			require.NotErrorIs(t, err, cluster.ErrConfigShaped)
		} else {
			require.NoError(t, err)
		}
	}
}

// TestRangeGatedPredicate_StillFlips pins the gap the per-site design
// closes here too: a restatement gated to one stamped version errors on every
// row it actually evaluates, while rows of other versions pass through
// cleanly without touching it. Those unrelated successes must not mask the
// broken predicate — the 10th gated row flips.
func TestRangeGatedPredicate_StillFlips(t *testing.T) {
	r := brokenPredicateReg(t, 1) // FromVersion 1: only v1-stamped rows evaluate
	ctx := context.Background()
	gated := 0

	for i := 1; gated < cluster.AmbiguityEvidenceThreshold; i++ {
		stamped := 2 // ungated: Matches misses, predicate never runs
		if i%2 == 0 {
			stamped = 1 // gated: predicate evaluates and errors
		}
		eff, err := r.EffectiveVersion(ctx, "person", uint64(i), stamped, []byte(`{"contact": 5}`))
		if stamped == 2 {
			require.NoError(t, err)
			require.Equal(t, 2, eff, "ungated rows pass through untouched")
			continue
		}
		gated++
		if gated < cluster.AmbiguityEvidenceThreshold {
			require.ErrorIs(t, err, cluster.ErrPermanent, fmt.Sprintf("gated failure %d stays entry-specific", gated))
			require.NotErrorIs(t, err, cluster.ErrConfigShaped)
		} else {
			require.ErrorIs(t, err, cluster.ErrConfigShaped, "interleaved ungated rows must not mask the broken predicate")
		}
	}
}

// TestWrapPassesClassifiedPredicateErrors: the Sync wrapper no longer
// blanket-wraps rebind failures Permanent — the classification travels from
// the restatement's tracker through Sync untouched, so an established
// config-shaped predicate wedges the worker.
func TestWrapPassesClassifiedPredicateErrors(t *testing.T) {
	r := brokenPredicateReg(t, 0)
	tp := &cluster.Type{ID: "person", Name: "Person", Version: 1}
	s := Wrap(okInner{}, func() *Registry { return r }, nil)
	ctx := context.Background()

	var err error
	for i := 1; i <= cluster.AmbiguityEvidenceThreshold; i++ {
		_, err = s.Sync(ctx, &cluster.Actual{Index: uint64(i), Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(tp, fmt.Appendf(nil, "k%02d", i), []byte(`{"contact": 5}`)),
		}})
		require.Error(t, err)
	}
	require.ErrorIs(t, err, cluster.ErrConfigShaped, "the wrapper must pass the site's classification through")
	require.NotErrorIs(t, err, cluster.ErrPermanent)
}

type okInner struct{}

func (okInner) Sync(context.Context, *cluster.Actual) (cluster.ShouldSnapshot, error) {
	return true, nil
}
func (okInner) Close() error { return nil }
