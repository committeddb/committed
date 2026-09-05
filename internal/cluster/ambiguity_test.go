package cluster

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAmbiguityTracker_DistinctRowRunFlips: failures on distinct rows
// accumulate; the threshold-th flips the classification from Permanent
// (entry-specific until proven otherwise) to ErrConfigShaped (transient).
func TestAmbiguityTracker_DistinctRowRunFlips(t *testing.T) {
	tr := NewAmbiguityTracker()
	base := errors.New("unknown key typo")
	for i := 1; i < AmbiguityEvidenceThreshold; i++ {
		err := tr.Classify(uint64(i), base)
		require.ErrorIs(t, err, ErrPermanent, "failure %d is still possibly entry-specific", i)
		require.NotErrorIs(t, err, ErrConfigShaped)
	}
	err := tr.Classify(uint64(AmbiguityEvidenceThreshold), base)
	require.ErrorIs(t, err, ErrConfigShaped, "the threshold-th distinct row establishes config-shape")
	require.NotErrorIs(t, err, ErrPermanent, "config-shaped must be transient — the worker wedges")
	require.ErrorIs(t, err, base, "the site's underlying error must stay reachable")
}

// TestAmbiguityTracker_RetriesAreNotEvidence: the worker re-presents a failed
// Actual; a retry of the same row must not advance the run — but once the
// threshold is reached, retries of the wedged row stay config-shaped (the
// wedge is stable).
func TestAmbiguityTracker_RetriesAreNotEvidence(t *testing.T) {
	tr := NewAmbiguityTracker()
	base := errors.New("boom")
	for i := 0; i < 100; i++ {
		err := tr.Classify(7, base) // same row every time
		require.ErrorIs(t, err, ErrPermanent, "one row retried forever is never config-shaped")
	}
	for i := 1; i < AmbiguityEvidenceThreshold; i++ { // 9 more distinct rows (row 7 counted once)
		tr.Classify(uint64(100+i), base)
	}
	err := tr.Classify(uint64(100+AmbiguityEvidenceThreshold-1), base)
	require.ErrorIs(t, err, ErrConfigShaped, "a retry of the threshold row must stay wedged")
}

// TestAmbiguityTracker_SuccessResetsRun: a success of the SITE resets the
// evidence — scattered genuine misses interleaved with matches never flip.
func TestAmbiguityTracker_SuccessResetsRun(t *testing.T) {
	tr := NewAmbiguityTracker()
	base := errors.New("absent in this row")
	idx := uint64(0)
	for round := 0; round < 5; round++ {
		for i := 0; i < AmbiguityEvidenceThreshold-1; i++ {
			idx++
			require.ErrorIs(t, tr.Classify(idx, base), ErrPermanent)
		}
		tr.Succeeded()
	}
	idx++
	require.ErrorIs(t, tr.Classify(idx, base), ErrPermanent,
		"the run restarts after every success — 45 scattered misses never establish config-shape")
}

// TestAmbiguityTracker_NilClassifiesPermanent: a nil tracker (a
// directly-constructed config that skipped parse wiring) must default to the
// pre-tracker behavior, not panic.
func TestAmbiguityTracker_NilClassifiesPermanent(t *testing.T) {
	var tr *AmbiguityTracker
	tr.Succeeded() // must not panic
	err := tr.Classify(1, errors.New("x"))
	require.ErrorIs(t, err, ErrPermanent)
	require.NotErrorIs(t, err, ErrConfigShaped)
}
