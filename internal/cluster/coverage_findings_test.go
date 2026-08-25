package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The interpretation split that makes a mistyped topic id LOUD: under
// complete coverage, "never appeared" is a config fault; under partial
// coverage it is a sampling note. The wrong wording under complete
// coverage would misdiagnose a typo as eternal under-sampling.
func TestCoverageFindings(t *testing.T) {
	require.Nil(t, CoverageFindings(nil, true))

	complete := CoverageFindings([]string{"transactoin-events"}, true)
	require.Len(t, complete, 1)
	require.Contains(t, complete[0], "NO entries anywhere in the log")
	require.Contains(t, complete[0], "config fault")
	require.Contains(t, complete[0], "transactoin-events")

	partial := CoverageFindings([]string{"txn-events"}, false)
	require.Len(t, partial, 1)
	require.Contains(t, partial[0], "sampled windows")
	require.Contains(t, partial[0], "maxEntries")
}
