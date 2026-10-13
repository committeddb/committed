package wal

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSeenFilter: no key the filter has seen is ever reported unseen, across
// enough keys to chain several segments, and the false-positive rate on keys it
// has not seen stays near the design rate.
func TestSeenFilter(t *testing.T) {
	f := newSeenFilter()
	const n = 300_000
	for i := range n {
		f.testAndAdd(fmt.Sprint("seen-", i))
	}
	require.Greater(t, len(f.parts), 2, "the filter grew past its first segment")
	for i := range n {
		require.True(t, f.testAndAdd(fmt.Sprint("seen-", i)), "key %d was seen", i)
	}
	falsePositives := 0
	const probes = 100_000
	for i := range probes {
		if f.testAndAdd(fmt.Sprint("unseen-", i)) {
			falsePositives++
		}
	}
	require.Less(t, falsePositives, probes*2/100, "false-positive rate stays under ~2% across segments")
}
