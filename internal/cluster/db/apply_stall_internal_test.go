package db

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The detector's contract: trip ONLY when committed work is pending and
// applied has made zero progress since the gap was first observed
// threshold ago. The subtle cases are the ones that must NOT trip.
func TestApplyStallDetector(t *testing.T) {
	const threshold = 20 * time.Millisecond

	t.Run("no pending work never stalls", func(t *testing.T) {
		d := &applyStallDetector{threshold: threshold}
		require.False(t, d.check(5, 5))
		time.Sleep(2 * threshold)
		require.False(t, d.check(5, 5), "an idle cluster is not stalled")
	})

	t.Run("idle cluster's first commit is not an instant stall", func(t *testing.T) {
		// The trap this design avoids: if the clock meant "time since last
		// apply", a commit arriving after a quiet period would read the
		// idle time as a stall on the first probe.
		d := &applyStallDetector{threshold: threshold}
		require.False(t, d.check(5, 5))
		time.Sleep(2 * threshold) // a quiet period, then work arrives
		require.False(t, d.check(6, 5), "the gap starts its clock at first observation")
	})

	t.Run("slow but advancing replay never stalls", func(t *testing.T) {
		d := &applyStallDetector{threshold: threshold}
		applied := uint64(0)
		deadline := time.Now().Add(4 * threshold)
		for time.Now().Before(deadline) {
			applied++ // progress on every probe, always behind commit
			require.False(t, d.check(1000, applied),
				"apply progress must reset the clock — a boot replay of a long backlog is not a stall")
			time.Sleep(threshold / 4)
		}
	})

	t.Run("zero progress with pending work trips after threshold", func(t *testing.T) {
		d := &applyStallDetector{threshold: threshold}
		require.False(t, d.check(10, 7), "first observation starts the clock")
		time.Sleep(2 * threshold)
		require.True(t, d.check(10, 7), "the wedge: pending work, no progress, past threshold")
	})

	t.Run("gap closing clears the stall", func(t *testing.T) {
		d := &applyStallDetector{threshold: threshold}
		require.False(t, d.check(10, 7))
		time.Sleep(2 * threshold)
		require.True(t, d.check(10, 7))
		require.False(t, d.check(10, 10), "catching up clears the condition")
		time.Sleep(2 * threshold)
		require.False(t, d.check(11, 10), "a fresh gap starts a fresh clock")
	})
}
