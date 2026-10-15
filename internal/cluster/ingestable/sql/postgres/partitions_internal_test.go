package postgres

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPartitionWatchObserve(t *testing.T) {
	w := &partitionWatch{known: make(map[string]map[uint32]string)}
	t0 := time.Unix(1000, 0)

	w.observe("events", map[uint32]string{1: "events_a", 2: "events_b"}, t0)
	require.Empty(t, w.removedPartitions(), "the first read is the baseline")

	w.observe("events", map[uint32]string{1: "events_a", 2: "events_b", 3: "events_c"}, t0.Add(time.Second))
	require.Empty(t, w.removedPartitions(), "a new partition is not a removal")

	w.observe("events", map[uint32]string{3: "events_c"}, t0.Add(2*time.Second))
	got := w.removedPartitions()
	require.Len(t, got, 2)
	require.Equal(t, "events_a", got[0].Partition)
	require.Equal(t, "events_b", got[1].Partition)
	require.Equal(t, "events", got[0].Table)
	require.Equal(t, t0.Add(2*time.Second), got[0].DetectedAt)

	// Recreated under the same name, it is a different partition: dropping it
	// again is a second removal.
	w.observe("events", map[uint32]string{3: "events_c", 4: "events_a"}, t0.Add(3*time.Second))
	w.observe("events", map[uint32]string{3: "events_c"}, t0.Add(4*time.Second))
	require.Len(t, w.removedPartitions(), 3)

	// Each watched table has its own baseline.
	w.observe("other", map[uint32]string{}, t0)
	require.Len(t, w.removedPartitions(), 3)
}

func TestPartitionWatchKeepsMostRecent(t *testing.T) {
	w := &partitionWatch{known: make(map[string]map[uint32]string)}
	parts := make(map[uint32]string)
	for i := range maxRemovedPartitions + 5 {
		parts[uint32(i)] = fmt.Sprintf("p%02d", i)
	}
	w.observe("events", parts, time.Unix(0, 0))
	for i := range maxRemovedPartitions + 5 {
		next := make(map[uint32]string)
		for oid, name := range parts {
			if oid > uint32(i) {
				next[oid] = name
			}
		}
		w.observe("events", next, time.Unix(int64(i+1), 0))
		parts = next
	}
	got := w.removedPartitions()
	require.Len(t, got, maxRemovedPartitions)
	require.Equal(t, "p05", got[0].Partition, "the oldest removals are forgotten first")
	require.Equal(t, fmt.Sprintf("p%02d", maxRemovedPartitions+4), got[len(got)-1].Partition)
}
