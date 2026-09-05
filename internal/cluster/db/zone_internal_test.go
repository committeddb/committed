package db

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestZoneOwner pins the resolution's pure core: lowest current-member id in
// the zone wins (the deterministic tie-break), non-members never own, and an
// unserved zone resolves 0 — the strict pin's unsatisfiable state.
func TestZoneOwner(t *testing.T) {
	members := map[uint64]struct{}{1: {}, 2: {}, 3: {}}
	zones := map[uint64]string{1: "z-a", 2: "z-b", 3: "z-b"}

	require.Equal(t, uint64(1), zoneOwner("z-a", members, zones))
	require.Equal(t, uint64(2), zoneOwner("z-b", members, zones), "lowest id wins the tie-break")
	require.Equal(t, uint64(0), zoneOwner("z-ghost", members, zones), "unserved zone: nobody owns")

	// An announced zone from a NON-member (removed node, stale entry) never
	// resolves ownership.
	require.Equal(t, uint64(0), zoneOwner("z-c", members, map[uint64]string{9: "z-c"}))

	// Empty zone never matches a pin (pins are non-empty by admission).
	require.Equal(t, uint64(0), zoneOwner("", map[uint64]struct{}{}, nil))
}
