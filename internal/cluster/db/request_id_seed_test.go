package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRequestID_SeededPerProcess is the raft-apply-ack-requestid-reuse
// regression. notifyApplied matches a proposal's apply to a blocking Propose's
// waiter by RequestID alone, but the RequestID counter reset to 0 each process —
// so two lifetimes (a restart) or two nodes both assigned 1, 2, 3, …, and a
// replayed/foreign proposal carrying a reused RequestID could falsely release a
// waiter another process registered on the same value ("applied" for something
// that hadn't committed). Seeding the counter at an independent random point in
// the top half of the uint64 space per process makes each process's contiguous
// block of ids disjoint, removing the collision at the source.
func TestRequestID_SeededPerProcess(t *testing.T) {
	// randomRequestIDBase confines the base to [2^62, 2^63): far above 0 (the
	// notifyApplied "no waiter" sentinel) and any small pre-upgrade-log id, with
	// bit 63 left clear as headroom so the counter can never wrap toward 0/small.
	const floor = uint64(1) << 62
	const ceil = uint64(1) << 63

	d1 := createDB()
	defer d1.Close()
	d2 := createDB()
	defer d2.Close()

	id1 := d1.NextRequestIDForTest()
	id2 := d2.NextRequestIDForTest()

	require.GreaterOrEqual(t, id1, floor, "RequestIDs must be seeded well above 0 and any small logged id")
	require.Less(t, id1, ceil, "the base leaves bit 63 clear as wrap headroom")
	require.GreaterOrEqual(t, id2, floor)
	require.Less(t, id2, ceil)

	// The crux: two independent process lifetimes must not assign the same
	// RequestID — they both used to start at 1.
	require.NotEqual(t, id1, id2, "two process lifetimes must not assign the same RequestID (the false-ACK root cause)")
}
