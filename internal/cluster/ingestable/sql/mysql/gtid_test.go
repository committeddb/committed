package mysql

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

// TestMergeGTID covers the consumed-set accumulation (no container needed): the
// nil-consumed seed, interval coalescing, the nil-txn (file:pos-only) passthrough,
// and retaining a second server UUID's interval across a failover.
func TestMergeGTID(t *testing.T) {
	const uuidA = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	const uuidB = "a1b2c3d4-71ca-11e1-9e33-c80aa9429562"

	parse := func(s string) mysql.GTIDSet {
		g, err := mysql.ParseMysqlGTIDSet(s)
		require.NoError(t, err)
		return g
	}

	// nil consumed → starts from a clone of txn.
	got, err := mergeGTID(nil, parse(uuidA+":1"))
	require.NoError(t, err)
	require.Equal(t, uuidA+":1", got.String())

	// the next consecutive transaction → interval coalesces.
	got, err = mergeGTID(got, parse(uuidA+":2"))
	require.NoError(t, err)
	require.Equal(t, uuidA+":1-2", got.String())

	// nil txn (a file:pos-only / gtid_mode=OFF source emits no GTIDEvent) →
	// consumed unchanged.
	same, err := mergeGTID(got, nil)
	require.NoError(t, err)
	require.Equal(t, got, same)

	// a transaction from a different server UUID (post-failover) → both intervals
	// are retained.
	got, err = mergeGTID(got, parse(uuidB+":1"))
	require.NoError(t, err)
	require.Contains(t, got.String(), uuidA+":1-2")
	require.Contains(t, got.String(), uuidB+":1")
}
