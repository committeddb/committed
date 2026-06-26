package mysql

import (
	"errors"
	"fmt"
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

// TestIsGtidPurged covers the error-1236 classifier that drives the re-snapshot
// recovery: only ER_MASTER_FATAL_ERROR_READING_BINLOG (purged binlogs) counts,
// including when wrapped, and an ordinary stream error does not.
func TestIsGtidPurged(t *testing.T) {
	require.True(t, isGtidPurged(&mysql.MyError{Code: mysql.ER_MASTER_FATAL_ERROR_READING_BINLOG}))
	require.True(t, isGtidPurged(&mysql.MyError{Code: 1236}))
	// go-mysql surfaces the error on GetEvent; runStream may wrap it.
	require.True(t, isGtidPurged(fmt.Errorf("binlog stream: %w", &mysql.MyError{Code: 1236})))

	require.False(t, isGtidPurged(&mysql.MyError{Code: 1234}))
	require.False(t, isGtidPurged(errors.New("connection reset by peer")))
	require.False(t, isGtidPurged(nil))
}

func mustParseGTID(t *testing.T, s string) mysql.GTIDSet {
	t.Helper()
	g, err := mysql.ParseMysqlGTIDSet(s)
	require.NoError(t, err)
	return g
}

// TestGtidLag covers the transaction-count difference (executed − consumed) that
// drives the streaming lag: fully caught up, a simple backlog, an unconsumed
// server UUID, a hole between consumed intervals, a nil consumed set, and an
// errant GTID in consumed that the source never executed (must not go negative).
func TestGtidLag(t *testing.T) {
	const uuidA = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	const uuidB = "a1b2c3d4-71ca-11e1-9e33-c80aa9429562"

	// caught up: executed ⊆ consumed → 0.
	require.Equal(t, uint64(0),
		gtidLag(mustParseGTID(t, uuidA+":1-5"), mustParseGTID(t, uuidA+":1-5")))

	// three transactions executed but not consumed.
	require.Equal(t, uint64(3),
		gtidLag(mustParseGTID(t, uuidA+":1-8"), mustParseGTID(t, uuidA+":1-5")))

	// an entire unconsumed second server UUID counts.
	require.Equal(t, uint64(3),
		gtidLag(mustParseGTID(t, uuidA+":1-5,"+uuidB+":1-3"), mustParseGTID(t, uuidA+":1-5")))

	// a hole between consumed intervals: executed 1-9, consumed {1-3, 7-9} → {4,5,6}.
	require.Equal(t, uint64(3),
		gtidLag(mustParseGTID(t, uuidA+":1-9"), mustParseGTID(t, uuidA+":1-3:7-9")))

	// nil consumed (no GTID checkpoint yet) → everything executed is lag.
	require.Equal(t, uint64(5),
		gtidLag(mustParseGTID(t, uuidA+":1-5"), nil))

	// errant GTID in consumed the source never executed → not negative, just 0.
	require.Equal(t, uint64(0),
		gtidLag(mustParseGTID(t, uuidA+":1-5"), mustParseGTID(t, uuidA+":1-10")))

	// empty executed → 0.
	require.Equal(t, uint64(0),
		gtidLag(mustParseGTID(t, ""), mustParseGTID(t, uuidA+":1-5")))
}

// TestNeedsResnapshot covers the purged-hole detector: a re-snapshot is required
// exactly when the source purged a GTID the consumed set does not contain.
func TestNeedsResnapshot(t *testing.T) {
	const uuidA = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	const uuidB = "a1b2c3d4-71ca-11e1-9e33-c80aa9429562"

	// nothing purged → never a hole.
	require.False(t, needsResnapshot(mustParseGTID(t, uuidA+":1-5"), mustParseGTID(t, "")))

	// purged ⊆ consumed → already consumed what was dropped, no hole.
	require.False(t, needsResnapshot(mustParseGTID(t, uuidA+":1-5"), mustParseGTID(t, uuidA+":1-3")))

	// purged past consumed → unrecoverable hole.
	require.True(t, needsResnapshot(mustParseGTID(t, uuidA+":1-5"), mustParseGTID(t, uuidA+":1-10")))

	// purged a server UUID consumed never saw → hole.
	require.True(t, needsResnapshot(mustParseGTID(t, uuidA+":1-5"), mustParseGTID(t, uuidB+":1-3")))

	// nil consumed with something purged → hole (consumed nothing already dropped).
	require.True(t, needsResnapshot(nil, mustParseGTID(t, uuidA+":1-3")))
}
