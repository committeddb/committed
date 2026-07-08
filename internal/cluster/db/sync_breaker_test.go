package db

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// TestSyncBreakerTripsAtCap is the circuit-breaker regression: consecutive
// permanent errors on one syncable count up and trip at the cap, so the worker
// parks instead of dead-lettering the whole topic one blocking raft round-trip
// at a time. A worker restart (resetSyncBreaker) clears the run.
func TestSyncBreakerTripsAtCap(t *testing.T) {
	db := &DB{}
	const id = "s1"

	for i := 1; i < syncBreakerMaxConsecutivePermanent; i++ {
		c, tripped := db.recordSyncPermanent(id)
		require.Equal(t, i, c)
		require.False(t, tripped, "must not trip before the cap")
	}
	require.False(t, db.syncBreakerTripped(id))

	c, tripped := db.recordSyncPermanent(id)
	require.Equal(t, syncBreakerMaxConsecutivePermanent, c)
	require.True(t, tripped, "the cap-th consecutive permanent error trips the breaker")
	require.True(t, db.syncBreakerTripped(id))

	db.resetSyncBreaker(id)
	require.False(t, db.syncBreakerTripped(id), "a worker restart clears the run")
	c, tripped = db.recordSyncPermanent(id)
	require.Equal(t, 1, c)
	require.False(t, tripped)
}

// TestTripSyncBreakerEmitsHighSeverityLog: a trip must be a distinct, loud
// signal (an ERROR log carrying the syncable id), not just a rising counter.
func TestTripSyncBreakerEmitsHighSeverityLog(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	db := &DB{logger: zap.New(core)}

	db.tripSyncBreaker("s1", syncBreakerMaxConsecutivePermanent, errors.New("bad config"))

	entries := logs.FilterMessageSnippet("circuit breaker").All()
	require.Len(t, entries, 1, "trip must emit exactly one high-severity log")
	require.Equal(t, zap.ErrorLevel, entries[0].Level)
	require.Equal(t, "s1", entries[0].ContextMap()["id"])
}
