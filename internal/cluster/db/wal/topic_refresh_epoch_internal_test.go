package wal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTopicRefreshEpoch_Store exercises the delete-surviving per-topic
// refresh-epoch highwater as a store: monotonic max, per-topic isolation, and
// durability across a reopen. The apply-path integration and delete-survival are
// covered in topic_refresh_epoch_test.go.
func TestTopicRefreshEpoch_Store(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, nil, nil, nil)
	require.NoError(t, err)
	s.stopScrubWorker()

	require.Equal(t, uint64(0), s.TopicRefreshEpoch("orders"), "an unknown topic reports 0")

	require.NoError(t, s.bumpTopicRefreshEpoch("orders", 3))
	require.Equal(t, uint64(3), s.TopicRefreshEpoch("orders"))

	// Monotonic max: a lower generation never lowers the highwater.
	require.NoError(t, s.bumpTopicRefreshEpoch("orders", 2))
	require.Equal(t, uint64(3), s.TopicRefreshEpoch("orders"))
	require.NoError(t, s.bumpTopicRefreshEpoch("orders", 5))
	require.Equal(t, uint64(5), s.TopicRefreshEpoch("orders"))

	// A zero generation is a no-op (internal/config entities carry no generation).
	require.NoError(t, s.bumpTopicRefreshEpoch("orders", 0))
	require.Equal(t, uint64(5), s.TopicRefreshEpoch("orders"))

	// Per-topic isolation.
	require.Equal(t, uint64(0), s.TopicRefreshEpoch("other"))
	require.NoError(t, s.bumpTopicRefreshEpoch("other", 9))
	require.Equal(t, uint64(9), s.TopicRefreshEpoch("other"))
	require.Equal(t, uint64(5), s.TopicRefreshEpoch("orders"))

	// Survives a restart (persisted to bbolt).
	require.NoError(t, s.Close())
	s2, err := Open(dir, nil, nil, nil)
	require.NoError(t, err)
	s2.stopScrubWorker()
	defer func() { _ = s2.Close() }()
	require.Equal(t, uint64(5), s2.TopicRefreshEpoch("orders"))
	require.Equal(t, uint64(9), s2.TopicRefreshEpoch("other"))
}
