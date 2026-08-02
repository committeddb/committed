package wal

import (
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// boltTxID returns the current bbolt transaction id — it advances only on
// read-write commits, so a delta of 0 across a window proves no write tx was
// opened in it.
func boltTxID(t *testing.T, s *Storage) uint64 {
	t.Helper()
	var id uint64
	require.NoError(t, s.view(func(tx *bolt.Tx) error {
		id = uint64(tx.ID())
		return nil
	}))
	return id
}

// TestBumpTopicRefreshEpoch_NoOpOpensNoWriteTx pins the guard-before-write
// invariant: a no-op raise (gen <= current highwater) must not open a bbolt
// write transaction. The helper runs for every applied generation-stamped
// entity — every ingested row — and a bbolt write tx fdatasyncs twice even with
// zero puts (fresh freelist page + meta, no clean-commit shortcut), so a no-op
// write tx here puts a ~ms fsync floor under every applied row (the mass-create
// churn incident's root). The bbolt txid advances only on rw commits, so a zero
// delta across M no-op raises proves the invariant; neutralizing the guard
// makes the delta scale with M.
func TestBumpTopicRefreshEpoch_NoOpOpensNoWriteTx(t *testing.T) {
	s, err := Open(t.TempDir(), nil, nil, nil)
	require.NoError(t, err)
	s.stopScrubWorker()
	defer func() { _ = s.Close() }()

	// Establish the highwater (one genuine raise — one write tx).
	require.NoError(t, s.bumpTopicRefreshEpoch("orders", 5))

	const m = 200
	before := boltTxID(t, s)
	for range m {
		require.NoError(t, s.bumpTopicRefreshEpoch("orders", 5)) // equal: no-op
		require.NoError(t, s.bumpTopicRefreshEpoch("orders", 3)) // below: no-op
	}
	require.Equal(t, before, boltTxID(t, s),
		"a no-op raise must not open a write tx (each costs two fdatasyncs — the per-row apply floor)")

	// A genuine raise still writes durably — exactly one commit.
	require.NoError(t, s.bumpTopicRefreshEpoch("orders", 6))
	require.Equal(t, before+1, boltTxID(t, s), "a genuine raise commits exactly once")
	require.Equal(t, uint64(6), s.TopicRefreshEpoch("orders"))
}

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
