package wal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEventCacheSegments pins the event-log segment-cache wiring
// (COMMITTED_EVENT_CACHE_SEGMENTS → WithEventCacheSegments): the default is
// DefaultEventCacheSegments, a configured value reaches the live event-log
// handle, the raft entry log deliberately keeps the library default (its
// reader is the single sequential Ready loop — not a tunable anyone needs),
// and — the regression that matters — the scrub-swap REOPEN carries the
// configured size. Without eventWalOpts on the reopen, a scrub swap silently
// reverted the cache to the library default until the next restart.
func TestEventCacheSegments(t *testing.T) {
	// Default when unconfigured.
	s, err := Open(t.TempDir(), nil, nil, nil)
	require.NoError(t, err)
	s.stopScrubWorker()
	require.Equal(t, DefaultEventCacheSegments, s.eventLog.SegmentCacheSize())
	require.NotEqual(t, DefaultEventCacheSegments, s.EntryLog.SegmentCacheSize(),
		"the entry log keeps the library default — single sequential reader, no thrash")
	require.NoError(t, s.Close())

	// Configured value reaches the handle…
	s2, err := Open(t.TempDir(), nil, nil, nil, WithEventCacheSegments(5))
	require.NoError(t, err)
	s2.stopScrubWorker()
	defer func() { _ = s2.Close() }()
	require.Equal(t, 5, s2.eventLog.SegmentCacheSize())

	// …and survives the scrub-swap reopen path (close → reopen, exactly the
	// sequence runScrub performs around the directory rename).
	s2.eventMu.Lock()
	s2.closeEventLogBeforeSwapOrFatal("test reopen")
	s2.reopenEventLogAfterSwapOrFatal("test reopen")
	s2.eventMu.Unlock()
	require.Equal(t, 5, s2.eventLog.SegmentCacheSize(),
		"the scrub-swap reopen must carry the configured cache size, not revert to the library default")
}
