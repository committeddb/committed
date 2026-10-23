package wal_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// scrubHistory applies a fixed history (type, alice PII, alice delete, tail) to
// s and returns the bound that scrubs alice's original. Shared by the recovery
// tests so the reference and recovered results are directly comparable.
func scrubHistory(t *testing.T, s *StorageWrapper) uint64 {
	t.Helper()
	s.RegisterType(t, "u", 1, 1)
	saveEntity(t, userUpsert("u", "alice", `{"pii":true}`), s, 1, 2)
	saveEntity(t, userDelete("u", "alice"), s, 1, 3)
	saveEntity(t, userUpsert("u", "tail", `{"ok":1}`), s, 1, 4)
	return 3
}

func waitScrubbed(t *testing.T, s *StorageWrapper, bound uint64) {
	t.Helper()
	require.Eventually(t, func() bool {
		return s.ScrubCompletedBound() >= bound
	}, 5*time.Second, 5*time.Millisecond, "scrub did not complete after recovery")
}

// TestScrub_RerunIsIdempotent verifies a second scrub at the same bound (e.g. a
// crash after the swap but before the completed bound persisted, forcing a
// re-run) leaves a byte-identical event log.
func TestScrub_RerunIsIdempotent(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()
	bound := scrubHistory(t, s)

	require.Nil(t, s.RunScrubForTest(bound))
	first, err := s.EventLogSnapshot()
	require.Nil(t, err)

	require.Nil(t, s.RunScrubForTest(bound))
	second, err := s.EventLogSnapshot()
	require.Nil(t, err)

	require.Equal(t, first, second, "re-running a scrub at the same bound must be idempotent")
}
