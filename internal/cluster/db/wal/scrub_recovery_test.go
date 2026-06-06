package wal_test

import (
	"fmt"
	"os"
	"path/filepath"
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

// referenceScrubSnapshot builds a clean-scrubbed event log on its own storage
// and returns its content snapshot, for comparison against recovered results.
func referenceScrubSnapshot(t *testing.T, bound uint64) []string {
	t.Helper()
	ref := NewStorage(t, nil)
	defer ref.Cleanup()
	scrubHistory(t, ref)
	require.Nil(t, ref.RunScrubForTest(bound))
	snap, err := ref.EventLogSnapshot()
	require.Nil(t, err)
	return snap
}

func waitScrubbed(t *testing.T, s *StorageWrapper, bound uint64) {
	t.Helper()
	require.Eventually(t, func() bool {
		return s.ScrubCompletedBound() >= bound
	}, 5*time.Second, 5*time.Millisecond, "scrub did not complete after recovery")
}

// TestScrubRecovery_MidRewrite simulates a crash during the rewrite: a partial
// events.scrub.<B>/ directory exists, events/ is intact, and a pending bound is
// set. On reopen the stale temp is discarded and the worker resumes from the
// intact log to the identical clean result.
func TestScrubRecovery_MidRewrite(t *testing.T) {
	want := referenceScrubSnapshot(t, 3)

	s := NewStorage(t, nil)
	defer s.Cleanup()
	bound := scrubHistory(t, s)
	require.Nil(t, s.SetPendingScrubBoundForTest(bound))
	require.Nil(t, s.closeIdempotent())

	// Simulate a half-built rewrite directory left behind by the crash.
	staleTmp := filepath.Join(s.path, fmt.Sprintf("events.scrub.%d", bound))
	require.Nil(t, os.MkdirAll(filepath.Join(staleTmp, "garbage"), 0o700))

	s = OpenStorage(t, s.path, s.parser, nil, nil)
	defer s.Cleanup()
	waitScrubbed(t, s, bound)

	require.NoDirExists(t, staleTmp, "stale rewrite temp must be cleaned up")
	got, err := s.EventLogSnapshot()
	require.Nil(t, err)
	require.Equal(t, want, got)
	require.Equal(t, s.EventIndex(), s.AppliedIndex(), "P_local == R_local after recovery")
}

// TestScrubRecovery_MidSwap simulates a crash after events/ was renamed out but
// before the new directory was renamed in: events/ is missing, events.retired/
// (the complete pre-swap log) and a stale events.scrub.<B>/ both exist. On
// reopen the swap is rolled back and the worker re-runs to the identical clean
// result — no data loss.
func TestScrubRecovery_MidSwap(t *testing.T) {
	want := referenceScrubSnapshot(t, 3)

	s := NewStorage(t, nil)
	defer s.Cleanup()
	bound := scrubHistory(t, s)
	require.Nil(t, s.SetPendingScrubBoundForTest(bound))
	path := s.path
	require.Nil(t, s.closeIdempotent())

	// Simulate the mid-swap state: events/ -> events.retired/ (the complete
	// pre-swap log), plus a leftover partial new dir.
	eventsDir := filepath.Join(path, "events")
	retiredDir := filepath.Join(path, "events.retired")
	require.Nil(t, os.Rename(eventsDir, retiredDir))
	require.NoDirExists(t, eventsDir)
	require.Nil(t, os.MkdirAll(filepath.Join(path, fmt.Sprintf("events.scrub.%d", bound)), 0o700))

	s = OpenStorage(t, path, s.parser, nil, nil)
	defer s.Cleanup()
	waitScrubbed(t, s, bound)

	require.NoDirExists(t, retiredDir, "retired dir must be cleaned up after rollback+rerun")
	got, err := s.EventLogSnapshot()
	require.Nil(t, err)
	require.Equal(t, want, got)
	require.Equal(t, s.EventIndex(), s.AppliedIndex(), "P_local == R_local after recovery")
	// The retained delete-tombstone and tail survived the rollback+rerun.
	tail, err := s.ActualAt(4)
	require.Nil(t, err)
	require.Equal(t, []byte("tail"), tail.Entities[0].Key)
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
