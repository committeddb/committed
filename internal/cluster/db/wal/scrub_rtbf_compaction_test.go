package wal_test

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

func readRawFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return b
}

// TestScrub_RTBFCompactionReDrivenAfterFailure pins F1: an RTBF scrub prunes the
// delete-tombstone (the raw subject identifier) from bbolt's logical tree, but a
// logical Delete only FREES the page — the raw bytes remain until compaction
// rewrites the file, and CreateSnapshot copies free pages. markScrubComplete
// advances the "completed" bound in the SAME tx as the prune, BEFORE compaction,
// so a compaction that ENOSPCs or crashes was never re-driven (the bound
// suppressed re-run), leaving the erased subject key physically in bbolt and in
// every subsequent snapshot cluster-wide — a broken erasure promise. The fix
// records a durable compaction-owed marker so a restart (or scrub signal)
// completes the physical erasure.
func TestScrub_RTBFCompactionReDrivenAfterFailure(t *testing.T) {
	s := NewStorage(t, nil)

	s.RegisterType(t, "user-events", 1, 1)
	subjectKey := []byte("SUBJECT-PII-alice-9f3ce21b7a")
	// Upsert @2, delete @3. The delete records a tombstone keyed by the raw
	// subject identifier; the scrub will prune it.
	saveEntity(t, &cluster.Entity{Type: &cluster.Type{ID: "user-events"}, Key: subjectKey, Data: []byte(`{"pii":1}`)}, s, 1, 2)
	saveEntity(t, cluster.NewDeleteEntity(&cluster.Type{ID: "user-events"}, subjectKey), s, 1, 3)

	boltPath := s.BoltPathForTest()

	// Scrub with compaction FORCED to fail (ENOSPC/crash surrogate): the tombstone
	// is logically pruned but bbolt is not compacted, so the raw subject key stays
	// physically in a freed page.
	s.SetFailCompactionForTest(func() error { return errors.New("enospc: no space left on device") })
	require.Error(t, s.RunScrubForTest(3), "the injected compaction failure must surface")

	require.True(t, s.IsCompactOwedForTest(),
		"a failed compaction must leave the durable compaction-owed marker set")
	require.True(t, bytes.Contains(readRawFile(t, boltPath), subjectKey),
		"after a failed compaction the erased subject key is still physically in bbolt (freed page)")

	// Restart: re-open the same data dir (no injected failure). Open must re-drive
	// the owed compaction and physically erase the key.
	s2 := s.CloseAndReopenStorage(t)
	defer s2.Cleanup()

	require.False(t, s2.IsCompactOwedForTest(),
		"Open must clear the compaction-owed marker after re-driving compaction")
	require.False(t, bytes.Contains(readRawFile(t, boltPath), subjectKey),
		"after restart the owed compaction must physically erase the subject key from bbolt and every future snapshot")
}
