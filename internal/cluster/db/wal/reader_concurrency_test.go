package wal_test

import (
	"sync"
	"testing"

	pb "go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/stretchr/testify/require"
)

// TestReader_ConcurrentReads verifies that concurrent calls to Read on the same
// Reader serialize correctly via its mutex and produce no duplicate or lost reads.
// Run with -race to detect data races.
func TestReader_ConcurrentReads(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	const numEntries = 5
	entries := make([]pb.Entry, numEntries)
	for i := 0; i < numEntries; i++ {
		entries[i] = makeEntry(t, uint64(i+1), makeUserEntity())
	}
	// Reader reads from the permanent event log (Phase 2), which is
	// populated only by ApplyCommitted. Save alone no longer makes
	// entries visible to the reader.
	saveAndApply(t, s, entries)

	reader := s.Reader("nonexistent-sync")

	var wg sync.WaitGroup
	results := make(chan uint64, numEntries)
	for i := 0; i < numEntries; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			idx, _, err := reader.Read()
			if err == nil {
				results <- idx
			}
		}()
	}
	wg.Wait()
	close(results)

	// All indices should be received exactly once.
	seen := make(map[uint64]bool)
	for idx := range results {
		require.False(t, seen[idx], "duplicate index %d", idx)
		seen[idx] = true
	}
	require.Equal(t, numEntries, len(seen))
}
