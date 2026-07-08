package db

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestDrainWorkers_BoundedOnWedgedWorker is the Close-hang regression: a syncable
// wedged in tx.Commit (its done channel never closes) must not make Close block
// forever. drainWorkers returns within the timeout, reporting the straggler,
// while a healthy (already-exited) worker drains instantly.
func TestDrainWorkers_BoundedOnWedgedWorker(t *testing.T) {
	exited := make(chan struct{})
	close(exited) // a healthy worker that already returned

	wedged := make(chan struct{}) // never closed — stuck in an uninterruptible commit

	handles := []*workerHandle{{done: exited}, {done: wedged}}

	start := time.Now()
	abandoned := drainWorkers(handles, 150*time.Millisecond)
	elapsed := time.Since(start)

	require.Equal(t, 1, abandoned, "the wedged worker is counted as abandoned")
	require.GreaterOrEqual(t, elapsed, 150*time.Millisecond, "drain waits up to the timeout for the wedged worker")
	require.Less(t, elapsed, 5*time.Second, "drain must return promptly — never hang on the wedged worker")
}

// TestDrainWorkers_AllExitedReturnsImmediately: with every worker already exited,
// drain returns at once and abandons nothing (the common, healthy shutdown).
func TestDrainWorkers_AllExitedReturnsImmediately(t *testing.T) {
	mk := func() *workerHandle {
		ch := make(chan struct{})
		close(ch)
		return &workerHandle{done: ch}
	}
	handles := []*workerHandle{mk(), mk(), mk()}

	start := time.Now()
	abandoned := drainWorkers(handles, 10*time.Second)
	elapsed := time.Since(start)

	require.Zero(t, abandoned)
	require.Less(t, elapsed, time.Second, "an all-exited drain must not wait")
}
