package wal_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// TestSafeMode_ScrubWorkerHeld pins the storage half of the escape hatch:
// opened with WithSafeMode, a pending scrub bound is NOT resumed — the
// diagnosis window must not have the event log rewritten and swapped
// underneath it, and a scrub that itself crashes the node must not re-fire
// on every safe boot. The bound stays durably recorded, and a subsequent
// NORMAL open resumes and completes it (the recovery contract
// scrub_recovery_test.go pins is deferred, not lost). Close must return
// promptly with no worker running.
func TestSafeMode_ScrubWorkerHeld(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()
	bound := scrubHistory(t, s)
	require.Nil(t, s.SetPendingScrubBoundForTest(bound))
	path := s.path
	require.Nil(t, s.closeIdempotent())

	// Reopen in safe mode: the pending scrub must NOT run.
	ws, err := wal.Open(path, s.parser, nil, nil, wal.WithoutFsync(), wal.WithSafeMode())
	require.NoError(t, err)
	safe := &StorageWrapper{ws, path, s.parser, &sync.Once{}}

	// The scrub worker resumes a pending bound as its very first act when it
	// exists; give a held one ample time to prove it doesn't.
	time.Sleep(200 * time.Millisecond)
	require.Zero(t, safe.ScrubCompletedBound(),
		"safe mode must not resume the pending scrub")

	// Close must not hang waiting for a worker that never ran.
	closed := make(chan struct{})
	go func() { _ = safe.closeIdempotent(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close hung in safe mode (scrubDone never closed?)")
	}

	// A normal reopen resumes the deferred scrub to completion.
	normal := OpenStorage(t, path, s.parser, nil, nil)
	defer normal.Cleanup()
	waitScrubbed(t, normal, bound)
}
