package db_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/stretchr/testify/require"
)

// ErrorSyncable returns errors from Sync but tracks call counts.
// shouldSnapshot controls whether successful syncs trigger a SyncableIndex proposal.
// Setting shouldSnapshot=false avoids races during cleanup since no extra proposals
// are emitted by the sync goroutine.
type ErrorSyncable struct {
	syncErr        error
	count          int
	maxBeforeStop  int
	cancel         func()
	receivedProps  []*cluster.Proposal
	shouldSnapshot cluster.ShouldSnapshot
}

func (s *ErrorSyncable) Sync(ctx context.Context, p *cluster.Proposal) (cluster.ShouldSnapshot, error) {
	s.count++
	s.receivedProps = append(s.receivedProps, p)
	if s.count >= s.maxBeforeStop && s.cancel != nil {
		s.cancel()
	}
	if s.syncErr != nil {
		return false, s.syncErr
	}
	return s.shouldSnapshot, nil
}

func (s *ErrorSyncable) Close() error {
	return nil
}

// TestSync_SyncError_Continues verifies that when Syncable.Sync returns an error,
// the sync loop continues processing the next proposal instead of crashing.
// This documents the current TODO behavior at sync.go:58.
func TestSync_SyncError_Continues(t *testing.T) {
	s := NewMemoryStorage()
	db := createDBWithStorage(s)
	defer db.Close()

	// Propose 3 proposals
	ps := createProposals([][]string{{"a"}, {"b"}, {"c"}})
	for _, p := range ps {
		require.Nil(t, db.Propose(p))
		<-db.CommitC
	}

	// Syncable that always returns an error but counts calls
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	syncable := &ErrorSyncable{
		syncErr:       fmt.Errorf("simulated sync failure"),
		maxBeforeStop: len(ps),
		cancel:        cancel,
	}

	err := db.Sync(ctx, "sync-1", syncable)
	require.Nil(t, err)

	// Wait until cancel fires (when count reaches maxBeforeStop)
	<-ctx.Done()

	// Despite errors, sync should have processed all proposals
	require.Equal(t, len(ps), syncable.count, "sync loop should continue after sync errors")
}

// TestSync_ContextCancel verifies that cancelling the context terminates the sync goroutine.
func TestSync_ContextCancel(t *testing.T) {
	s := NewMemoryStorage()
	db := createDBWithStorage(s)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())

	syncable := &ErrorSyncable{maxBeforeStop: 1000} // Won't actually fire
	err := db.Sync(ctx, "sync-cancel", syncable)
	require.Nil(t, err)

	// Cancel after a brief moment
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Give the goroutine a chance to exit cleanly. There's no direct way
	// to observe goroutine termination, but we can verify no panic occurs.
	time.Sleep(50 * time.Millisecond)
}

// TestSync_EOF_Continues verifies that when the Reader returns io.EOF
// (no more entries to read), the sync loop continues without crashing.
// New proposals added later should still be picked up.
//
// shouldSnapshot is set to false to avoid the sync goroutine emitting a
// SyncableIndex proposal during cleanup (which would race with db.Close()).
func TestSync_EOF_Continues(t *testing.T) {
	s := NewMemoryStorage()
	db := createDBWithStorage(s)
	defer db.Close()

	// Drain commits asynchronously so propose calls never block.
	db.EatCommitC()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Syncable that does NOT request a snapshot, so no SyncableIndex proposal
	// is emitted by the sync goroutine. This prevents races during cleanup.
	syncable := &ErrorSyncable{maxBeforeStop: 1, cancel: cancel, shouldSnapshot: false}
	err := db.Sync(ctx, "sync-eof", syncable)
	require.Nil(t, err)

	// Brief moment to let sync loop spin on EOF
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 0, syncable.count)

	// Now add a proposal - sync should pick it up despite earlier EOFs
	p := createProposals([][]string{{"after-eof"}})[0]
	require.Nil(t, db.Propose(p))

	// Wait for sync to process the proposal (which fires cancel)
	<-ctx.Done()
	require.Equal(t, 1, syncable.count, "sync should resume after EOF when new data arrives")
}
