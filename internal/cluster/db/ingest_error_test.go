package db_test

import (
	"context"
	"testing"
	"time"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/stretchr/testify/require"
)

// SignalingIngestable runs forever (until ctx cancellation), signaling
// when Ingest is called and again when it exits.
type SignalingIngestable struct {
	started chan struct{}
	stopped chan struct{}
}

func NewSignalingIngestable() *SignalingIngestable {
	return &SignalingIngestable{
		started: make(chan struct{}, 1),
		stopped: make(chan struct{}, 1),
	}
}

func (i *SignalingIngestable) Ingest(ctx context.Context, pos cluster.Position, pr chan<- *cluster.Proposal, po chan<- cluster.Position) error {
	i.started <- struct{}{}
	defer func() { i.stopped <- struct{}{} }()
	<-ctx.Done()
	return nil
}

func (i *SignalingIngestable) Close() error {
	return nil
}

// TestIngest_ContextCancel verifies that cancelling the parent context
// terminates the ingest goroutine cleanly.
func TestIngest_ContextCancel(t *testing.T) {
	s := NewMemoryStorage()
	db := createDBWithStorage(s)
	defer db.Close()

	// db.New now calls EatCommitC automatically.

	ctx, cancel := context.WithCancel(context.Background())

	ingestable := NewSignalingIngestable()
	err := db.Ingest(ctx, "ingest-cancel", ingestable)
	require.Nil(t, err)

	// Wait for the inner Ingest goroutine to start
	select {
	case <-ingestable.started:
	case <-time.After(2 * time.Second):
		t.Fatal("ingest never started")
	}

	// Cancel the parent context
	cancel()

	// Wait for the inner Ingest to exit cleanly
	select {
	case <-ingestable.stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("ingest did not stop after context cancel")
	}
}

// TestIngest_LeaderChangeStopsIngestGoroutine verifies that when the node
// transitions from leader to non-leader, the inner ingest goroutine is
// cancelled (via the cancel() call at ingest.go:56).
func TestIngest_LeaderChangeStopsIngestGoroutine(t *testing.T) {
	s := NewMemoryStorage()
	db := createDBWithStorage(s)
	defer db.Close()

	// db.New now calls EatCommitC automatically.

	// Start as leader (default node ID matches db ID)
	s.SetNode(db.ID())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ingestable := NewSignalingIngestable()
	err := db.Ingest(ctx, "ingest-leader", ingestable)
	require.Nil(t, err)

	// Wait for ingest goroutine to start (because we are leader)
	select {
	case <-ingestable.started:
	case <-time.After(2 * time.Second):
		t.Fatal("ingest never started while leader")
	}

	// Now transition to not-leader
	s.SetNode(uint64(0xdeadbeef))

	// Wait for ingest goroutine to stop (cancelled by ingest.go state machine)
	select {
	case <-ingestable.stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("ingest did not stop after losing leadership")
	}
}
