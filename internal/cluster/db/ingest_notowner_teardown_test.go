package db_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// floodingIngestable emits bare-row proposals in a tight, ctx-aware loop until
// its context is cancelled. The unbroken stream keeps the ingest worker's
// proposalChan case perpetually ready, which STARVES the worker's fallback
// time.After(backoff) branch — historically the only place a leader→follower
// ownership-loss teardown lived. It signals when Ingest starts, once it is
// demonstrably flooding, and again when it exits.
type floodingIngestable struct {
	started chan struct{}
	flowing chan struct{}
	stopped chan struct{}
}

func newFloodingIngestable() *floodingIngestable {
	return &floodingIngestable{
		started: make(chan struct{}, 1),
		flowing: make(chan struct{}, 1),
		stopped: make(chan struct{}, 1),
	}
}

func (f *floodingIngestable) Ingest(ctx context.Context, _ cluster.Position, pr chan<- *cluster.Proposal, _ chan<- cluster.Position) error {
	f.started <- struct{}{}
	defer func() { f.stopped <- struct{}{} }()
	for i := 0; ; i++ {
		// A fresh bare row per send (SourceSeq 0, no bundled position → the
		// worker's snapshot-pipeline lane). Fresh so the owner-phase worker can
		// stamp it without racing a shared pointer; the send is ctx-aware so a
		// teardown (ingress.stop cancels this ctx) unblocks it promptly instead
		// of wedging stop()'s Wait.
		p := &cluster.Proposal{Entities: []*cluster.Entity{{
			Type: &cluster.Type{ID: "string"},
			Key:  []byte("k"),
			Data: []byte("v"),
		}}}
		select {
		case pr <- p:
		case <-ctx.Done():
			return nil
		}
		if i == 8 {
			// Enough rows have been accepted that the flood is demonstrably
			// saturating the worker; the test may now flip ownership with the
			// fallback timer already starved (no idle window for the timer
			// branch to reap the session before the data case under test can).
			f.flowing <- struct{}{}
		}
	}
}

func (f *floodingIngestable) Status(context.Context, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{}, nil
}

func (f *floodingIngestable) Close() error { return nil }

// TestIngest_NotOwnerProposalTearsDownUnderStarvedTimer red-proves the
// leader-flap not-owner-drop fix. When a leader-based ingestable's worker loses
// ownership mid-stream while its source is actively producing, the worker must
// tear the ingress session down promptly — releasing the source and, crucially,
// stopping the drain-and-drop loop that (pre-fix) let a later checkpoint advance
// past the silently-dropped rows (permanent, signal-less loss of committed-at-
// source data).
//
// The teardown used to live ONLY in the worker's time.After(backoff) branch,
// which a busy source starves — so under a flood the deposed worker kept
// receiving-and-discarding rows forever, never tearing down. The fix moves the
// teardown into the data (proposalChan) case, so it fires on the first
// not-owner row regardless of the timer.
//
// The continuous flood keeps that timer deterministically starved, so this test
// FAILS against the pre-fix code (the session never tears down → stopped never
// fires) and passes only with the data-case teardown. SetNode drives isNode
// directly, so ownership flips deterministically without racing the raft loop's
// per-Ready IsLeader refresh.
func TestIngest_NotOwnerProposalTearsDownUnderStarvedTimer(t *testing.T) {
	s := NewMemoryStorage()
	d := createDBWithStorage(s)
	defer d.Close()

	// Own the ingestable so the worker starts the session and the flood begins.
	s.SetNode(d.ID())

	ing := newFloodingIngestable()
	require.NoError(t, d.Ingest(context.Background(), "flap-ingest", ing))

	select {
	case <-ing.started:
	case <-time.After(2 * time.Second):
		t.Fatal("ingest session never started while owned")
	}
	// Wait until the flood is demonstrably saturating the worker before
	// flipping ownership — this closes the idle window in which the timer
	// branch (not the data case under test) would otherwise do the teardown.
	select {
	case <-ing.flowing:
	case <-time.After(2 * time.Second):
		t.Fatal("flood never saturated the worker")
	}

	// Lose ownership mid-flood. The timer stays starved, so ONLY the data-case
	// teardown can stop the session.
	s.SetNode(uint64(0xdeadbeef))

	select {
	case <-ing.stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not tear down the session after losing ownership " +
			"mid-flood: the not-owner proposal path failed to stop the session " +
			"(pre-fix, the drain-and-drop loop starves the timer forever and a " +
			"later checkpoint can advance past the dropped rows)")
	}
}
