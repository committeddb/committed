package db_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestDiskGate_UserProposalLifecycle walks a single-node DB through the disk
// states and asserts the user-proposal gate: writable at ok, rejected with
// cluster.ErrInsufficientStorage at critical and full, and writable again once
// disk recovers to ok. The rejections short-circuit before raft, so they
// return immediately regardless of leader state.
func TestDiskGate_UserProposalLifecycle(t *testing.T) {
	d := createDB()
	defer d.Close()

	user := func() *cluster.Proposal { return createProposals([][]string{{"v"}})[0] }

	// Baseline: ok state accepts a user proposal (commits and applies).
	require.NoError(t, d.Propose(testCtx(t), user()))

	// Critical: user-data writes are rejected.
	d.SetDiskStateForTest("critical")
	require.ErrorIs(t, d.Propose(testCtx(t), user()), cluster.ErrInsufficientStorage)

	// Full: still rejected (read-only mode).
	d.SetDiskStateForTest("full")
	require.ErrorIs(t, d.Propose(testCtx(t), user()), cluster.ErrInsufficientStorage)

	// Recovery: back to ok re-enables writes.
	d.SetDiskStateForTest("ok")
	require.NoError(t, d.Propose(testCtx(t), user()))
}

// TestDiskGate_ConfigAcceptedInCritical asserts the critical-state carve-out:
// a config proposal (here a type config) still commits while a user-data
// proposal on the same node is rejected, so the cluster can keep changing
// configuration and draining to make room.
func TestDiskGate_ConfigAcceptedInCritical(t *testing.T) {
	d, _ := newWalDB(t)

	d.SetDiskStateForTest("critical")

	// Config proposal: accepted (proposeTypeTOML asserts no error).
	proposeTypeTOML(t, d, "person", "Person", `{"type":"object"}`, "")

	// User-data proposal on the same node: rejected. An unregistered type is
	// fine — the gate rejects before raft/type resolution.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	userP := &cluster.Proposal{Entities: []*cluster.Entity{{
		Type: &cluster.Type{ID: "unregistered"},
		Key:  []byte("k"),
		Data: []byte("d"),
	}}}
	require.ErrorIs(t, d.Propose(ctx, userP), cluster.ErrInsufficientStorage)
}

// TestDiskGate_FullFreezesConfigButAllowsCheckpoints asserts the full-state
// policy: config (here a type config) is frozen, but checkpoint-kind proposals
// (sync index bumps) still commit so syncables keep delivering + checkpointing
// instead of re-delivering in a loop. Uses real wal storage because ProposeType
// reads existing state the in-memory stub can't model.
func TestDiskGate_FullFreezesConfigButAllowsCheckpoints(t *testing.T) {
	d, _ := newWalDB(t)

	d.SetDiskStateForTest("full")

	// Config: frozen.
	cfg := &cluster.Configuration{ID: "person", MimeType: "text/toml", Data: []byte("[type]\nname = \"Person\"")}
	require.ErrorIs(t, d.ProposeType(testCtx(t), cfg), cluster.ErrInsufficientStorage)

	// Checkpoint (sync index bump): still accepted and committed.
	require.NoError(t, d.ProposeSyncableIndexForTest(testCtx(t), "s-1", 7))
}

// TestDiskGate_IngestPausesAndResumes asserts the ingest worker's
// disk-pressure handling: a data proposal does not return (it retries) while
// the disk is full, and completes once the disk recovers — so ingestion pauses
// cleanly instead of dropping data and advancing its position past it.
func TestDiskGate_IngestPausesAndResumes(t *testing.T) {
	d := createDB()
	defer d.Close()

	d.SetDiskStateForTest("full")

	ctx := t.Context()
	p := createProposals([][]string{{"row"}})[0]

	done := make(chan error, 1)
	go func() { done <- d.ProposeIngestDataForTest(ctx, p) }()

	// While full, the ingest propose must keep retrying, not return.
	select {
	case err := <-done:
		t.Fatalf("ingest propose returned while disk full (err=%v); it must pause", err)
	case <-time.After(100 * time.Millisecond):
	}

	// Recover: the retry should now succeed and commit.
	d.SetDiskStateForTest("ok")
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("ingest propose did not resume after disk recovery")
	}
}
