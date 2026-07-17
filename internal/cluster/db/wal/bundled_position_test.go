package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
)

// A snapshot batch carries its resume checkpoint INLINE on the proposal
// (Proposal.Position), so the rows and the checkpoint commit in one raft entry.
// These tests exercise that apply-path bundling — the fix for the snapshot
// effectively-once gap. The docs promise "you do not get duplicates in the topic
// across a restart" (docs/operations/cdc-setup.md), but SourceSeq dedup can't
// cover snapshot rows (they carry SourceSeq 0); bundling closes the crash window
// between a committed batch and a separate position proposal at the source.

// TestBundledPosition_AppliesAtomicallyWithData proves ApplyCommitted persists
// the checkpoint a proposal carries inline — in the SAME entry as the batch — so
// a restart resumes past the committed batch instead of re-reading it. Before
// the fix (no Proposal.Position, no apply handling), the position stayed empty
// and the batch would re-emit at a new index → duplicate history / delivery.
func TestBundledPosition_AppliesAtomicallyWithData(t *testing.T) {
	const id = "ing"
	s := NewStorageWithParser(t, nil, parser.New())
	defer s.Cleanup()

	seedIngestableConfig(t, s, id, 1)
	require.Nil(t, s.Position(id), "no checkpoint before the batch")

	pos := cluster.Position([]byte("lsn=42;pk=1000"))
	// A real snapshot batch also carries topic rows; the checkpoint rides the
	// SAME proposal, so it commits in one raft entry with them. Here the position
	// field is exercised directly — atomicity with any rows is structural (one
	// entry, applied or replayed whole).
	p := &cluster.Proposal{IngestableID: id, Position: pos}
	saveProposal(t, p, s, 1, 2)

	require.Equal(t, pos, s.Position(id),
		"the bundled checkpoint must advance in the batch's own apply")
}

// TestBundledPosition_ReapedWhenConfigGone proves the bundled path honors the
// same config-existence guard as a standalone position entity: a checkpoint that
// commits after the ingestable was deleted is reaped, never re-established as an
// orphan a same-id recreate would resume from (resuming CDC from a dropped slot's
// LSN). This is the delete-race guard, preserved through the shared put.
func TestBundledPosition_ReapedWhenConfigGone(t *testing.T) {
	const id = "ing"
	s := NewStorageWithParser(t, nil, parser.New())
	defer s.Cleanup()

	// No seedIngestableConfig: the ingestable config does not exist.
	pos := cluster.Position([]byte("lsn=42;pk=1000"))
	p := &cluster.Proposal{IngestableID: id, Position: pos}
	saveProposal(t, p, s, 1, 2)

	require.Nil(t, s.Position(id),
		"a bundled checkpoint for a nonexistent config must be reaped, not orphaned")
}
