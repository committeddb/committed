package wal_test

import (
	"testing"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/stretchr/testify/require"

	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

// TestApplyCommitted_UnmarshalError_FailsAndPreservesAppliedIndex is the
// regression test for apply-unmarshal-error-handling.md: a committed raft
// entry whose Data cannot be unmarshaled as a Proposal must propagate the
// error up to the raft Ready loop (which fatal-exits) rather than silently
// advancing appliedIndex.
//
// The old special case in ApplyCommitted swallowed the unmarshal error,
// bumped appliedIndex, and returned nil — which let two replicas diverge
// whenever unmarshal succeeded on one and failed on the other, and let
// snapshots capture state-as-of an index that was never actually applied.
func TestApplyCommitted_UnmarshalError_FailsAndPreservesAppliedIndex(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	require.Equal(t, uint64(0), s.AppliedIndex(), "fresh storage starts at appliedIndex 0")

	// Byte payload that is not a valid clusterpb.LogProposal. proto.Unmarshal
	// will reject it, which is the precondition Proposal.Unmarshal surfaces
	// as its error return.
	corrupt := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	entry := pb.Entry{Term: 1, Index: 1, Type: pb.EntryNormal, Data: corrupt}

	// Persist the entry in the raft log, then apply it. Save must not fail
	// (it doesn't inspect entry.Data) — the failure we care about is in
	// ApplyCommitted.
	require.Nil(t, s.Save(defaultHardState, []pb.Entry{entry}, defaultSnap))

	err := s.ApplyCommitted(entry)
	require.Error(t, err, "ApplyCommitted must propagate unmarshal failures")

	require.Equal(t, uint64(0), s.AppliedIndex(),
		"appliedIndex must NOT advance past an entry that failed to apply")
}

// TestApplyCommitted_MissingTypeVersion_Fails is the second regression
// test from apply-unmarshal-error-handling.md. A proposal stamped with a
// TypeVersion that storage has no record of fails type resolution during
// Proposal.Unmarshal; that failure must propagate (via the new fatal-exit
// policy) rather than silently advancing appliedIndex.
//
// The scenario models the exact invariant violation type-schema-versioning
// introduced: every log entry that references a user-defined Type is
// preceded by that Type's definition entry. If the referenced type is
// absent, the log or state is corrupt and the node must stop rather than
// project a fabricated stub.
func TestApplyCommitted_MissingTypeVersion_Fails(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	require.Equal(t, uint64(0), s.AppliedIndex(), "fresh storage starts at appliedIndex 0")

	// User-defined type ID (not one of the system meta-type IDs — those
	// bypass the resolver and would not trigger the missing-version path).
	// Version 99 is deliberately not registered with storage.
	entity := &cluster.Entity{
		Type: &cluster.Type{ID: "user-defined-type", Version: 99},
		Key:  []byte("k"),
		Data: []byte("payload"),
	}
	p := &cluster.Proposal{Entities: []*cluster.Entity{entity}}
	bs, err := p.Marshal()
	require.Nil(t, err)

	entry := pb.Entry{Term: 1, Index: 1, Type: pb.EntryNormal, Data: bs}
	require.Nil(t, s.Save(defaultHardState, []pb.Entry{entry}, defaultSnap))

	err = s.ApplyCommitted(entry)
	require.Error(t, err, "ApplyCommitted must propagate missing-type-version failures")

	require.Equal(t, uint64(0), s.AppliedIndex(),
		"appliedIndex must NOT advance past an entry whose type could not be resolved")
}
