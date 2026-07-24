package wal_test

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
)

// Namespaced system-type UUIDs (see cluster's reserved-system prefix). Layout:
// "c01177ed-0000-0000-0000-00000000" + [1 hex: state][3 hex: index]. Pinned as
// literals because the mint helper is unexported to the cluster package.
const (
	ungatedUnknownType = "c01177ed-0000-0000-0000-000000001fa0" // state 1 (ungated),   index 4000
	gatedUnknownType   = "c01177ed-0000-0000-0000-000000000fa0" // state 0 (gated),     index 4000
	undefinedStateType = "c01177ed-0000-0000-0000-000000007001" // state 7 (undefined class)
)

func applyReservedType(t *testing.T, s *StorageWrapper, idx uint64, typeID string) error {
	t.Helper()
	p := &cluster.Proposal{Entities: []*cluster.Entity{{
		Type: &cluster.Type{ID: typeID}, Key: []byte("k"), Data: []byte("v"),
	}}}
	bs, err := p.Marshal()
	require.NoError(t, err)
	entry := &raftpb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(idx), Type: raftpb.EntryNormal.Enum(), Data: bs}
	require.NoError(t, s.Save(&defaultHardState, []*raftpb.Entry{entry}, &defaultSnap))
	return s.ApplyCommitted(entry)
}

// An UNGATED system entry from a newer version is SKIPPED: appliedIndex advances
// and apply returns no error (no fatal), instead of bricking the node on a
// coordination record it doesn't understand.
func TestApply_SkipsUnknownUngatedSystemType(t *testing.T) {
	s := NewStorage(t, nil)
	require.NoError(t, applyReservedType(t, s, 1, ungatedUnknownType))
	require.Equal(t, uint64(1), s.AppliedIndex(), "appliedIndex advances past the skipped entry")
}

// A GATED or UNDEFINED-state system entry from a newer version FATALS: apply
// returns an error (which the raft loop turns into logger.Fatal) and does NOT
// advance appliedIndex — the loud backstop against a missed feature gate.
func TestApply_FatalsOnUnknownGatedOrUndefinedSystemType(t *testing.T) {
	for _, typeID := range []string{gatedUnknownType, undefinedStateType} {
		s := NewStorage(t, nil)
		require.Errorf(t, applyReservedType(t, s, 1, typeID),
			"unknown gated/undefined system type %s must fatal, not skip", typeID)
		require.Equalf(t, uint64(0), s.AppliedIndex(),
			"appliedIndex must not advance past a fatal entry (%s)", typeID)
	}
}

// A syncable reader that crosses an already-applied UNGATED unknown system entry
// must skip it (as the apply path did) and reach EOF, not stall on the
// unresolved-type error.
func TestReader_SkipsUnknownUngatedSystemType(t *testing.T) {
	s := NewStorage(t, nil)
	require.NoError(t, applyReservedType(t, s, 1, ungatedUnknownType))

	_, err := s.Reader("test-sync").Read()
	require.ErrorIs(t, err, io.EOF, "reader must skip the ungated system entry, not surface its type error")
}
