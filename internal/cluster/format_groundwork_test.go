package cluster

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/clusterpb"
)

// These tests pin the 0.8.x format groundwork: record shapes and identities
// that ship EARLY in the series — before their semantics — so on-disk formats
// never migrate mid-series. See the three-layer track's design notes.

// TestRestatementTypeID_ReservedGatedRegistered pins the restatement record's
// identity and classification now that the registry is BUILT: the ID is
// minted (stable forever, written into permanent logs), classified gated
// (must-understand — an OLDER binary that cannot fold restatements fatals rather
// than skipping), registered with apply semantics on this binary, and
// append-only (Standalone kind, never metadata-GC compacted).
func TestRestatementTypeID_ReservedGatedRegistered(t *testing.T) {
	// The exact UUID is load-bearing: it may never drift.
	require.Equal(t, "c01177ed-0000-0000-0000-000000000000", RestatementTypeID)

	state, ok := reservedSystemClass(RestatementTypeID)
	require.True(t, ok, "RestatementTypeID must be in the reserved system-type namespace")
	require.Equal(t, compatGated, state,
		"restatements are correctness-bearing — a node that skipped them would serve stale readings")
	require.True(t, IsReservedSystemID(RestatementTypeID))

	require.True(t, IsInternal(RestatementTypeID), "registered: this binary folds restatements")
	got, err := resolveType(TypeRef{ID: RestatementTypeID}, &stubResolver{})
	require.NoError(t, err)
	require.Equal(t, EntityKindStandalone, got.EntityKind, "append-only registry")
	require.False(t, IsSystemTombstonable(RestatementTypeID),
		"no restatement is ever compacted — every one is part of the fold history")
}

// TestSyncableIndexInterpretationPairRoundTrip proves the checkpoint's
// determinism pair (data index, interpretation index) survives
// Marshal/Unmarshal.
func TestSyncableIndexInterpretationPairRoundTrip(t *testing.T) {
	in := &SyncableIndex{ID: "sync-1", Index: 9001, InterpretationIndex: 4200}
	bs, err := in.Marshal()
	require.NoError(t, err)

	out := &SyncableIndex{}
	require.NoError(t, out.Unmarshal(bs))
	require.Equal(t, in, out)
}

// TestSyncableIndexZeroInterpretationWireBackCompatible pins the add-only
// contract: a checkpoint with no interpretation records folded (the only
// possible state until the restatement registry lands) marshals byte-identically
// to a pre-feature checkpoint, and pre-feature bytes unmarshal to 0.
func TestSyncableIndexZeroInterpretationWireBackCompatible(t *testing.T) {
	got, err := (&SyncableIndex{ID: "sync-1", Index: 9001}).Marshal()
	require.NoError(t, err)

	want, err := proto.Marshal(&clusterpb.LogSyncableIndex{ID: "sync-1", Index: 9001})
	require.NoError(t, err)
	require.True(t, bytes.Equal(got, want),
		"a zero interpretation index must stamp no bytes (pre-feature checkpoints stay byte-identical)")

	out := &SyncableIndex{}
	require.NoError(t, out.Unmarshal(want))
	require.Zero(t, out.InterpretationIndex, "pre-feature checkpoint bytes decode to 0 = nothing folded")
}
