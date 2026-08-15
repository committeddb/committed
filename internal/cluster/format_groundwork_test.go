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

// TestErratumTypeID_ReservedGatedUnregistered pins the erratum record's
// reserved identity and, critically, its CURRENT behavior: the ID is minted
// (stable forever), classified gated (must-understand), and deliberately NOT
// registered — so a binary at this version that encounters an Erratum record
// takes the fatal gated-unknown path, never a silent skip or a
// half-understood apply.
func TestErratumTypeID_ReservedGatedUnregistered(t *testing.T) {
	// The exact UUID is load-bearing: it will be written into permanent logs.
	// It may never drift.
	require.Equal(t, "c01177ed-0000-0000-0000-000000000000", ErratumTypeID)

	state, ok := reservedSystemClass(ErratumTypeID)
	require.True(t, ok, "ErratumTypeID must be in the reserved system-type namespace")
	require.Equal(t, compatGated, state,
		"errata are correctness-bearing — a node that skipped them would serve stale readings")
	require.True(t, IsReservedSystemID(ErratumTypeID))

	// Not registered yet: no apply semantics exist in this version.
	require.False(t, IsInternal(ErratumTypeID),
		"registering the type before its fold/apply semantics exist would turn the fatal backstop into a silent misapply")

	// And therefore resolution takes the typed gated-unknown path.
	_, err := resolveType(TypeRef{ID: ErratumTypeID}, &stubResolver{})
	var ure *UnknownReservedTypeError
	require.ErrorAs(t, err, &ure)
	require.False(t, ure.Skippable(), "gated: fatal if encountered, never skipped")
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
// possible state until the errata registry lands) marshals byte-identically
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
