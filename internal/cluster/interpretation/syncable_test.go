package interpretation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestWrapExposesNoCapabilities: the wrapper implements Sync, Close, and
// Unwrap — never a capability interface on the sink's behalf. The engine
// resolves capabilities through the Unwrap chain (cluster.SyncableAs), so a
// forwarding-with-fallback method here would make every wrapped syncable
// look teardownable, rematerializable, or stamped when its sink is not.
func TestWrapExposesNoCapabilities(t *testing.T) {
	w := Wrap(okInner{}, func() *Registry { return nil }, nil)
	_, td := any(w).(cluster.Teardownable)
	_, rm := any(w).(cluster.Rematerializable)
	_, rs := any(w).(cluster.RenderingStamped)
	_, cc := any(w).(cluster.CheckpointConfigurable)
	require.False(t, td, "a wrapper must not claim Teardownable")
	require.False(t, rm, "a wrapper must not claim Rematerializable")
	require.False(t, rs, "a wrapper must not claim RenderingStamped")
	require.False(t, cc, "a wrapper must not claim CheckpointConfigurable")
	// The chain still reaches a sink that has the capability.
	inner := rematInner{}
	w = Wrap(inner, func() *Registry { return nil }, nil)
	got, ok := cluster.SyncableAs[cluster.Rematerializable](w)
	require.True(t, ok)
	require.True(t, got.CanRematerialize())
}

type rematInner struct{ okInner }

func (rematInner) CanRematerialize() bool                               { return true }
func (rematInner) BeginRematerialization(context.Context, uint64) error { return nil }
func (rematInner) CompleteRematerialization(context.Context) error      { return nil }
