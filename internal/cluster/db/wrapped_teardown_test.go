package db_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/migration"
)

// teardownFake is a syncable that records whether its destination teardown
// ran — the observable the wrapper-masking bug erased.
type teardownFake struct {
	*clusterfakes.FakeSyncable
	torn atomic.Bool
}

func (t *teardownFake) Teardown() error { t.torn.Store(true); return nil }

// An always-current syncable's DELETE must tear down its destination. The
// field incident: twelve mode="always-current" projections were deleted
// (all 200), every table survived, and the documented DELETE→re-POST
// schema-change path then died on the stale table — because
// migration.Wrap forwarded Sync/Close but MASKED Teardownable, and
// deleteSync's interface assertion silently failed. The fix resolves
// capability interfaces through the Unwrap chain, so no wrapper ever has
// to remember to forward them again.
func TestDeleteSyncable_TearsDownThroughMigrationWrapper(t *testing.T) {
	p := parser.New()
	d, _ := newWalDBWithSyncListener(t, p)

	inner := &teardownFake{FakeSyncable: &clusterfakes.FakeSyncable{}}
	wrapped := migration.Wrap(inner, nil, nil)
	require.NotSame(t, cluster.Syncable(inner), wrapped, "vacuity guard: the wrapper must actually wrap")

	const id = "ac-teardown"
	require.NoError(t, d.Sync(testCtx(t), id, wrapped))

	require.NoError(t, d.DeleteSyncable(testCtx(t), id, false))
	require.Eventually(t, func() bool { return inner.torn.Load() },
		5*time.Second, 10*time.Millisecond,
		"DELETE of an always-current syncable must reach the inner Teardown through the wrapper")
}
