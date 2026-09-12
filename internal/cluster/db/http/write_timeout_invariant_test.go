package http

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	sqlsync "github.com/committeddb/committed/internal/cluster/syncable/sql"
)

// TestWriteTimeoutExceedsHandlerWorkBudget pins the invariant the vanishing-
// 400 field finding taught: the server's write budget must exceed the SUM of
// the bounded phases a handler can legitimately chain before writing its
// response — not merely the largest single bound. Inventory as of this pin:
// a config POST validates (destination build, sql.InitTimeout) and then
// proposes (~30s bound); rebuild/delete chain drain+teardown+propose bounds
// of the same order. If either side of this inequality changes, this test
// forces the arithmetic to be redone consciously instead of re-creating the
// dead-socket-instead-of-a-400 failure.
func TestWriteTimeoutExceedsHandlerWorkBudget(t *testing.T) {
	const proposeBound = 30 * time.Second // db-layer propose timeout (documented bound)
	const margin = 30 * time.Second
	require.GreaterOrEqual(t, defaultWriteTimeout, sqlsync.InitTimeout+proposeBound+margin,
		"the server write budget must cover build+propose plus margin, or slow-but-correct rejections die on the wire")
}
