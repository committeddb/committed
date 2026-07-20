package db

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// panickingSyncable panics from inside the implementation zone with a value
// that embeds row-like data — the worst case for both node survival and PII.
type panickingSyncable struct{ batch bool }

func (p *panickingSyncable) Sync(context.Context, *cluster.Actual) (cluster.ShouldSnapshot, error) {
	panic("renderer blew up on row: SECRET-ROW-DATA")
}

func (p *panickingSyncable) SyncBatch(context.Context, []*cluster.Actual) (bool, error) {
	panic(errors.New("driver blew up on row: SECRET-ROW-DATA"))
}

func (p *panickingSyncable) Close() error { return nil }

// TestCallSync_ConvertsPanicToTransientError pins the trust boundary: a panic
// in the syncable implementation must come back as an ERROR — transient (never
// auto-dead-lettered: skipping data on a guess breaks the stall-visibly
// contract) — and its replicable text must carry only the panic's TYPE, never
// the value (a panic value can embed row data, and sync errors reach the
// replicated stuck record).
func TestCallSync_ConvertsPanicToTransientError(t *testing.T) {
	d := &DB{logger: zap.NewNop()}
	a := &cluster.Actual{Index: 42}

	snap, err := d.callSync(context.Background(), "s1", &panickingSyncable{}, a)
	require.Error(t, err, "the panic must surface as an error, not unwind the process")
	require.False(t, bool(snap))
	require.False(t, errors.Is(err, cluster.ErrPermanent),
		"a panic is an unknown — it must be TRANSIENT so the stuck/skip flow decides, never auto-dead-lettered")
	require.NotContains(t, err.Error(), "SECRET-ROW-DATA",
		"the error reaches replicated stuck records — it must never carry the panic value")
	require.Contains(t, err.Error(), "42", "the wedged index is the operator's correlation handle")
}

// TestCallSyncBatch_ConvertsPanicToTransientError is the batch twin.
func TestCallSyncBatch_ConvertsPanicToTransientError(t *testing.T) {
	d := &DB{logger: zap.NewNop()}
	batch := []*cluster.Actual{{Index: 7}, {Index: 9}}

	snap, err := d.callSyncBatch(context.Background(), "s1", &panickingSyncable{batch: true}, batch)
	require.Error(t, err)
	require.False(t, snap)
	require.False(t, errors.Is(err, cluster.ErrPermanent))
	require.NotContains(t, err.Error(), "SECRET-ROW-DATA")
	require.Contains(t, err.Error(), "[7..9]")
}
