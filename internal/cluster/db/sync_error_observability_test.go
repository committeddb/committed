package db_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/metrics"
)

// permanentSyncable returns a permanent error on the first USER proposal
// it sees (skipping system proposals like the type registration) and then
// signals the test via cancel. The permanent error drives the dead-letter
// + metric path under test.
type permanentSyncable struct {
	err    error
	cancel func()

	mu        sync.Mutex
	userCount int
}

func (s *permanentSyncable) Sync(ctx context.Context, p *cluster.Actual) (cluster.ShouldSnapshot, error) {
	if allSystem(p) {
		return cluster.ShouldSnapshot(false), nil
	}
	s.mu.Lock()
	s.userCount++
	first := s.userCount == 1
	s.mu.Unlock()
	if first && s.cancel != nil {
		s.cancel()
	}
	return cluster.ShouldSnapshot(false), cluster.Permanent(s.err)
}

func (s *permanentSyncable) Close() error { return nil }

// TestSync_PermanentError_DeadLettersAndCounts is the end-to-end
// observability criterion: a permanent sync error (a) writes a queryable
// dead-letter record through the replicated apply path, and (b)
// increments the permanent error counter. Uses a real wal-backed DB
// because the dead letter lands in bbolt via apply.
func TestSync_PermanentError_DeadLettersAndCounts(t *testing.T) {
	d, s, reader := newWalDBWithMetrics(t)
	id := "perm-sync"
	seedUserProposals(t, d, s, "evt", []string{"bad-row"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	syncable := &permanentSyncable{err: fmt.Errorf("unique constraint violated"), cancel: cancel}
	require.NoError(t, d.Sync(context.Background(), id, syncable))

	select {
	case <-ctx.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("syncable never processed the proposal")
	}

	// The dead-letter record is written just after Sync returns (the
	// worker proposes it on the next line), so poll for it.
	var dls []cluster.SyncableDeadLetter
	require.Eventually(t, func() bool {
		var err error
		dls, err = d.SyncableDeadLetters(id, 0, 10)
		return err == nil && len(dls) == 1
	}, 5*time.Second, 5*time.Millisecond, "permanent error must produce exactly one dead-letter record")

	require.Equal(t, "permanent", dls[0].Kind)
	require.Contains(t, dls[0].Message, "unique constraint violated",
		"the dead letter must carry the failing error message")
	require.Greater(t, dls[0].Index, uint64(0),
		"the dead letter must point at the raft index of the skipped proposal")

	// The permanent error counter incremented for this syncable.
	require.Eventually(t, func() bool {
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			return false
		}
		return syncErrorCount(rm, id, "permanent") >= 1
	}, 5*time.Second, 5*time.Millisecond, "committed.sync.errors{kind=permanent} must increment")
}

// TestSync_TransientError_Counts proves transient errors increment the
// transient counter (each retry occurrence counts, so a stuck worker
// shows a rising rate). MemoryStorage is sufficient — the counter is
// emitted regardless of storage backend.
func TestSync_TransientError_Counts(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	m := metrics.New(provider.Meter("test"))
	s := NewMemoryStorage()
	d := db.New(uint64(1), db.Peers{1: ""}, s, parser.New(), nil, nil,
		db.WithTickInterval(testTickInterval), db.WithMetrics(m))
	defer d.Close()
	s.SetNode(d.ID())

	id := "transient-sync"
	require.NoError(t, d.Propose(testCtx(t), createProposals([][]string{{"x"}})[0]))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	syncable := &ErrorSyncable{
		syncErr:       fmt.Errorf("downstream temporarily unavailable"),
		maxBeforeStop: 3,
		cancel:        cancel,
	}
	require.NoError(t, d.Sync(ctx, id, syncable))

	select {
	case <-ctx.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("syncable never retried the proposal")
	}

	require.Eventually(t, func() bool {
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			return false
		}
		// No permanent errors should be recorded for a transient failure.
		require.Zero(t, syncErrorCount(rm, id, "permanent"))
		return syncErrorCount(rm, id, "transient") >= 1
	}, 5*time.Second, 5*time.Millisecond, "committed.sync.errors{kind=transient} must increment on a transient failure")
}

// syncErrorCount returns the committed.sync.errors counter value for a
// given syncable_id + kind, or 0 if absent.
func syncErrorCount(rm metricdata.ResourceMetrics, id, kind string) int64 {
	m := findMetric(rm, "committed.sync.errors")
	if m == nil {
		return 0
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		return 0
	}
	for _, dp := range sum.DataPoints {
		var gotID, gotKind string
		for _, attr := range dp.Attributes.ToSlice() {
			switch string(attr.Key) {
			case "syncable_id":
				gotID = attr.Value.AsString()
			case "kind":
				gotKind = attr.Value.AsString()
			}
		}
		if gotID == id && gotKind == kind {
			return dp.Value
		}
	}
	return 0
}
