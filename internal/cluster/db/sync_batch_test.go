package db_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster"
)

// TestSyncBatch_Basic verifies that a BatchSyncable receives proposals
// via SyncBatch and that all proposals are delivered.
func TestSyncBatch_Basic(t *testing.T) {
	tests := map[string]struct {
		inputs [][]string
	}{
		"simple":  {inputs: [][]string{{"foo"}}},
		"two":     {inputs: [][]string{{"foo", "bar"}}},
		"two-one": {inputs: [][]string{{"foo", "bar"}, {"baz"}}},
	}

	id := "batch-basic"

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewMemoryStorage()
			db := createDBWithStorage(s)
			defer db.Close()

			size := len(tc.inputs)

			ctx, cancel := context.WithCancel(context.Background())
			ps := createProposals(tc.inputs)
			for _, p := range ps {
				require.Nil(t, db.Propose(testCtx(t), p))
			}

			syncable := NewBatchSyncable(size, cancel)
			err := db.Sync(ctx, id, syncable)
			require.Nil(t, err)
			<-ctx.Done()

			require.Equal(t, size, syncable.Count())
			got := syncable.Proposals()
			for i, p := range ps {
				require.Equal(t, string(p.Entities[0].Data), string(got[i].Entities[0].Data))
			}
		})
	}
}

// TestSyncBatch_IndexAdvancesToLastInBatch verifies that after a batch
// of N proposals, SyncableIndex is set to the last proposal's index,
// not the first.
func TestSyncBatch_IndexAdvancesToLastInBatch(t *testing.T) {
	s := NewMemoryStorage()
	db := createDBWithStorage(s)
	defer db.Close()

	// Propose 5 proposals
	inputs := [][]string{{"a"}, {"b"}, {"c"}, {"d"}, {"e"}}
	ps := createProposals(inputs)
	for _, p := range ps {
		require.Nil(t, db.Propose(testCtx(t), p))
	}

	ctx, cancel := context.WithCancel(context.Background())
	syncable := NewBatchSyncable(len(inputs), cancel)
	err := db.Sync(ctx, "batch-index", syncable)
	require.Nil(t, err)
	<-ctx.Done()

	require.Equal(t, len(inputs), syncable.Count())

	// Wait for the SyncableIndex proposals to be committed. The batch
	// path fires one proposeSyncableIndex per flush (not per proposal),
	// so we expect len(ps) normal + at least 1 SyncableIndex entry.
	var ents []*cluster.Proposal
	require.Eventually(t, func() bool {
		var err error
		ents, err = db.ents()
		if err != nil {
			return false
		}
		// We need at least the original proposals + 1 SyncableIndex
		hasSyncIdx := false
		for _, e := range ents {
			if cluster.IsSyncableIndex(e.Entities[0].Type.ID) {
				hasSyncIdx = true
				break
			}
		}
		return hasSyncIdx
	}, 5*time.Second, 5*time.Millisecond)

	// Collect all normal (non-system) proposal raft indices so we can
	// verify the SyncableIndex points to the last one. We don't know
	// exact raft indices (config change entries precede normal entries).
	var normalIndices []uint64
	r := s.Reader("raw")
	for {
		idx, p, err := r.Read()
		if err != nil {
			break
		}
		if len(p.Entities) > 0 && !cluster.IsSystem(p.Entities[0].Type.ID) {
			normalIndices = append(normalIndices, idx)
		}
	}
	require.Equal(t, len(inputs), len(normalIndices))
	lastNormalIndex := normalIndices[len(normalIndices)-1]

	// Find the SyncableIndex entry and verify its index points to the
	// last proposal in the batch, not the first.
	for _, e := range ents {
		if cluster.IsSyncableIndex(e.Entities[0].Type.ID) {
			si := &cluster.SyncableIndex{}
			require.Nil(t, si.Unmarshal(e.Entities[0].Data))
			require.Equal(t, lastNormalIndex, si.Index,
				"SyncableIndex should point to the last proposal in the batch")
			break
		}
	}
}

// TestSyncBatch_PermanentErrorFallback verifies that when SyncBatch
// returns a permanent error, the batch is retried per-proposal via
// Sync to isolate the bad proposal.
func TestSyncBatch_PermanentErrorFallback(t *testing.T) {
	s := NewMemoryStorage()
	db := createDBWithStorage(s)
	defer db.Close()

	// Propose 3 proposals: the middle one will cause a permanent error
	ps := createProposals([][]string{{"good1"}, {"bad"}, {"good2"}})
	for _, p := range ps {
		require.Nil(t, db.Propose(testCtx(t), p))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	syncable := &errorBatchSyncable{
		// SyncBatch always returns a permanent error to trigger fallback
		batchErr: cluster.Permanent(fmt.Errorf("batch has bad data")),
		// Per-proposal Sync: permanent on "bad", success on everything else
		perProposalErr: func(p *cluster.Proposal) error {
			if string(p.Entities[0].Data) == "bad" {
				return cluster.Permanent(fmt.Errorf("bad proposal"))
			}
			return nil
		},
		doneAtCount: 3,
		cancel:      cancel,
	}

	err := db.Sync(ctx, "batch-perm", syncable)
	require.Nil(t, err)
	<-ctx.Done()

	syncable.mu.Lock()
	defer syncable.mu.Unlock()

	// SyncBatch should have been called once (and failed), then
	// per-proposal Sync should have been called 3 times.
	require.Equal(t, 1, syncable.batchCallCount, "SyncBatch should be called once")
	require.Equal(t, 3, syncable.syncCallCount, "per-proposal Sync should be called 3 times for fallback")

	// All 3 proposals should have been seen by Sync
	require.Equal(t, 3, len(syncable.syncedProps))
	require.Equal(t, "good1", string(syncable.syncedProps[0].Entities[0].Data))
	require.Equal(t, "bad", string(syncable.syncedProps[1].Entities[0].Data))
	require.Equal(t, "good2", string(syncable.syncedProps[2].Entities[0].Data))
}

// TestSyncBatch_NonBatchSyncableUsesPerProposalPath verifies that a
// plain Syncable (not implementing BatchSyncable) still works with
// the per-proposal path.
func TestSyncBatch_NonBatchSyncableUsesPerProposalPath(t *testing.T) {
	s := NewMemoryStorage()
	db := createDBWithStorage(s)
	defer db.Close()

	ps := createProposals([][]string{{"x"}, {"y"}})
	for _, p := range ps {
		require.Nil(t, db.Propose(testCtx(t), p))
	}

	ctx, cancel := context.WithCancel(context.Background())
	syncable := NewSyncable(2, cancel)
	err := db.Sync(ctx, "non-batch", syncable)
	require.Nil(t, err)
	<-ctx.Done()

	require.Equal(t, 2, syncable.Count())
}

// TestSyncBatch_TransientErrorRetries verifies that a transient error
// from SyncBatch causes the entire batch to be retried.
func TestSyncBatch_TransientErrorRetries(t *testing.T) {
	s := NewMemoryStorage()
	db := createDBWithStorage(s)
	defer db.Close()

	ps := createProposals([][]string{{"a"}})
	require.Nil(t, db.Propose(testCtx(t), ps[0]))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	syncable := &transientBatchSyncable{
		failCount: 2, // fail first 2 SyncBatch calls, succeed on 3rd
		cancel:    cancel,
	}

	err := db.Sync(ctx, "batch-transient", syncable)
	require.Nil(t, err)
	<-ctx.Done()

	syncable.mu.Lock()
	defer syncable.mu.Unlock()
	require.True(t, syncable.batchCallCount >= 3,
		"SyncBatch should be called at least 3 times (2 failures + 1 success), got %d",
		syncable.batchCallCount)
}

// errorBatchSyncable implements BatchSyncable. SyncBatch always returns
// batchErr, and per-proposal Sync calls perProposalErr to decide per-
// proposal behavior. Used to test the permanent-error fallback path.
type errorBatchSyncable struct {
	batchErr       error
	perProposalErr func(*cluster.Proposal) error
	doneAtCount    int
	cancel         func()

	mu             sync.Mutex
	batchCallCount int
	syncCallCount  int
	syncedProps    []*cluster.Proposal
}

func (s *errorBatchSyncable) SyncBatch(ctx context.Context, ps []*cluster.Proposal) (bool, error) {
	s.mu.Lock()
	s.batchCallCount++
	s.mu.Unlock()
	return false, s.batchErr
}

func (s *errorBatchSyncable) Sync(ctx context.Context, p *cluster.Proposal) (cluster.ShouldSnapshot, error) {
	s.mu.Lock()
	s.syncCallCount++
	s.syncedProps = append(s.syncedProps, p)
	count := s.syncCallCount
	s.mu.Unlock()

	if count >= s.doneAtCount && s.cancel != nil {
		s.cancel()
	}

	if s.perProposalErr != nil {
		if err := s.perProposalErr(p); err != nil {
			return false, err
		}
	}
	return false, nil
}

func (s *errorBatchSyncable) Close() error {
	return nil
}

// transientBatchSyncable fails the first `failCount` SyncBatch calls
// with a transient error, then succeeds.
type transientBatchSyncable struct {
	failCount int
	cancel    func()

	mu             sync.Mutex
	batchCallCount int
}

func (s *transientBatchSyncable) SyncBatch(ctx context.Context, ps []*cluster.Proposal) (bool, error) {
	s.mu.Lock()
	s.batchCallCount++
	count := s.batchCallCount
	s.mu.Unlock()

	if count <= s.failCount {
		return false, fmt.Errorf("simulated transient batch failure")
	}

	s.cancel()
	return false, nil
}

func (s *transientBatchSyncable) Sync(ctx context.Context, p *cluster.Proposal) (cluster.ShouldSnapshot, error) {
	return false, nil
}

func (s *transientBatchSyncable) Close() error {
	return nil
}
