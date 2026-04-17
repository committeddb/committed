package db_test

import (
	"sync"
	"syscall"

	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/philborlin/committed/internal/cluster/db"
)

// FaultyStorage wraps a db.Storage and returns syscall.ENOSPC from Save
// once the number of entries persisted crosses a threshold. This simulates
// the "disk full" failure mode that scenario (f) of the adversarial suite
// needs to exercise.
//
// Once tripped, EVERY subsequent Save returns ENOSPC — the failure is
// sticky, matching real-world disk-full behaviour (the filesystem doesn't
// spontaneously free space while the node is running). ApplyCommitted and
// AppliedIndex pass through to the inner storage: the ticket specifically
// calls out that "the wrapper must still report a correct AppliedIndex
// for entries that were applied before the threshold hit — the ENOSPC
// simulation is specifically about the Save path."
//
// Threshold counts ENTRIES (not Save calls). Raft bootstrap consumes a
// handful of entries (one conf-change per peer plus an empty leader
// entry), so tests that want "N user entries then ENOSPC" should pick a
// threshold comfortably above those bootstrap entries.
type FaultyStorage struct {
	db.Storage

	mu        sync.Mutex
	threshold int
	saved     int
	tripped   bool
}

// NewFaultyStorage wraps inner with an ENOSPC simulator. Once the total
// count of entries passed to Save exceeds threshold, all subsequent Save
// calls return ENOSPC.
func NewFaultyStorage(inner db.Storage, threshold int) *FaultyStorage {
	return &FaultyStorage{Storage: inner, threshold: threshold}
}

// Save counts the entries passed in (regardless of type — conf-change,
// leader-empty, and normal user entries all count) and once the running
// total exceeds threshold, starts returning ENOSPC. Pre-threshold calls
// pass through to the inner storage.
//
// The tripped flag is sticky: once set, it stays set. Returning "sometimes
// ENOSPC, sometimes OK" would let raft make partial durable progress in a
// way real disk-full failures do not, and would muddy the test's
// assertions.
//
// The test does not also block entries from being applied (ApplyCommitted
// inherits via embedding) because ENOSPC as modelled here is a Save-path
// failure, not a process crash. The test's correctness argument depends
// on the surviving two-node quorum observing their own commits, which is
// independent of what node 1 does with its own apply path.
func (f *FaultyStorage) Save(st raftpb.HardState, ents []raftpb.Entry, snap raftpb.Snapshot) error {
	f.mu.Lock()
	f.saved += len(ents)
	if f.saved > f.threshold {
		f.tripped = true
	}
	tripped := f.tripped
	f.mu.Unlock()
	if tripped {
		return syscall.ENOSPC
	}
	return f.Storage.Save(st, ents, snap)
}

// Tripped reports whether the ENOSPC threshold has been crossed. Used by
// scenario (f) to observe the failure before asserting on surviving-pair
// liveness.
func (f *FaultyStorage) Tripped() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tripped
}
