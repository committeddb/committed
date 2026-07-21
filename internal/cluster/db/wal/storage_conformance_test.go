package wal_test

import (
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"

	parser "github.com/committeddb/committed/internal/cluster/db/parser"
)

// TestStorageConformance_ConcurrentCompact pins the raft Storage contract's
// atomicity requirement: a compaction racing Entries/Term/FirstIndex must
// surface as raft.ErrCompacted (or a clean result), NEVER as the underlying
// wal not-found — etcd raft's node goroutine panics on any error other than
// ErrCompacted/ErrUnavailable (raftLog.slice / raftLog.term), so a leaked
// low-level error is a whole-process crash on the leader, and the race is
// widest exactly when a follower lags under compaction pressure.
//
// The reader hammers full-range Entries (a long per-entry read loop — the wide
// TOCTOU window) while the compactor advances the boundary step by step. This
// is a permanent conformance suite, not a one-off regression test: any future
// Storage change that breaks the racing-compaction contract should fail here
// under -race.
func TestStorageConformance_ConcurrentCompact(t *testing.T) {
	const n = 512
	ents := make([]*pb.Entry, n)
	for i := range ents {
		term, idx := uint64(1), uint64(i+1)
		ents[i] = &pb.Entry{Term: &term, Index: &idx, Type: pb.EntryNormal.Enum(), Data: []byte("x")}
	}
	s := NewStorageWithParser(t, ents, parser.New())
	defer s.Cleanup()

	allowed := func(err error) bool {
		return err == nil || errors.Is(err, raft.ErrCompacted) || errors.Is(err, raft.ErrUnavailable)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // compactor: advance the boundary in small steps
		defer wg.Done()
		for ci := uint64(4); ci < n-8; ci += 4 {
			select {
			case <-stop:
				return
			default:
			}
			_ = s.Compact(ci)
			time.Sleep(200 * time.Microsecond)
		}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		fi, err := s.FirstIndex()
		require.NoError(t, err)
		li, err := s.LastIndex()
		require.NoError(t, err)

		if _, err := s.Entries(fi, li+1, math.MaxUint64); !allowed(err) {
			t.Fatalf("Entries(%d, %d) leaked a non-contract error during racing compaction: %v", fi, li+1, err)
		}
		if _, err := s.Term(fi); !allowed(err) {
			t.Fatalf("Term(%d) leaked a non-contract error during racing compaction: %v", fi, err)
		}
	}

	close(stop)
	wg.Wait()
}
