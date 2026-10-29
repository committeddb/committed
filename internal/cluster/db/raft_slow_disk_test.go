package db_test

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster/db"
)

// stallingStorage holds any Save that carries an entry with the given data
// until released, once armed: a disk write that takes as long as the test
// wants.
type stallingStorage struct {
	db.Storage

	mu      sync.Mutex
	armed   bool
	data    []byte
	stalled chan struct{}
	release chan struct{}
}

func (s *stallingStorage) arm(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.armed, s.data = true, data
	s.stalled = make(chan struct{})
	s.release = make(chan struct{})
}

func (s *stallingStorage) Save(st *raftpb.HardState, ents []*raftpb.Entry, snap *raftpb.Snapshot) error {
	s.mu.Lock()
	armed, data, stalled, release := s.armed, s.data, s.stalled, s.release
	s.mu.Unlock()
	if armed {
		for _, e := range ents {
			if bytes.Equal(e.GetData(), data) {
				close(stalled)
				<-release
				break
			}
		}
	}
	return s.Storage.Save(st, ents, snap)
}

// TestSlowLeaderDiskKeepsLeadership: a leader whose own disk write of an entry
// takes several election timeouts keeps sending heartbeats and appends
// meanwhile. No follower starts an election, and the entry commits on the
// followers' acknowledgements while the leader is still writing it. Before
// raft ran with AsyncStorageWrites, the leader sent nothing until its write
// finished, and the followers elected a new leader.
func TestSlowLeaderDiskKeepsLeadership(t *testing.T) {
	var mu sync.Mutex
	stalling := map[uint64]*stallingStorage{}
	rafts, _ := createFaultyRaftsWithStorageWrapper(3, func(id uint64, s db.Storage) db.Storage {
		w := &stallingStorage{Storage: s}
		mu.Lock()
		stalling[id] = w
		mu.Unlock()
		return w
	})
	defer rafts.Close()
	rafts.WaitForLeader(t)
	leader := rafts.LeaderRaft()
	if leader == nil {
		t.Fatal("no stable leader")
	}
	term := leader.raft.TermForTest()

	payload := []byte("written slowly by the leader")
	s := stalling[leader.id]
	s.arm(payload)
	leader.proposeC <- payload
	<-s.stalled

	// Hold the leader's write for three election timeouts.
	electionTimeout := 10 * multiNodeTickInterval
	deadline := time.Now().Add(3 * electionTimeout)
	for time.Now().Before(deadline) {
		for _, r := range rafts {
			if got := r.raft.TermForTest(); got != term {
				close(s.release)
				t.Fatalf("node %d moved to term %d while the leader's disk write was held (was %d)", r.id, got, term)
			}
		}
		time.Sleep(multiNodeTickInterval)
	}
	for _, r := range rafts {
		if r.id != leader.id {
			waitForUserEntry(t, r, payload)
		}
	}
	close(s.release)
	waitForUserEntry(t, leader, payload)
	if got := leader.raft.TermForTest(); got != term {
		t.Fatalf("term moved to %d (was %d)", got, term)
	}
}

// commitCheckingStorage records the highest commit index Save has made
// durable, slows each Save that records a new commit without entries (the
// HardState write that trails a commit), and counts every apply of an entry
// above the durable commit.
type commitCheckingStorage struct {
	db.Storage

	mu         sync.Mutex
	commit     uint64
	violations []uint64
}

func (s *commitCheckingStorage) Save(st *raftpb.HardState, ents []*raftpb.Entry, snap *raftpb.Snapshot) error {
	if len(ents) == 0 && st.GetCommit() > 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if err := s.Storage.Save(st, ents, snap); err != nil {
		return err
	}
	s.mu.Lock()
	s.commit = max(s.commit, st.GetCommit())
	s.mu.Unlock()
	return nil
}

func (s *commitCheckingStorage) ApplyCommittedBatch(entries []*raftpb.Entry) error {
	if k := len(entries); k > 0 {
		s.mu.Lock()
		if last := entries[k-1].GetIndex(); last > s.commit {
			s.violations = append(s.violations, last)
		}
		s.mu.Unlock()
	}
	return s.Storage.ApplyCommittedBatch(entries)
}

// TestApplyNeverPassesTheDurableCommit: raft hands an entry to apply once it
// is committed and in this node's stable log, while the HardState recording
// the commit may still be on its way to disk. A restart refuses an applied
// index above the stored commit (raft panics at start), so apply waits for
// the commit to be durable. The HardState writes are slowed here to widen the
// window.
func TestApplyNeverPassesTheDurableCommit(t *testing.T) {
	var mu sync.Mutex
	checked := map[uint64]*commitCheckingStorage{}
	rafts, _ := createFaultyRaftsWithStorageWrapper(3, func(id uint64, s db.Storage) db.Storage {
		w := &commitCheckingStorage{Storage: s}
		mu.Lock()
		checked[id] = w
		mu.Unlock()
		return w
	})
	defer rafts.Close()
	rafts.WaitForLeader(t)
	leader := rafts.LeaderRaft()
	if leader == nil {
		t.Fatal("no stable leader")
	}
	for i := 0; i < 20; i++ {
		proposeAndCheck(t, leader, fmt.Sprintf("entry %d", i))
	}
	for _, r := range rafts {
		waitForUserEntry(t, r, []byte("entry 19"))
	}
	for id, s := range checked {
		s.mu.Lock()
		v := s.violations
		s.mu.Unlock()
		if len(v) > 0 {
			t.Fatalf("node %d applied through %v before the commit was durable", id, v)
		}
	}
}

// slowStorage takes d over every Save.
type slowStorage struct {
	db.Storage
	d time.Duration
}

func (s *slowStorage) Save(st *raftpb.HardState, ents []*raftpb.Entry, snap *raftpb.Snapshot) error {
	time.Sleep(s.d)
	return s.Storage.Save(st, ents, snap)
}

// TestSlowDiskStillElects: a single node whose every disk write takes longer
// than its election timeout (10-20 ticks of 1ms) still elects itself. Its vote
// counts only once written, so an election timer that ran during the write
// restarted every election before the vote landed, forever.
func TestSlowDiskStillElects(t *testing.T) {
	rafts, _ := createFaultyRaftsWithStorageWrapper(1, func(_ uint64, s db.Storage) db.Storage {
		return &slowStorage{Storage: s, d: 30 * time.Millisecond}
	})
	defer rafts.Close()
	deadline := time.Now().Add(5 * time.Second)
	for rafts.LeaderRaft() == nil {
		if time.Now().After(deadline) {
			t.Fatal("a single node with a slow disk never elected itself")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
