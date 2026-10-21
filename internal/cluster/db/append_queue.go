package db

import (
	"sync"

	raftpb "go.etcd.io/raft/v3/raftpb"
)

// appendQueue hands raft's local append messages (MsgStorageAppend) from the
// Ready loop to the append goroutine, in order. The Ready loop never blocks on
// it, so a slow disk write never stops this node's ticks, heartbeats or
// replication. Its size is bounded by raft: a leader's uncommitted entries
// (MaxUncommittedEntriesSize) and a follower's in-flight appends
// (MaxInflightMsgs).
type appendQueue struct {
	mu      sync.Mutex
	cond    *sync.Cond
	msgs    []*raftpb.Message
	nudged  bool
	stopped bool
}

func newAppendQueue() *appendQueue {
	q := &appendQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *appendQueue) push(m *raftpb.Message) {
	q.mu.Lock()
	q.msgs = append(q.msgs, m)
	q.mu.Unlock()
	q.cond.Broadcast()
}

// nudge wakes the append goroutine with nothing to append, so it runs its
// between-writes work (a prepared compaction cut) on an idle node.
func (q *appendQueue) nudge() {
	q.mu.Lock()
	q.nudged = true
	q.mu.Unlock()
	q.cond.Broadcast()
}

// maxAppendMerge caps how many queued messages pop merges into one write.
const maxAppendMerge = 256

// pop waits for queued messages, or a nudge (none), and takes every queued
// message up to maxAppendMerge as one write: raft keeps producing Readies
// while a write is in progress, and writing each on its own would pay an
// fsync per Ready. A message carrying a snapshot is taken alone, and ends
// the run before it. pop returns false once stopped; queued messages are
// dropped, since raft acknowledged none of them.
func (q *appendQueue) pop() ([]*raftpb.Message, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.msgs) == 0 && !q.nudged && !q.stopped {
		q.cond.Wait()
	}
	if q.stopped {
		return nil, false
	}
	q.nudged = false
	n := 0
	for n < len(q.msgs) && n < maxAppendMerge {
		if hasSnapshot(q.msgs[n]) {
			if n == 0 {
				n = 1
			}
			break
		}
		n++
	}
	out := append([]*raftpb.Message(nil), q.msgs[:n]...)
	clear(q.msgs[:n])
	q.msgs = q.msgs[n:]
	return out, true
}

func hasSnapshot(m *raftpb.Message) bool {
	return m.GetSnapshot().GetMetadata().GetIndex() != 0
}

// mergeAppends combines consecutive MsgStorageAppends into one write: the
// latest HardState, and the entries as raft's log holds them — a later
// append that starts at or below the earlier tail replaces that tail (a new
// leader overwriting uncommitted entries), exactly as Save would have.
func mergeAppends(msgs []*raftpb.Message) (*raftpb.HardState, []*raftpb.Entry) {
	hs := &raftpb.HardState{}
	var ents []*raftpb.Entry
	for _, m := range msgs {
		if m.Term != nil || m.Vote != nil || m.Commit != nil {
			hs = &raftpb.HardState{Term: m.Term, Vote: m.Vote, Commit: m.Commit}
		}
		next := m.GetEntries()
		if len(next) == 0 {
			continue
		}
		if len(ents) > 0 {
			from := next[0].GetIndex()
			keep := len(ents)
			for keep > 0 && ents[keep-1].GetIndex() >= from {
				keep--
			}
			ents = ents[:keep]
		}
		ents = append(ents, next...)
	}
	return hs, ents
}

func (q *appendQueue) stop() {
	q.mu.Lock()
	q.stopped = true
	q.mu.Unlock()
	q.cond.Broadcast()
}

// durableCommit is the highest commit index the append goroutine has made
// durable. Apply waits on it: raft hands an entry to apply once it is
// committed and in this node's stable log, but the HardState recording that
// commit may still be on its way to disk, and a restart refuses an applied
// index above the stored commit.
type durableCommit struct {
	mu      sync.Mutex
	cond    *sync.Cond
	index   uint64
	stopped bool
}

func newDurableCommit(index uint64) *durableCommit {
	d := &durableCommit{index: index}
	d.cond = sync.NewCond(&d.mu)
	return d
}

func (d *durableCommit) advance(index uint64) {
	d.mu.Lock()
	if index > d.index {
		d.index = index
	}
	d.mu.Unlock()
	d.cond.Broadcast()
}

// wait blocks until index is durable. It returns false if stopped first.
func (d *durableCommit) wait(index uint64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for d.index < index && !d.stopped {
		d.cond.Wait()
	}
	return d.index >= index
}

func (d *durableCommit) stop() {
	d.mu.Lock()
	d.stopped = true
	d.mu.Unlock()
	d.cond.Broadcast()
}
