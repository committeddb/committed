package db

import (
	"sync"

	raftpb "go.etcd.io/raft/v3/raftpb"
)

// applyQueue hands committed entries from the Ready loop to the apply
// goroutine, in order. The Ready loop never blocks on it: a slow apply must
// not stop raft's clock (ticks, elections, leases) or its replication. The
// queue is unbounded here; db.proposeAsync bounds it, by waiting while the
// applied index trails the queued index by more than maxApplyBacklog.
type applyQueue struct {
	mu      sync.Mutex
	cond    *sync.Cond
	batches [][]*raftpb.Entry
	// busy is true while the apply goroutine is applying a popped batch, so
	// drain waits for it too.
	busy    bool
	stopped bool
}

func newApplyQueue() *applyQueue {
	q := &applyQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// push queues a Ready's committed entries.
func (q *applyQueue) push(entries []*raftpb.Entry) {
	q.mu.Lock()
	q.batches = append(q.batches, entries)
	q.mu.Unlock()
	q.cond.Broadcast()
}

// maxApplyMerge caps how many entries pop merges into one apply.
const maxApplyMerge = 10000

// pop waits for queued entries and takes every queued batch, up to
// maxApplyMerge entries, as one: they are consecutive committed entries, and
// applying them together shares one event-log append, one data-run commit and
// one applied-index save. When apply falls behind, Readies queue up one or a
// few entries each; applied one by one, each would pay those fsyncs again.
// pop marks the queue busy until done. It returns false once stopped.
func (q *applyQueue) pop() ([]*raftpb.Entry, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.batches) == 0 && !q.stopped {
		q.cond.Wait()
	}
	if q.stopped {
		return nil, false
	}
	b := q.batches[0]
	n := 1
	if len(q.batches) > 1 {
		b = append([]*raftpb.Entry(nil), b...)
		for ; n < len(q.batches) && len(b)+len(q.batches[n]) <= maxApplyMerge; n++ {
			b = append(b, q.batches[n]...)
		}
	}
	clear(q.batches[:n])
	q.batches = q.batches[n:]
	q.busy = true
	return b, true
}

// done marks the popped batch applied.
func (q *applyQueue) done() {
	q.mu.Lock()
	q.busy = false
	q.mu.Unlock()
	q.cond.Broadcast()
}

// drain waits until every queued batch has been applied. It returns false if
// the queue stopped first.
func (q *applyQueue) drain() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for (len(q.batches) > 0 || q.busy) && !q.stopped {
		q.cond.Wait()
	}
	return !q.stopped
}

// stop wakes every waiter; queued batches are dropped (they are durable in
// the raft log, and a restart replays them from the durable applied index).
func (q *applyQueue) stop() {
	q.mu.Lock()
	q.stopped = true
	q.mu.Unlock()
	q.cond.Broadcast()
}
