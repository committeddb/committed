package db

import (
	"sync"

	raftpb "go.etcd.io/raft/v3/raftpb"
)

// committedBatch is committed entries to apply and the responses raft asked
// to have delivered once they are applied (its MsgStorageApplyResp).
type committedBatch struct {
	entries   []*raftpb.Entry
	responses []*raftpb.Message
}

// applyQueue hands committed entries from the Ready loop to the apply
// goroutine, in order. The Ready loop never blocks on it: a slow apply must
// not stop raft's clock (ticks, elections, leases) or its replication. The
// queue is unbounded here; db.proposeAsync bounds it, by waiting while the
// applied index trails the queued index by more than maxApplyBacklog.
type applyQueue struct {
	mu      sync.Mutex
	cond    *sync.Cond
	batches []committedBatch
	// busy is true while the apply goroutine applies a popped batch, or
	// while exclusive runs its function.
	busy    bool
	stopped bool
}

func newApplyQueue() *applyQueue {
	q := &applyQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// push queues committed entries and the responses to deliver once they are
// applied.
func (q *applyQueue) push(entries []*raftpb.Entry, responses []*raftpb.Message) {
	q.mu.Lock()
	q.batches = append(q.batches, committedBatch{entries: entries, responses: responses})
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
// The merged batch's responses keep their order. pop marks the queue busy
// until done. It returns false once stopped.
func (q *applyQueue) pop() (committedBatch, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for (len(q.batches) == 0 || q.busy) && !q.stopped {
		q.cond.Wait()
	}
	if q.stopped {
		return committedBatch{}, false
	}
	b := q.batches[0]
	n := 1
	if len(q.batches) > 1 {
		b.entries = append([]*raftpb.Entry(nil), b.entries...)
		b.responses = append([]*raftpb.Message(nil), b.responses...)
		for ; n < len(q.batches) && len(b.entries)+len(q.batches[n].entries) <= maxApplyMerge; n++ {
			b.entries = append(b.entries, q.batches[n].entries...)
			b.responses = append(b.responses, q.batches[n].responses...)
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

// exclusive waits until every queued batch has been applied, then runs fn
// with apply held off. It returns false, without running fn, if the queue
// stopped first. A snapshot install uses it: it replaces the state apply
// writes to.
func (q *applyQueue) exclusive(fn func()) bool {
	q.mu.Lock()
	for (len(q.batches) > 0 || q.busy) && !q.stopped {
		q.cond.Wait()
	}
	if q.stopped {
		q.mu.Unlock()
		return false
	}
	q.busy = true
	q.mu.Unlock()
	defer q.done()
	fn()
	return true
}

// ifIdle runs fn, and returns true, only if nothing is queued or being
// applied; the apply goroutine cannot start a batch while fn runs. The Ready
// loop uses it for work that needs the state to match the applied index
// exactly, when no batch will come along to do it (an idle cluster's
// compaction snapshot).
func (q *applyQueue) ifIdle(fn func()) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.batches) > 0 || q.busy || q.stopped {
		return false
	}
	fn()
	return true
}

// stop wakes every waiter; queued batches are dropped (they are durable in
// the raft log, and a restart replays them from the durable applied index).
func (q *applyQueue) stop() {
	q.mu.Lock()
	q.stopped = true
	q.mu.Unlock()
	q.cond.Broadcast()
}
