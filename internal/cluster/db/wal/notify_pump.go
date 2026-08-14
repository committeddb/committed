package wal

import (
	"sync"
)

// notifyPump decouples the raft APPLY PATH from the config-listener channel:
// apply pushes (never blocks), a pump goroutine drains to the real channel
// (may block, harmlessly, off the apply path).
//
// The field wedge this exists for: destination-DB I/O stalled the listener
// (an undeadlined ALTER waiting on an analyst's table lock), the bounded
// channel filled, and the apply loop blocked ON THE SEND — appliedIndex
// froze one entry behind commitIndex and every proposal cluster-wide timed
// out while /ready stayed green. Init deadlines bound the listener's stalls
// now, but the invariant this pump enforces is absolute: appliedIndex NEVER
// stalls on destination-DB state, bounded or not.
//
// Unbounded by design: config notifications are rare (human-issued config
// changes, boot reconciles) and small (an id + a built syncable pointer), so
// the queue's practical bound is the number of config changes a wedged
// listener can accumulate — memory-irrelevant, and strictly better than the
// alternative (a frozen cluster). FIFO is preserved: one pump per channel,
// popping in push order — the same ordering the direct sends had.
//
// Crash semantics are unchanged from the buffered channel: queued items are
// in-memory and lost on restart, and boot reconciliation re-emits from
// durable state (the same "reconcile re-emits on next start" contract the
// direct sends documented for close-time drops).
type notifyPump[T any] struct {
	mu      sync.Mutex
	cond    *sync.Cond
	items   []T
	stopped bool
}

// newNotifyPump starts the pump goroutine draining to out until closeC.
// A second goroutine watches closeC to wake the pump for shutdown (sync.Cond
// cannot select on a channel).
func newNotifyPump[T any](out chan<- T, closeC <-chan struct{}) *notifyPump[T] {
	q := &notifyPump[T]{}
	q.cond = sync.NewCond(&q.mu)

	go func() {
		<-closeC
		q.mu.Lock()
		q.stopped = true
		q.mu.Unlock()
		q.cond.Broadcast()
	}()

	go func() {
		for {
			q.mu.Lock()
			for len(q.items) == 0 && !q.stopped {
				q.cond.Wait()
			}
			if q.stopped {
				// Close-time drop, same contract as the direct sends had:
				// reconcile re-emits from durable state on next start.
				q.mu.Unlock()
				return
			}
			item := q.items[0]
			q.items = q.items[1:]
			q.mu.Unlock()

			select {
			case out <- item:
			case <-closeC:
				return
			}
		}
	}()
	return q
}

// push enqueues without ever blocking — safe from the apply path.
func (q *notifyPump[T]) push(item T) {
	q.mu.Lock()
	q.items = append(q.items, item)
	q.mu.Unlock()
	q.cond.Signal()
}
