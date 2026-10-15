package postgres

import (
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
)

// ackPoll is how often the stream wakes to confirm a transaction the worker
// has made durable since the last status update, while one is outstanding.
const ackPoll = 100 * time.Millisecond

// sourceAck decides what the stream may confirm to Postgres. A confirmed
// position is final: the slot's confirmed_flush_lsn moves forward only, and a
// later START_REPLICATION below it is forwarded to it. So the stream confirms
// a transaction only once the worker reports it durable (cluster.Proposal
// .OnDurable); confirming it when it was handed off would skip it for good if
// its proposal were lost (a leader change, a failed proposal, a crash).
//
// It lives as long as the dialect's Ingest call, across the stream's
// reconnects: a reconnect resumes from the last HANDED-OFF position, which is
// not durable.
type sourceAck struct {
	// durable is the end LSN through which every handed-off transaction is
	// durable. Written by the worker's goroutine.
	durable atomic.Uint64
	// handedOff is the end LSN of the last transaction handed to the worker.
	// Stream goroutine only.
	handedOff pglogrepl.LSN
}

// newSourceAck starts from the durable resume position Ingest was given.
func newSourceAck(durable pglogrepl.LSN) *sourceAck {
	a := &sourceAck{handedOff: durable}
	a.durable.Store(uint64(durable))
	return a
}

// handOff records a transaction handed to the worker.
func (a *sourceAck) handOff(end pglogrepl.LSN) { a.handedOff = max(a.handedOff, end) }

// onDurable is the hook for the proposal that completes the transaction
// ending at end. The worker commits transactions in order, so durable only
// moves forward.
func (a *sourceAck) onDurable(end pglogrepl.LSN) func() {
	return func() {
		for {
			cur := a.durable.Load()
			if cur >= uint64(end) || a.durable.CompareAndSwap(cur, uint64(end)) {
				return
			}
		}
	}
}

// confirmable is the position the stream may confirm, having read the stream
// through received. With every handed-off transaction durable, everything
// read is either durable or holds nothing for this ingestable (a transaction
// that emitted nothing, other databases' WAL); otherwise only the durable
// position.
func (a *sourceAck) confirmable(received pglogrepl.LSN) pglogrepl.LSN {
	d := pglogrepl.LSN(a.durable.Load())
	if d >= a.handedOff {
		return max(received, d)
	}
	return d
}

// outstanding reports whether a handed-off transaction has not been
// confirmed yet.
func (a *sourceAck) outstanding(lastSent pglogrepl.LSN) bool { return a.handedOff > lastSent }
