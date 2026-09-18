package wal

import (
	"context"
	"errors"
	"sync"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// errEventRewriteDeferred is retryable and does not poison the log. The caller
// must retain the pending rewrite and retry after protected reads have finished.
var errEventRewriteDeferred = errors.New("segment event log: rewrite deferred by protected readers")

// protectedEventReader protects one logical rewrite generation across Read
// calls. Appends/rotation continue; this is not a fixed EOF or a backup file pin.
// A deadline is required. Closing or canceling stops future reads and releases
// protection after any in-flight read exits the adapter lock. Callbacks must
// cooperate: cancellation cannot interrupt a blocked type resolver.
type protectedEventReader struct {
	*eventActualReader
	cancel  context.CancelCauseFunc
	stop    func() bool
	release func()
}

func (l *eventLogAdapter) protectedReaderAt(ctx context.Context, index uint64, resolver cluster.TypeResolver, applied func() uint64) (*protectedEventReader, error) {
	if ctx == nil {
		return nil, eventlog.ErrInvalid
	}
	if _, ok := ctx.Deadline(); !ok {
		return nil, eventlog.ErrInvalid
	}
	r, err := l.readerAt(index, resolver, applied)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	if err := ctx.Err(); err != nil {
		l.mu.Unlock()
		return nil, err
	}
	if _, err := l.eventIndexLocked(); err != nil {
		l.mu.Unlock()
		return nil, err
	}
	lifetime, cancel := context.WithCancelCause(ctx)
	r.ctx = lifetime
	l.protectedReads.Add(1)
	l.mu.Unlock()
	release := sync.OnceFunc(func() { l.mu.Lock(); l.protectedReads.Add(-1); l.mu.Unlock() })
	stop := context.AfterFunc(lifetime, release)
	return &protectedEventReader{eventActualReader: r, cancel: cancel, stop: stop, release: release}, nil
}

// Close is concurrent-safe and idempotent. Do not call it from a resolver invoked
// by this reader: release waits for the enclosing Read to finish.
func (r *protectedEventReader) Close() error {
	r.cancel(eventlog.ErrClosed)
	r.stop()
	r.release()
	return nil
}

// protectedReadCount includes cancellation releases still waiting for an active
// Read to finish. It describes actual rewrite blockers, not just live contexts.
func (l *eventLogAdapter) protectedReadCount() int {
	return int(l.protectedReads.Load())
}
