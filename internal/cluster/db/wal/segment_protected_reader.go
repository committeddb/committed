package wal

import (
	"context"
	"errors"
	"sync"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/pkg/segmentlog"
)

// errSegmentRewriteDeferred is retryable and does not poison the log. The caller
// must retain the pending rewrite and retry after protected reads have finished.
var errSegmentRewriteDeferred = errors.New("segment event log: rewrite deferred by protected readers")

// protectedSegmentReader protects one logical rewrite generation across Read
// calls. Appends/rotation continue; this is not a fixed EOF or a backup file pin.
// A deadline is required. Closing or canceling stops future reads and releases
// protection after any in-flight read exits the adapter lock. Callbacks must
// cooperate: cancellation cannot interrupt a blocked type resolver.
type protectedSegmentReader struct {
	*segmentActualReader
	cancel  context.CancelCauseFunc
	stop    func() bool
	release func()
}

func (l *segmentEventLog) protectedReaderAt(ctx context.Context, index uint64, resolver cluster.TypeResolver, applied func() uint64) (*protectedSegmentReader, error) {
	if ctx == nil {
		return nil, segmentlog.ErrInvalid
	}
	if _, ok := ctx.Deadline(); !ok {
		return nil, segmentlog.ErrInvalid
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
	return &protectedSegmentReader{segmentActualReader: r, cancel: cancel, stop: stop, release: release}, nil
}

// Close is concurrent-safe and idempotent. Do not call it from a resolver invoked
// by this reader: release waits for the enclosing Read to finish.
func (r *protectedSegmentReader) Close() error {
	r.cancel(segmentlog.ErrClosed)
	r.stop()
	r.release()
	return nil
}

// protectedReadCount includes cancellation releases still waiting for an active
// Read to finish. It describes actual rewrite blockers, not just live contexts.
func (l *segmentEventLog) protectedReadCount() int {
	return int(l.protectedReads.Load())
}
