package wal

import (
	"context"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// rewriteRaw adapts an existing raw-entry transformation such as scrubFilterEntry.
// The caller supplies selections already capped at the authorized scrub bound.
// It does not authorize a scrub, update BoltDB, or declare physical erasure done.
// Even removal validates the original entry's identity before invoking transform;
// surviving replacements must retain it. Callbacks may not reenter the log.
func (l *eventLogAdapter) rewriteRaw(ctx context.Context, generation uint64, transform func([]byte) (bool, []byte, error)) (eventlog.RewriteResult, error) {
	l.mutationMu.Lock()
	defer l.mutationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rewriteRawLocked(ctx, generation, transform)
}

// rewriteRawLocked requires mutationMu and mu exclusively on entry. It releases
// mu during the backend call, which reacquires it for publication, and restores
// it before returning. mutationMu keeps appends and new protected readers out.
func (l *eventLogAdapter) rewriteRawLocked(ctx context.Context, generation uint64, transform func([]byte) (bool, []byte, error)) (eventlog.RewriteResult, error) {
	if ctx == nil || transform == nil {
		return eventlog.RewriteResult{}, eventlog.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return eventlog.RewriteResult{}, err
	}
	if l.protectedReads.Load() > 0 {
		return eventlog.RewriteResult{}, errEventRewriteDeferred
	}
	l.mu.Unlock()
	defer l.mu.Lock()
	return l.log.RewriteWithPublicationLock(ctx, generation, checkedEntryTransform(transform), entryInvalidatingPublicationLock{l})
}

// Invalidate decoded entries before releasing the existing publication gate,
// including uncertain publication failures.
type entryInvalidatingPublicationLock struct{ events *eventLogAdapter }

func (g entryInvalidatingPublicationLock) Lock() { g.events.mu.Lock() }
func (g entryInvalidatingPublicationLock) Unlock() {
	g.events.readEpoch++
	g.events.mu.Unlock()
}

// checkedEntryTransform validates identity before removal and after replacement.
// It is shared by experimental adapter rewrites and Storage publication tests.
func checkedEntryTransform(transform func([]byte) (bool, []byte, error)) eventlog.Transform {
	return func(r eventlog.Record) ([]byte, bool, error) {
		raw, err := checkedEventEntry(r, nil)
		if err != nil {
			return nil, false, err
		}
		keep, payload, err := transform(raw)
		if err != nil || !keep {
			return nil, false, err
		}
		payload, err = checkedEventEntry(eventlog.Record{ID: r.ID, Payload: payload}, nil)
		return payload, true, err
	}
}
