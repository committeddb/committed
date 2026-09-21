package segmentlog

import (
	"context"
	"errors"
)

// SealedRewriteResult describes a sealed-only transaction. SealedEnd is the
// exclusive end of its scope; the active tail is untouched. ChangedSegments
// includes EmptiedSegments. Counts describe prepared replacements, even on error.
// Published is true only after catalog durability is confirmed; false with an
// error does not prove the previous catalog remained selected. Reopen to resolve uncertainty.
type SealedRewriteResult struct {
	SealedEnd       uint64
	ChangedSegments uint64
	EmptiedSegments uint64
	Published       bool
}

// RewriteSealed atomically publishes transformations of the current sealed
// ranges at a strictly newer logical generation. IDs and original coverage stay
// fixed; unchanged files retain their names and bytes. Entirely erased ranges
// become empty catalog descriptors without payload files. Even a no-op can
// publish the requested generation, writing only catalog metadata.
//
// This is deliberately a sealed-only operation, not a whole-log scrub. The active
// tail, append frontier, and rotation accounting are unchanged. It cannot complete
// a request whose scope includes active records. Generation describes this scoped
// transaction; Committed's full scrub-generation protocol is not integrated yet.
//
// The mutex excludes reads, appends, rotation, reclamation, and other rewrites.
// Transform runs once per examined record and must not call back into this Log.
// Old files remain until Reclaim. After preparation begins, any failure poisons
// the handle conservatively (including callback failure/cancellation); Close and
// reopen before retrying or reclaiming unpublished replacements.
func (l *Log) RewriteSealed(ctx context.Context, generation uint64, transform Transform) (result SealedRewriteResult, err error) {
	resultAll, err := l.rewrite(ctx, generation, transform, false)
	return resultAll.SealedRewriteResult, err
}

// RewriteResult describes a whole-log transaction. TailChanged reports completed
// tail preparation, including on failure. Published has the same uncertainty
// semantics as SealedRewriteResult.Published.
type RewriteResult struct {
	SealedRewriteResult
	TailChanged bool
}

// Rewrite atomically transforms every surviving record in the captured log,
// including the active tail. It preserves original append progress and rotation
// accounting. All operations are serialized until publication finishes; callbacks
// must not reenter this Log. Old payload files remain until explicit Reclaim.
// Invalid input leaves the handle usable; preparation/publication failures require
// Close and reopen. This storage transaction does not update application metadata.
func (l *Log) Rewrite(ctx context.Context, generation uint64, transform Transform) (RewriteResult, error) {
	return l.rewrite(ctx, generation, transform, true)
}

func (l *Log) rewrite(ctx context.Context, generation uint64, transform Transform, includeTail bool) (result RewriteResult, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err = l.usable(); err != nil {
		return result, err
	}
	if ctx == nil || transform == nil {
		return result, ErrInvalid
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	c, err := l.catalog.head()
	if err != nil {
		return result, l.fail(err)
	}
	result.SealedEnd = c.Active.Start
	if generation <= c.Generation || c.Revision == ^uint64(0) {
		return result, ErrInvalid
	}
	if err = l.catalog.preflight(); err != nil {
		return result, l.fail(err)
	}
	prepared := &preparedLogRewrite{baseRevision: c.Revision, generation: generation, result: result}
	defer func() { err = errors.Join(err, prepared.close()) }()
	if err = prepared.prepare(l, ctx, c, transform, includeTail); err != nil {
		return prepared.result, l.fail(err)
	}
	if err = prepared.publish(l, ctx); err != nil {
		return prepared.result, l.fail(err)
	}
	return prepared.result, nil
}

// prepareSealed acquires and releases the source under Log.mu. The writer only
// borrows the source's replayable record stream for the duration of the call.
func (l *Log) prepareSealed(ctx context.Context, ref SegmentRef, transform Transform) (replacement SegmentRef, changed bool, err error) {
	segment, release, err := l.acquireRange(ref)
	if err != nil {
		return replacement, false, err
	}
	defer func() { err = errors.Join(err, release()) }()
	return (rewriteWriter{dir: l.dir, encoding: l.encoding}).sealed(ctx, ref, segment.Records(), transform)
}

func (l *Log) prepareTail(ctx context.Context, ref TailRef, target uint64, transform Transform) (TailRef, bool, error) {
	state, err := l.tail.State()
	if err != nil {
		return ref, false, err
	}
	input := tailRecords(l.file, state.End)
	if l.resident != nil {
		input = l.resident.view(ref.Start).Records()
	}
	// This captured stream still borrows live state; Log.mu protects its lifetime.
	return (rewriteWriter{dir: l.dir, encoding: l.encoding}).tail(ctx, ref, target, state, input, transform)
}
