package wal

import (
	"context"

	"github.com/committeddb/committed/pkg/segmentlog"
)

// rewriteMetadata performs experimental snapshot compaction with one adapter
// write lock covering bound validation, selection, and publication. The caller
// supplies an authorized bound and a concurrency-safe applied watermark; this
// method checks bound <= applied <= original durable append progress. The applied
// callback must not reenter the adapter or its log.
//
// Protected readers defer the operation before selection. This applies snapshot
// supersession rules only: no RTBF tombstone selection, delete-key erasure, or
// BoltDB metadata updates. It preserves entries beyond bound and requires Reclaim
// for physical cleanup. Production Storage is not wired to this operation.
func (l *segmentEventLog) rewriteMetadata(ctx context.Context, generation, bound uint64, applied func() uint64) (segmentlog.RewriteResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if ctx == nil || applied == nil {
		return segmentlog.RewriteResult{}, segmentlog.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return segmentlog.RewriteResult{}, err
	}
	if l.protectedReads.Load() > 0 {
		return segmentlog.RewriteResult{}, errSegmentRewriteDeferred
	}
	frontier, err := l.eventIndexLocked()
	if err != nil {
		return segmentlog.RewriteResult{}, err
	}
	watermark := applied()
	if bound > watermark || watermark > frontier {
		return segmentlog.RewriteResult{}, segmentlog.ErrInvalid
	}
	selections, err := l.metadataSupersessionsLocked(ctx, bound)
	if err != nil {
		return segmentlog.RewriteResult{}, err
	}
	return l.rewriteRawLocked(ctx, generation, func(raw []byte) (bool, []byte, error) { return scrubFilterEntry(raw, nil, selections, 0) })
}
