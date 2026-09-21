package segmentlog

import (
	"context"
	"iter"
)

// layout is the managed log's metadata boundary. The Log mutex serializes its
// use across preparation, publication and file retirement. head never includes
// historical ranges; ranges yields only overlapping references. Current is a
// diagnostic full snapshot, not an operation used by the managed hot path.
// verifyRewrite only accesses private files and may run outside the mutex
// while the caller retains maintenance ownership.
type layout interface {
	head() (Catalog, error)
	verifyMetadata(context.Context) (Catalog, error)
	ranges(Coverage) iter.Seq2[SegmentRef, error]
	Current() (Catalog, error)
	preflight() error
	publishRollover(*preparedRollover) error
	// A nil tail reference preserves the current active tail without revalidation.
	verifyRewrite([]SegmentRef, *TailRef) (*verifiedRewriteFiles, error)
	publishRewrite(uint64, uint64, *verifiedRewriteFiles) error
	reclaim(*Log, context.Context) (ReclaimResult, error)
	reclaimOrphans(*Log, context.Context) (ReclaimResult, error)
	Close() error
}
