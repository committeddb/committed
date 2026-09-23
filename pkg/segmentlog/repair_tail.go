package segmentlog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/committeddb/committed/internal/durablefs"
)

// RepairTail verifies a stopped log and removes only an incomplete final append
// group, provided the verified prefix still covers requiredThrough and the
// catalog's rewritten checkpoint. The caller must establish requiredThrough
// from its own durable state; the engine cannot infer it from a partial group.
// Complete corrupt groups are never truncated. With commit=false no files change.
// The returned inspection describes the retained prefix, including on refusal.
func RepairTail(ctx context.Context, path string, requiredThrough uint64, commit bool) (result Inspection, retErr error) {
	if ctx == nil {
		return result, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	lock, err := durablefs.Lock(path)
	if err != nil {
		return result, err
	}
	defer func() { retErr = errors.Join(retErr, lock.Close()) }()
	result, err = inspectDirectoryLocked(ctx, path)
	if !errors.Is(err, ErrIncompleteTail) {
		return result, err
	}
	state := result.Tail
	// With an empty active tail, the contiguous closed ranges prove coverage
	// through Start-1. A rewritten tail can retain progress with no survivors.
	through := state.Last
	if !state.HasRecords && state.Start > 0 {
		through = state.Start - 1
	}
	if (!state.HasRecords && state.Start == 0) || through < requiredThrough || state.End < result.checkpointEnd {
		return result, fmt.Errorf("segmentlog: incomplete tail cannot be truncated while preserving required index %d and rewritten checkpoint: %w", requiredThrough, ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if !commit {
		return result, nil
	}
	f, err := os.OpenFile(filepath.Join(path, result.tailFile), os.O_RDWR, 0) // #nosec G304 G703 -- Tail basename comes from the verified catalog under the directory lock.
	if err != nil {
		return result, err
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()
	if err := f.Truncate(state.End); err != nil {
		return result, err
	}
	return result, f.Sync()
}
