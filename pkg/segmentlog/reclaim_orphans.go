package segmentlog

import "context"

// ReclaimOrphans explicitly scans for files left by interrupted preparation.
// It holds exclusive log ownership throughout, excluding readers and writers.
// Bbolt validates all range metadata, then scans directory entries in bounded
// batches and removes unselected managed data/temp files. It does not verify
// payloads or acknowledge queued retirements; Reclaim drains that queue separately.
// Unknown names, directories, and symlinks are preserved. The complete-catalog
// backend uses its existing Reclaim implementation, including payload verification.
// This is full-directory maintenance, never part of open or append. Cancellation
// can report partial progress; other errors poison the handle until reopen.
func (l *Log) ReclaimOrphans(ctx context.Context) (ReclaimResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.usable(); err != nil {
		return ReclaimResult{}, err
	}
	if ctx == nil {
		return ReclaimResult{}, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return ReclaimResult{}, err
	}
	return l.catalog.reclaimOrphans(l, ctx)
}
